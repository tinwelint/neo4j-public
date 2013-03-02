/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.index;

import static org.neo4j.helpers.collection.IteratorUtil.asIterable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.IndexNotFoundKernelException;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Manages the "schema indexes" that were introduced in 2.0. These indexes depend on the normal neo4j logical log
 * for transactionality. Each index has an {@link IndexRule}, which it uses to filter changes that come into the
 * database. Changes that apply to the the rule are indexed. This way, "normal" changes to the database can be
 * replayed to perform recovery after a crash.
 *
 * <h3>Recovery procedure</h3>
 *
 * Each index has a state, as defined in {@link org.neo4j.kernel.api.InternalIndexState}, which is used during recovery. If
 * an index is anything but {@link org.neo4j.kernel.api.InternalIndexState#ONLINE}, it will simply be destroyed and re-created.
 *
 * If, however, it is {@link org.neo4j.kernel.api.InternalIndexState#ONLINE}, the index provider is required to also guarantee
 * that the index had been flushed to disk.
 */
public class IndexingService extends LifecycleAdapter
{
    // TODO create hierarchy of filters for smarter update processing

    /**
     * The indexing services view of the universe.
     */
    public interface IndexStoreView
    {
        boolean nodeHasLabel( long nodeId, long label );

        /**
         * Get properties of a node, if those properties exist.
         * @param nodeId
         * @param propertyKeys
         */
        Iterator<Pair<Integer, Object>> getNodeProperties( long nodeId, Iterator<Long> propertyKeys );

        /**
         * Retrieve all nodes in the database with a given label and property, as pairs of node id and
         * property value.
         *
         * @param labelId
         * @param propertyKeyId
         * @return
         */
        void visitNodesWithPropertyAndLabel( long labelId, long propertyKeyId, Visitor<Pair<Long, Object>> visitor );
    }

    private final JobScheduler scheduler;
    private final SchemaIndexProvider provider;
    private final IndexStoreView storeView;
    private final LabelUpdateToPropertyUpdate labelUpdateToPropertyUpdate;
    private final IndexRuleRepository indexRules;

    private final StringLogger log;

    private final Map<Long, IndexContext> indexes = new HashMap<Long, IndexContext>();

    private boolean serviceRunning = false;

    public IndexingService( JobScheduler scheduler, SchemaIndexProvider provider, IndexStoreView storeView, StringLogger log )
    {
        this.scheduler = scheduler;
        this.provider = provider;
        this.storeView = storeView;
        this.indexRules = new IndexRuleRepository();
        this.labelUpdateToPropertyUpdate = new LabelUpdateToPropertyUpdate( storeView, indexRules );
        this.log = log;

        if(provider == null)
        {
            // For now
            throw new IllegalStateException( "You cannot run the database without providing a schema index provider, " +
                    "please make sure that a valid provider is on your classpath." );
        }
    }

    // Recovery semantics: This is to be called after initIndexes, and after the database has run recovery.
    @Override
    public void start()
    {
        // Find all indexes that are not already online, and create them
        for ( IndexContext indexContext : indexes.values() )
        {
            switch ( indexContext.getState() )
            {
                case ONLINE:
                    // Don't do anything, index is ok.
                    break;
                case POPULATING:
                case NON_EXISTENT:
                    // Re-create the index
                    indexContext.create();
                    break;
                case FAILED:
                    // Don't do anything, the user needs to drop the index and re-create
                    break;
            }
        }

        serviceRunning = true;
    }

    @Override
    public void stop()
    {
        serviceRunning = false;
    }

    public void update( Iterator<NodePropertyUpdate> updates ) {

        Iterable<NodePropertyUpdate> iterableUpdates = asIterable( updates );
        for (IndexContext context : indexes.values())
            context.update( iterableUpdates.iterator() );
    }

    public void updateLabel( Iterator<NodeLabelUpdate> updates ) {
        update( labelUpdateToPropertyUpdate.apply(updates) );
    }

    public IndexContext getContextForRule( long indexId ) throws IndexNotFoundKernelException
    {
        IndexContext indexContext = indexes.get( indexId );
        if(indexContext == null)
        {
            throw new IndexNotFoundKernelException( "No index with id " + indexId + " exists." );
        }
        return indexContext;
    }

    /**
     * Called while the database starts up, before recovery.
     *
     * @param indexRules Known index rules before recovery.
     */
    public void initIndexes( Iterable<IndexRule> indexRules )
    {
        for ( IndexRule indexRule : indexRules )
        {
            long id = indexRule.getId();
            switch ( provider.getInitialState( id ) )
            {
                case ONLINE:
                    indexes.put( id, createOnlineIndexContext( indexRule ) );
                    break;
                case POPULATING:
                case NON_EXISTENT:
                    // The database was shut down during population, or a crash has occurred, or some other
                    // sad thing.
                    indexes.put( id, createPopulatingIndexContext( indexRule, storeView ) );
                    break;
                case FAILED:
                    indexes.put( id, new FailedIndexContext( provider.getPopulator( indexRule.getId() )));
                    break;
            }
        }
    }

    /*
     * Creates a new index.
     *
     * This code is called from the transaction infrastructure during transaction commits, which means that
     * it is *vital* that it is stable, and handles errors very well. Failing here means that the entire db
     * will shut down.
     */
    public void createIndex( IndexRule rule, NeoStore neoStore )
    {
        IndexContext index = indexes.get( rule.getId() );
        if ( serviceRunning )
        {
            assert index == null : "Index " + rule + " already exists";
            index = createPopulatingIndexContext( rule, storeView );

            // Trigger the creation, only if the service is online. Otherwise,
            // creation will be triggered on start().
            index.create();
        }
        else if ( index == null )
        {
            index = createPopulatingIndexContext( rule, storeView );
        }
        
        indexes.put( rule.getId(), index );
    }

    private IndexContext createOnlineIndexContext( IndexRule rule )
    {
        indexRules.add(rule);

        IndexContext result = new OnlineIndexContext( rule, provider.getWriter( rule.getId() ), storeView );
        result = new RuleUpdateFilterIndexContext( result, rule, storeView );
        result = new ServiceStateUpdatingIndexContext( rule, result );

        return result;
    }

    private IndexContext createPopulatingIndexContext( final IndexRule rule, final IndexStoreView storeView )
    {
        indexRules.add(rule);

        long ruleId = rule.getId();
        FlippableIndexContext flippableContext = new FlippableIndexContext( );

        // TODO: This is here because there is a circular dependency from PopulatingIndexContext to FlippableContext
        flippableContext.setFlipTarget(
                new PopulatingIndexContext( scheduler, rule, provider.getPopulator( ruleId ), flippableContext, storeView, log)
        );
        flippableContext.flip();

        // Prepare for flipping to online mode
        flippableContext.setFlipTarget( new DelegatingIndexContext(new Function<Object, IndexContext>(){
            @Override
            public IndexContext apply( Object o )
            {
                return new OnlineIndexContext( rule, provider.getWriter( rule.getId() ), storeView );
            }
        }));

        IndexContext result = new RuleUpdateFilterIndexContext( flippableContext, rule, storeView );
        result = new ServiceStateUpdatingIndexContext( rule, result );
        return result;
    }

    class ServiceStateUpdatingIndexContext extends DelegatingIndexContext {

        private final IndexRule rule;

        ServiceStateUpdatingIndexContext( IndexRule rule, IndexContext delegate )
        {
            super( delegate );
            this.rule = rule;
        }

        @Override
        public void drop()
        {
            super.drop();
            indexes.remove( rule.getId() );
            indexRules.remove( rule );
        }
    }

    public void flushAll()
    {
        for ( IndexContext context : indexes.values() )
        {
            context.force();
        }
    }

    private static IndexContextFactory eager( final IndexContext context )
    {
        return new IndexContextFactory()
        {
            @Override
            public IndexContext create()
            {
                return context;
            }
        };
    }
}
