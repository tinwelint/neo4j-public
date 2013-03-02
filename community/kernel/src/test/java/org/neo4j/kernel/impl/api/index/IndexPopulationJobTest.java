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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.IteratorUtil.asIterator;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.ImpermanentGraphDatabase;

public class IndexPopulationJobTest
{
    @Test
    public void shouldPopulateIndexWithOneNode() throws Exception
    {
        // GIVEN
        String value = "Taylor";
        long nodeId = createNode( map( name, value ), FIRST );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexContext() );

        // WHEN
        job.run();

        // THEN
        verify( populator ).createIndex();
        verify( populator ).add( nodeId, value );
        verify( populator ).populationCompleted();

        verifyNoMoreInteractions( populator );
    }

    @Test
    public void shouldPopulateIndexWithASmallDataset() throws Exception
    {
        // GIVEN
        String value = "Mattias";
        long node1 = createNode( map( name, value ), FIRST );
        createNode( map( name, value ), SECOND );
        createNode( map( age, 31 ), FIRST );
        long node4 = createNode( map( age, 35, name, value ), FIRST );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexContext() );

        // WHEN
        job.run();

        // THEN
        verify( populator ).createIndex();
        verify( populator ).add( node1, value );
        verify( populator ).add( node4, value );
        verify( populator ).populationCompleted();

        verifyNoMoreInteractions( populator );
    }
    
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldIndexUpdatesWhenDoingThePopulation() throws Exception
    {
        // GIVEN
        Object value1 = "Mattias", value2 = "Jacob", value3 = "Stefan", changedValue = "changed";
        long node1 = createNode( map( name, value1 ), FIRST );
        long node2 = createNode( map( name, value2 ), FIRST );
        long node3 = createNode( map( name, value3 ), FIRST );
        long changeNode = node1;
        long propertyKeyId = context.getPropertyKeyId( name );
        NodeChangingWriter populator = new NodeChangingWriter( changeNode, propertyKeyId, value1, changedValue );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexContext() );
        populator.setJob( job );

        // WHEN
        job.run();

        // THEN
        Set<Pair<Long,Object>> expected = asSet(
                Pair.of( node1, value1 ),
                Pair.of( node2, value2 ),
                Pair.of( node3, value3 ),
                Pair.of( node1, changedValue ) );
        assertEquals( expected, populator.added ); 
    }
    
    @Test
    public void shouldRemoveViaIndexUpdatesWhenDoingThePopulation() throws Exception
    {
        // GIVEN
        String value1 = "Mattias", value2 = "Jacob", value3 = "Stefan";
        long node1 = createNode( map( name, value1 ), FIRST );
        long node2 = createNode( map( name, value2 ), FIRST );
        long node3 = createNode( map( name, value3 ), FIRST );
        long propertyKeyId = context.getPropertyKeyId( name );
        NodeDeletingWriter populator = new NodeDeletingWriter( node2, propertyKeyId, value2 );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, populator, new FlippableIndexContext() );
        populator.setJob( job );

        // WHEN
        job.run();

        // THEN
        Map<Long, Object> expectedAdded = MapUtil.<Long,Object>genericMap( node1, value1, node2, value2, node3, value3 );
        assertEquals( expectedAdded, populator.added ); 
        Map<Long, Object> expectedRemoved = MapUtil.<Long,Object>genericMap( node2, value2 );
        assertEquals( expectedRemoved, populator.removed ); 
    }

    @Test
    public void shouldTransitionToFailedStateIfPopulationJobCrashes() throws Exception
    {
        // GIVEN
        IndexPopulator failingPopulator = mock( IndexPopulator.class );
        doThrow(new RuntimeException("BORK BORK")).when( failingPopulator ).add( anyLong(), any() );

        FlippableIndexContext index = new FlippableIndexContext();

        createNode( map( name, "Taylor" ), FIRST );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name, failingPopulator, index );

        // WHEN
        job.run();

        // THEN
        assertThat( index.getState(), equalTo( InternalIndexState.FAILED) );
    }

    private class NodeChangingWriter extends IndexPopulator.Adapter
    {
        private final Set<Pair<Long, Object>> added = new HashSet<Pair<Long,Object>>();
        private IndexPopulationJob job;
        private final long changedNode;
        private final Object newValue;
        private final Object previousValue;
        private final long propertyKeyId;
        
        public NodeChangingWriter( long changedNode, long propertyKeyId, Object previousValue, Object newValue )
        {
            this.changedNode = changedNode;
            this.propertyKeyId = propertyKeyId;
            this.previousValue = previousValue;
            this.newValue = newValue;
        }

        @Override
        public void add( long nodeId, Object propertyValue )
        {
            if ( nodeId == 2 )
            {
                job.update( asIterator( new NodePropertyUpdate( changedNode, propertyKeyId, previousValue, newValue ) ) );
            }
            added.add( Pair.of( nodeId, propertyValue ) );
        }

        @Override
        public void update(Iterator<NodePropertyUpdate> updates)
        {
            while(updates.hasNext())
            {
                NodePropertyUpdate update = updates.next();
                switch ( update.getUpdateMode() )
                {
                    case ADDED:
                    case CHANGED:
                        added.add( Pair.of( update.getNodeId(), update.getValueAfter()) );
                }
            }

        }

        public void setJob( IndexPopulationJob job )
        {
            this.job = job;
        }
    }
    
    private class NodeDeletingWriter extends IndexPopulator.Adapter
    {
        private final Map<Long, Object> added = new HashMap<Long, Object>();
        private final Map<Long, Object> removed = new HashMap<Long, Object>();
        private final long nodeToDelete;
        private IndexPopulationJob job;
        private final long propertyKeyId;
        private final Object valueToDelete;

        public NodeDeletingWriter( long nodeToDelete, long propertyKeyId, Object valueToDelete )
        {
            this.nodeToDelete = nodeToDelete;
            this.propertyKeyId = propertyKeyId;
            this.valueToDelete = valueToDelete;
        }
        
        public void setJob( IndexPopulationJob job )
        {
            this.job = job;
        }

        @Override
        public void add( long nodeId, Object propertyValue )
        {
            if ( nodeId == 3 )
            {
                job.update( asIterator( new NodePropertyUpdate( nodeToDelete, propertyKeyId, valueToDelete, null ) ) );
            }
            added.put( nodeId, propertyValue );
        }

        @Override
        public void update(Iterator<NodePropertyUpdate> updates)
        {
            while(updates.hasNext())
            {
                NodePropertyUpdate update = updates.next();
                switch ( update.getUpdateMode() )
                {
                    case ADDED:
                    case CHANGED:
                        added.put( update.getNodeId(), update.getValueAfter() );
                    case REMOVED:
                        removed.put( update.getNodeId(), update.getValueBefore() );
                }
            }

        }
    }
    
    private ImpermanentGraphDatabase db;
    private final Label FIRST = DynamicLabel.label( "FIRST" ), SECOND = DynamicLabel.label( "SECOND" );
    private final String name = "name", age = "age";
    private ThreadToStatementContextBridge ctxProvider;
    private StatementContext context;
    private IndexPopulator populator;

    @Before
    public void before() throws Exception
    {
        db = new ImpermanentGraphDatabase();
        ctxProvider = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        context = ctxProvider.getCtxForReading();
        populator = mock( IndexPopulator.class );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private IndexPopulationJob newIndexPopulationJob( Label label, String propertyKey, IndexPopulator populator, FlippableIndexContext flipper )
            throws LabelNotFoundKernelException, PropertyKeyNotFoundException
    {
        IndexRule indexRule = new IndexRule( 0, context.getLabelId( label.name() ), context.getPropertyKeyId( propertyKey ) );
        NeoStore neoStore = db.getXaDataSourceManager().getNeoStoreDataSource().getNeoStore();
        return new IndexPopulationJob( indexRule, populator, flipper, new NeoStoreIndexStoreView( neoStore ), StringLogger.DEV_NULL );
    }

    private long createNode( Map<String, Object> properties, Label... labels )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode( labels );
            for ( Map.Entry<String, Object> property : properties.entrySet() )
                node.setProperty( property.getKey(), property.getValue() );
            tx.success();
            return node.getId();
        }
        finally
        {
            tx.finish();
        }
    }
}
