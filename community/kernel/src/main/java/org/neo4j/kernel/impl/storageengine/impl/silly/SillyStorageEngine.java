/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.TransactionalCursorDependencies;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.ExplicitIndexProviderLookup;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.kernel.impl.util.DependencySatisfier;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.singleton;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.count;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.Iterables.resourceIterable;
import static org.neo4j.kernel.impl.storemigration.StoreFileType.ID;
import static org.neo4j.register.Registers.newDoubleLongRegister;

/**
 * Purely in-memory storage just to see where other code makes assumptions about the current record storage engine
 * or one or more of its internals.
 */
public class SillyStorageEngine extends LifecycleAdapter implements
        StorageEngine, StoreReadLayer, CommandCreationContext, CommandReaderFactory, Visitor<StorageCommand,IOException>
{
    private final SillyData data;
    private final SchemaCache schemaCache;
    private final IndexProviderMap schemaIndexProviderMap;
    private final IndexingService indexService;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokens;
    private final RelationshipTypeTokenHolder relationshipTypeTokens;
    private final DatabaseHealth databaseHealth;
    private final SimpleTransactionIdStore txIdStore;
    private final SimpleLogVersionRepository logVersionRepo;
    private final SillyIndexStoreView indexStoreView;
    private final StoreId storeId;

    public SillyStorageEngine(
            File storeDir,
            Config config,
            LogProvider logProvider,
            PropertyKeyTokenHolder propertyKeyTokenHolder,
            LabelTokenHolder labelTokens,
            RelationshipTypeTokenHolder relationshipTypeTokens,
            SchemaState schemaState,
            ConstraintSemantics constraintSemantics,
            JobScheduler scheduler,
            TokenNameLookup tokenNameLookup,
            IndexProviderMap indexProviderMap,
            IndexingService.Monitor indexingServiceMonitor,
            DatabaseHealth databaseHealth,
            ExplicitIndexProviderLookup explicitIndexProviderLookup,
            IndexConfigStore indexConfigStore,
            IdOrderingQueue explicitIndexTransactionOrdering,
            IdGeneratorFactory idGeneratorFactory,
            Monitors monitors,
            OperationalMode operationalMode )
    {
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokens = labelTokens;
        this.relationshipTypeTokens = relationshipTypeTokens;
        this.databaseHealth = databaseHealth;
        this.data = new SillyData( labelTokens, propertyKeyTokenHolder, relationshipTypeTokens );
        this.indexStoreView = new SillyIndexStoreView( data );
        this.schemaCache = new SchemaCache( constraintSemantics, Collections.emptyList() );
        this.schemaIndexProviderMap = indexProviderMap;
        this.indexService = IndexingServiceFactory.createIndexingService( config, scheduler, schemaIndexProviderMap,
                indexStoreView, tokenNameLookup, Collections.emptyList(), logProvider, indexingServiceMonitor, schemaState );
        this.txIdStore = new SimpleTransactionIdStore();
        this.logVersionRepo = new SimpleLogVersionRepository();
        this.storeId = new StoreId( currentTimeMillis(), ThreadLocalRandom.current().nextLong(), 0, 0, 0 );

        // TODO an annoying thing where an external token creator uses an externally instantiated IdGeneratorFactory,
        // to generate ids for new tokens. So we need to ensure that those id generators are open, which means
        // also creating their backing files in the file system which is decided upon externally.
        createTokenIdGenerator( storeDir, idGeneratorFactory, StoreType.LABEL_TOKEN, IdType.LABEL_TOKEN );
        createTokenIdGenerator( storeDir, idGeneratorFactory, StoreType.PROPERTY_KEY_TOKEN, IdType.PROPERTY_KEY_TOKEN );
        createTokenIdGenerator( storeDir, idGeneratorFactory, StoreType.RELATIONSHIP_TYPE_TOKEN, IdType.RELATIONSHIP_TYPE_TOKEN );
    }

    private static void createTokenIdGenerator( File storeDir, IdGeneratorFactory idGeneratorFactory, StoreType storeType, IdType idType )
    {
        File file = new File( storeDir, storeType.getStoreFile().fileName( ID ) );
        long highId = 1 << 32;
        idGeneratorFactory.create( file, highId, true );
        idGeneratorFactory.open( file, idType, () -> 0, highId );
    }

    @Override
    public StoreReadLayer storeReadLayer()
    {
        return this;
    }

    @Override
    public CursorFactory cursors( TransactionalCursorDependencies dependencies )
    {
        return new SillyCursorFactory( data, dependencies );
    }

    @Override
    public CommandCreationContext allocateCommandCreationContext()
    {
        return this;
    }

    @Override
    public void createCommands( Collection<StorageCommand> target, ReadableTransactionState state, StorageStatement storageStatement,
            ResourceLocker locks, long lastTransactionIdWhenStarted )
            throws TransactionFailureException, CreateConstraintFailureException, ConstraintValidationException
    {
        state.accept( new TxStateVisitor()
        {
            @Override
            public void visitRemovedIndex( SchemaIndexDescriptor element )
            {
                target.add( new SillyStorageCommand.DropIndex( element ) );
            }

            @Override
            public void visitRemovedConstraint( ConstraintDescriptor element )
            {
                target.add( new SillyStorageCommand.DropConstraint( element ) );
            }

            @Override
            public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                    Iterator<Integer> removed ) throws ConstraintValidationException
            {
                while ( added.hasNext() )
                {
                    target.add( new SillyStorageCommand.SetRelationshipProperty( id, added.next() ) );
                }
                while ( changed.hasNext() )
                {
                    target.add( new SillyStorageCommand.SetRelationshipProperty( id, changed.next() ) );
                }
                while ( removed.hasNext() )
                {
                    target.add( new SillyStorageCommand.RemoveRelationshipProperty( id, removed.next() ) );
                }
            }

            @Override
            public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed,
                    Iterator<Integer> removed ) throws ConstraintValidationException
            {
                while ( added.hasNext() )
                {
                    target.add( new SillyStorageCommand.SetNodeProperty( id, added.next() ) );
                }
                while ( changed.hasNext() )
                {
                    target.add( new SillyStorageCommand.SetNodeProperty( id, changed.next() ) );
                }
                while ( removed.hasNext() )
                {
                    target.add( new SillyStorageCommand.RemoveNodeProperty( id, removed.next() ) );
                }
            }

            @Override
            public void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed ) throws ConstraintValidationException
            {
                target.add( new SillyStorageCommand.ChangeLabels( id, added, removed ) );
            }

            @Override
            public void visitGraphPropertyChanges( Iterator<StorageProperty> added, Iterator<StorageProperty> changed, Iterator<Integer> removed )
            {
                while ( added.hasNext() )
                {
                    target.add( new SillyStorageCommand.SetGraphProperty( added.next() ) );
                }
                while ( changed.hasNext() )
                {
                    target.add( new SillyStorageCommand.SetGraphProperty( changed.next() ) );
                }
                while ( removed.hasNext() )
                {
                    target.add( new SillyStorageCommand.RemoveGraphProperty( removed.next() ) );
                }
            }

            @Override
            public void visitDeletedRelationship( long id )
            {
                RelationshipData rel = data.relationships.get( id );
                target.add( new SillyStorageCommand.DeleteRelationship( id, rel.type(), rel.startNode(), rel.endNode() ) );
            }

            @Override
            public void visitDeletedNode( long id )
            {
                target.add( new SillyStorageCommand.DeleteNode( id ) );
            }

            @Override
            public void visitCreatedRelationshipTypeToken( String name, int id )
            {
                target.add( new SillyStorageCommand.CreateRelationshipTypeToken( name, id ) );
            }

            @Override
            public void visitCreatedRelationship( long id, int type, long startNode, long endNode ) throws ConstraintValidationException
            {
                target.add( new SillyStorageCommand.CreateRelationship( id, type, startNode, endNode ) );
            }

            @Override
            public void visitCreatedPropertyKeyToken( String name, int id )
            {
                target.add( new SillyStorageCommand.CreatePropertyKeyToken( name, id ) );
            }

            @Override
            public void visitCreatedNode( long id )
            {
                target.add( new SillyStorageCommand.CreateNode( id ) );
            }

            @Override
            public void visitCreatedLabelToken( String name, int id )
            {
                target.add( new SillyStorageCommand.CreateLabelToken( name, id ) );
            }

            @Override
            public void visitAddedIndex( SchemaIndexDescriptor element )
            {
                target.add( new SillyStorageCommand.CreateIndex( data.nextSchemaId.getAndIncrement(), element ) );
            }

            @Override
            public void visitAddedConstraint( ConstraintDescriptor element ) throws CreateConstraintFailureException
            {
                target.add( new SillyStorageCommand.CreateConstraint( data.nextSchemaId.getAndIncrement(), element ) );
            }

            @Override
            public void close()
            {
            }
        } );
    }

    @Override
    public void apply( CommandsToApply batch, TransactionApplicationMode mode ) throws Exception
    {
        try
        {
            CommandsToApply current = batch;
            while ( current != null )
            {
                databaseHealth.assertHealthy( Exception.class );
                current.accept( this );
                current = current.next();
            }
        }
        catch ( Throwable t )
        {
            databaseHealth.panic( t );
            throw t;
        }
    }

    @Override
    public boolean visit( StorageCommand element ) throws IOException
    {
        ((SillyStorageCommand)element).applyTo( data );
        return false; // meaning "continue" to apply
    }

    @Override
    public StoreId getStoreId()
    {
        return storeId;
    }

    @Override
    public CommandReaderFactory commandReaderFactory()
    {
        return this;
    }

    @Override
    public void flushAndForce( boolean disableIoLimit )
    {   // don't
    }

    @Override
    public void registerDiagnostics()
    {
    }

    @Override
    public void forceClose()
    {
    }

    @Override
    public void prepareForRecoveryRequired()
    {
    }

    @Override
    public ResourceIterator<StoreFileMetadata> listStorageFiles()
    {
        return resourceIterable( Collections.<StoreFileMetadata>emptyList() ).iterator();
    }

    @Override
    public void satisfyDependencies( DependencySatisfier satisfier )
    {
        satisfier.satisfyDependency( txIdStore );
        satisfier.satisfyDependency( logVersionRepo );
        satisfier.satisfyDependency( indexStoreView );
        satisfier.satisfyDependency( indexService );
    }

    @Override
    public void loadSchemaCache()
    {
//        schemaCache.load( data.indexRules.values() );
    }

    /////////////////////////////////////////////////////// StoreReadLayer ///////////////////////////////////////////////////

    @Override
    public StorageStatement newStatement()
    {
        return new SillyStorageStatement( data, indexService );
    }

    @Override
    public Iterator<SchemaIndexDescriptor> indexesGetForLabel( int labelId )
    {
        return schemaCache.indexDescriptorsForLabel( labelId );
    }

    @Override
    public Iterator<SchemaIndexDescriptor> indexesGetAll()
    {
        return map( IndexRule::getIndexDescriptor, schemaCache.indexRules() ).iterator();
    }

    @Override
    public Iterator<SchemaIndexDescriptor> indexesGetRelatedToProperty( int propertyId )
    {
        return schemaCache.indexesByProperty( propertyId );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( SchemaIndexDescriptor index )
    {
        IndexRule rule = indexRule( index );
        if ( rule != null )
        {
            return rule.getOwningConstraint();
        }
        return null;
    }

    private IndexRule indexRule( SchemaIndexDescriptor index )
    {
        for ( IndexRule rule : schemaCache.indexRules() )
        {
            if ( rule.getIndexDescriptor().equals( index ) )
            {
                return rule;
            }
        }
        return null;
    }

    @Override
    public long indexGetCommittedId( SchemaIndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index );
        if ( rule == null )
        {
            throw new SchemaRuleNotFoundException( SchemaRule.Kind.INDEX_RULE.userString(), index.schema() );
        }
        return rule.getId();
    }

    @Override
    public PrimitiveIntIterator graphGetPropertyKeys()
    {
        return PrimitiveIntCollections.emptyIterator();
    }

    @Override
    public Object graphGetProperty( int propertyKeyId )
    {
        return null;
    }

    @Override
    public Iterator<StorageProperty> graphGetAllProperties()
    {
        return emptyIterator();
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor )
    {
        return schemaCache.constraintsForSchema( descriptor );
    }

    @Override
    public boolean constraintExists( ConstraintDescriptor descriptor )
    {
        return schemaCache.hasConstraintRule( descriptor );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId )
    {
        return schemaCache.constraintsForLabel( labelId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        return schemaCache.constraintsForRelationshipType( typeId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll()
    {
        return schemaCache.constraints();
    }

    static PrimitiveLongResourceIterator nodeIds( SillyData data, Predicate<NodeData> filter )
    {
        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
        {
            private final Iterator<Entry<Long,NodeData>> iterator = data.nodes.entrySet().iterator();

            @Override
            protected boolean fetchNext()
            {
                while ( iterator.hasNext() )
                {
                    Entry<Long,NodeData> node = iterator.next();
                    if ( filter.test( node.getValue() ) )
                    {
                        return next( node.getKey() );
                    }
                }
                return false;
            }
        };
    }

    @Override
    public PrimitiveLongResourceIterator nodesGetForLabel( StorageStatement statement, int labelId )
    {
        return nodeIds( data, node -> node.hasLabel( labelId ) );
    }

    @Override
    public SchemaIndexDescriptor indexGetForSchema( SchemaDescriptor descriptor )
    {
        return schemaCache.indexDescriptor( descriptor );
    }

    @Override
    public InternalIndexState indexGetState( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor.schema() ).getState();
    }

    @Override
    public IndexProviderDescriptor indexGetProviderDescriptor( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor.schema() ).getProviderDescriptor();
    }

    @Override
    public CapableIndexReference indexReference( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return null;
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor ).getIndexPopulationProgress();
    }

    @Override
    public String indexGetFailure( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor ).getPopulationFailure().asString();
    }

    @Override
    public int labelGetForName( String labelName )
    {
        return labelTokens.getIdByName( labelName );
    }

    @Override
    public String labelGetName( int labelId ) throws LabelNotFoundKernelException
    {
        try
        {
            return labelTokens.getTokenById( labelId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new LabelNotFoundKernelException( "Label by id " + labelId, e );
        }
    }

    @Override
    public int propertyKeyGetForName( String propertyKeyName )
    {
        return propertyKeyTokenHolder.getIdByName( propertyKeyName );
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName )
    {
        return propertyKeyTokenHolder.getOrCreateId( propertyKeyName );
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        try
        {
            return propertyKeyTokenHolder.getTokenById( propertyKeyId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new PropertyKeyIdNotFoundKernelException( propertyKeyId, e );
        }
    }

    @Override
    public Iterator<Token> propertyKeyGetAllTokens()
    {
        return propertyKeyTokenHolder.getAllTokens().iterator();
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        return labelTokens.getAllTokens().iterator();
    }

    @Override
    public Iterator<Token> relationshipTypeGetAllTokens()
    {
        return (Iterator) relationshipTypeTokens.getAllTokens().iterator();
    }

    @Override
    public int relationshipTypeGetForName( String relationshipTypeName )
    {
        return relationshipTypeTokens.getIdByName( relationshipTypeName );
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        try
        {
            return relationshipTypeTokens.getTokenById( relationshipTypeId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new RelationshipTypeIdNotFoundKernelException( relationshipTypeId, e );
        }
    }

    @Override
    public int labelGetOrCreateForName( String labelName ) throws TooManyLabelsException
    {
        return labelTokens.getOrCreateId( labelName );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName )
    {
        return relationshipTypeTokens.getOrCreateId( relationshipTypeName );
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( long relationshipId, RelationshipVisitor<EXCEPTION> relationshipVisitor )
            throws EntityNotFoundException, EXCEPTION
    {
    }

    @Override
    public PrimitiveLongIterator nodesGetAll()
    {
        return nodeIds( data, Predicates.alwaysTrue() );
    }

    @Override
    public RelationshipIterator relationshipsGetAll()
    {
        return new AllRelationshipsIterator( data.relationships );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( StorageStatement statement, NodeItem nodeItem, Direction direction )
    {
        return Cursors.cursorOf( Iterators.cast( ((NodeData)nodeItem).relationships( direction ) ) );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( StorageStatement statement, NodeItem nodeItem, Direction direction,
            IntPredicate typeIds )
    {
        return Cursors.cursorOf( Iterators.cast( ((NodeData)nodeItem).relationships( direction, typeIds ) ) );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperties( StorageStatement statement, NodeItem node, AssertOpen assertOpen )
    {
        return Cursors.cursorOf( Iterables.cast( ((NodeData)node).properties().values() ) );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperty( StorageStatement statement, NodeItem node, int propertyKeyId, AssertOpen assertOpen )
    {
        PropertyData property = ((NodeData)node).properties().get( propertyKeyId );
        return property != null ? Cursors.cursorOf( singleton( property ) ) : Cursors.empty();
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperties( StorageStatement statement, RelationshipItem relationship,
            AssertOpen assertOpen )
    {
        return Cursors.cursorOf( Iterables.cast( ((RelationshipData)relationship).properties().values() ) );
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperty( StorageStatement statement, RelationshipItem relationshipItem, int propertyKeyId,
            AssertOpen assertOpen )
    {
        PropertyData property = ((RelationshipData)relationshipItem).properties().get( propertyKeyId );
        return property != null ? Cursors.cursorOf( singleton( property ) ) : Cursors.empty();
    }

    @Override
    public void releaseNode( long id )
    {
    }

    @Override
    public void releaseRelationship( long id )
    {
    }

    @Override
    public long countsForNode( int labelId )
    {
        return count( nodesGetForLabel( null, labelId ) );
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        long count = 0;
        PrimitiveLongResourceIterator startNodes = nodesGetForLabel( null, startLabelId );
        while ( startNodes.hasNext() )
        {
            NodeData node = data.nodes.get( startNodes.next() );
            Iterator<RelationshipData> rels = node.relationships( Direction.BOTH, type -> type == typeId );
            while ( rels.hasNext() )
            {
                RelationshipData rel = rels.next();
                NodeData otherNode = data.nodes.get( rel.otherNode( node.id() ) );
                if ( otherNode.hasLabel( endLabelId ) )
                {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public long indexSize( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return 0;
    }

    @Override
    public double indexUniqueValuesPercentage( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return 0;
    }

    @Override
    public long nodesGetCount()
    {
        return data.nodes.size();
    }

    @Override
    public long relationshipsGetCount()
    {
        return data.relationships.size();
    }

    @Override
    public int labelCount()
    {
        return 0;
    }

    @Override
    public int propertyKeyCount()
    {
        return 0;
    }

    @Override
    public int relationshipTypeCount()
    {
        return 0;
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( SchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        return newDoubleLongRegister();
    }

    @Override
    public DoubleLongRegister indexSample( SchemaDescriptor descriptor, DoubleLongRegister target ) throws IndexNotFoundKernelException
    {
        return newDoubleLongRegister();
    }

    @Override
    public boolean nodeExists( long id )
    {
        return data.nodes.containsKey( id );
    }

    @Override
    public boolean relationshipExists( long id )
    {
        return data.relationships.containsKey( id );
    }

    @Override
    public PrimitiveIntSet relationshipTypes( StorageStatement statement, NodeItem node )
    {
        return ((NodeData)node).types();
    }

    @Override
    public void degrees( StorageStatement statement, NodeItem nodeItem, DegreeVisitor visitor )
    {
        ((NodeData)nodeItem).visitDegrees( visitor );
    }

    @Override
    public int degreeRelationshipsInGroup( StorageStatement storeStatement, long nodeId, long groupId, Direction direction,
            Integer relType )
    {
        NodeData node = data.nodes.get( nodeId );
        TotalCountingDegreeVisitor visitor = new TotalCountingDegreeVisitor();
        node.visitDegrees( visitor );
        return visitor.getTotalCount();
    }

    @Override
    public <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StoreReadLayer,T> factory )
    {
        return schemaCache.getOrCreateDependantState( type, factory, this );
    }

    /////////////////////////////////////////// CommandCreationContext ////////////////////////////////////////////////

    @Override
    public void close()
    {
    }

    /////////////////////////////////////////// CommandReaderFactory ////////////////////////////////////////////////

    @Override
    public CommandReader byVersion( byte version )
    {
        return null;
    }
}
