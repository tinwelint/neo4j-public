package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.ExplicitIndexProviderLookup;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.internal.DatabaseHealth;
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

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singleton;

import static org.neo4j.helpers.collection.Iterables.map;

/**
 * Purely in-memory storage just to see where other code makes assumptions about the current record storage engine
 * or one or more of its internals.
 */
public class SillyStorageEngine implements StorageEngine, StoreReadLayer, CursorFactory, CommandCreationContext, CommandReaderFactory
{
    private final ConcurrentMap<Long,NodeData> nodes = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long,RelationshipData> relationships = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long,SchemaRule> indexRules = new ConcurrentHashMap<>();
    private final SchemaCache schemaCache;
    private final SchemaIndexProviderMap schemaIndexProviderMap;
    private final IndexingService indexService;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokens;
    private final RelationshipTypeTokenHolder relationshipTypeTokens;

    public SillyStorageEngine(
            Config config,
            LogProvider logProvider,
            PropertyKeyTokenHolder propertyKeyTokenHolder,
            LabelTokenHolder labelTokens,
            RelationshipTypeTokenHolder relationshipTypeTokens,
            SchemaState schemaState,
            ConstraintSemantics constraintSemantics,
            JobScheduler scheduler,
            TokenNameLookup tokenNameLookup,
            LockService lockService,
            SchemaIndexProviderMap indexProviderMap,
            IndexingService.Monitor indexingServiceMonitor,
            DatabaseHealth databaseHealth,
            ExplicitIndexProviderLookup explicitIndexProviderLookup,
            IndexConfigStore indexConfigStore,
            IdOrderingQueue explicitIndexTransactionOrdering,
            IdGeneratorFactory idGeneratorFactory,
            IdController idController,
            Monitors monitors,
            OperationalMode operationalMode )
    {
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokens = labelTokens;
        this.relationshipTypeTokens = relationshipTypeTokens;
        schemaCache = new SchemaCache( constraintSemantics, Collections.emptyList() );
        IndexStoreView indexStoreView = new SillyIndexStoreView();
        schemaIndexProviderMap = indexProviderMap;
        indexService = IndexingServiceFactory.createIndexingService( config, scheduler, schemaIndexProviderMap,
                indexStoreView, tokenNameLookup, Collections.emptyList(), logProvider, indexingServiceMonitor, schemaState );
    }

    @Override
    public StoreReadLayer storeReadLayer()
    {
        return this;
    }

    @Override
    public CursorFactory cursors()
    {
        return this;
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
    }

    @Override
    public void apply( CommandsToApply batch, TransactionApplicationMode mode ) throws Exception
    {
    }

    @Override
    public CommandReaderFactory commandReaderFactory()
    {
        return this;
    }

    @Override
    public void flushAndForce( IOLimiter limiter )
    {   // don't
    }

    @Override
    public void registerDiagnostics( DiagnosticsManager diagnosticsManager )
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
    public Collection<StoreFileMetadata> listStorageFiles()
    {
        return Collections.emptyList();
    }

    @Override
    public void loadSchemaCache()
    {
        schemaCache.load( indexRules.values() );
    }

    /////////////////////////////////////////////////////// CursorFactory ///////////////////////////////////////////////////

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return new SillyNodeCursor( nodes );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new SillyRelationshipScanCursor();
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return new SillyRelationshipTraversalCursor();
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return new SillyPropertyCursor();
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return new SillyRelationshipGroupCursor();
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return new SillyNodeValueIndexCursor();
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return new SillyNodeLabelIndexCursor();
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        throw new UnsupportedOperationException();
    }

    /////////////////////////////////////////////////////// StoreReadLayer ///////////////////////////////////////////////////

    @Override
    public StorageStatement newStatement()
    {
        return null;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        return schemaCache.indexDescriptorsForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        return map( IndexRule::getIndexDescriptor, schemaCache.indexRules() ).iterator();
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetRelatedToProperty( int propertyId )
    {
        return schemaCache.indexesByProperty( propertyId );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
    {
        IndexRule rule = indexRule( index );
        if ( rule != null )
        {
            return rule.getOwningConstraint();
        }
        return null;
    }

    private IndexRule indexRule( IndexDescriptor index )
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
    public long indexGetCommittedId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index );
        if ( rule == null )
        {
            throw new SchemaRuleNotFoundException( SchemaRule.Kind.INDEX_RULE, index.schema() );
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

    @Override
    public PrimitiveLongResourceIterator nodesGetForLabel( StorageStatement statement, int labelId )
    {
        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
        {
            private final Iterator<Entry<Long,NodeData>> iterator = nodes.entrySet().iterator();

            @Override
            protected boolean fetchNext()
            {
                while ( iterator.hasNext() )
                {
                    Entry<Long,NodeData> node = iterator.next();
                    if ( node.getValue().labelSet().contains( labelId ) )
                    {
                        return next( node.getKey() );
                    }
                }
                return false;
            }
        };
    }

    @Override
    public IndexDescriptor indexGetForSchema( LabelSchemaDescriptor descriptor )
    {
        return schemaCache.indexDescriptor( descriptor );
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor.schema() ).getState();
    }

    @Override
    public Descriptor indexGetProviderDescriptor( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor.schema() ).getProviderDescriptor();
    }

    @Override
    public IndexCapability indexGetCapability( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor.schema() ).getIndexCapability();
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( descriptor ).getIndexPopulationProgress();
    }

    @Override
    public String indexGetFailure( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException
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
        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
        {
            private final Iterator<Long> iterator = nodes.keySet().iterator();

            @Override
            protected boolean fetchNext()
            {
                return iterator.hasNext() ? next( iterator.next() ) : false;
            }
        };
    }

    @Override
    public RelationshipIterator relationshipsGetAll()
    {
        return new AllRelationshipsIterator( relationships );
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
        return 0;
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        return 0;
    }

    @Override
    public long indexSize( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return 0;
    }

    @Override
    public double indexUniqueValuesPercentage( LabelSchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return 0;
    }

    @Override
    public long nodesGetCount()
    {
        return 0;
    }

    @Override
    public long relationshipsGetCount()
    {
        return 0;
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
    public DoubleLongRegister indexUpdatesAndSize( LabelSchemaDescriptor descriptor, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        return null;
    }

    @Override
    public DoubleLongRegister indexSample( LabelSchemaDescriptor descriptor, DoubleLongRegister target ) throws IndexNotFoundKernelException
    {
        return null;
    }

    @Override
    public boolean nodeExists( long id )
    {
        return false;
    }

    @Override
    public PrimitiveIntSet relationshipTypes( StorageStatement statement, NodeItem node )
    {
        return null;
    }

    @Override
    public void degrees( StorageStatement statement, NodeItem nodeItem, DegreeVisitor visitor )
    {
    }

    @Override
    public int degreeRelationshipsInGroup( StorageStatement storeStatement, long id, long groupId, Direction direction, Integer relType )
    {
        return 0;
    }

    @Override
    public <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StoreReadLayer,T> factory )
    {
        return null;
    }

    ///////////

    @Override
    public void close()
    {
    }

    @Override
    public CommandReader byVersion( byte version )
    {
        return null;
    }
}
