package org.neo4j.kernel.impl.store.newprop;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.transaction.state.PropertyDeleter;
import org.neo4j.kernel.impl.transaction.state.PropertyTraverser;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.batchinsert.internal.DirectRecordAccess;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.kernel.impl.transaction.state.Loaders.propertyLoader;

/**
 * The current format could be implemented in a way which creates less garbage, either by copying code
 * from PropertyStore/PropertyRecord/PropertyBlock into this class and using cursor-style approach,
 * or make use of the new property cursors in 2.3. So when comparing to another format which might not
 * produce as much garbage this implementation will be at a deficit right off the bat, which is
 * perhaps suboptimal.
 */
public class CurrentFormat implements SimplePropertyStoreAbstraction
{
    private final PropertyStore propertyStore;
    private final PropertyTraverser propertyTraverser;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeletor;
    private final DirectRecordAccess<PropertyRecord,PrimitiveRecord> recordAccess;
    private final NeoStores neoStores;

    public CurrentFormat( PageCache pageCache, FileSystemAbstraction fs, File directory )
    {
        StoreFactory storeFactory = new StoreFactory( directory, Config.defaults(), new DefaultIdGeneratorFactory( fs ),
                pageCache, fs, NullLogProvider.getInstance() );
        neoStores = storeFactory.openNeoStores( true,
                StoreType.PROPERTY_KEY_TOKEN_NAME, StoreType.PROPERTY_KEY_TOKEN,
                StoreType.PROPERTY_ARRAY, StoreType.PROPERTY_STRING, StoreType.PROPERTY );
        this.propertyStore = neoStores.getPropertyStore();
        this.propertyTraverser = new PropertyTraverser();
        this.propertyCreator = new PropertyCreator( propertyStore, propertyTraverser );
        this.propertyDeletor = new PropertyDeleter( propertyTraverser );
        this.recordAccess = new DirectRecordAccess<>( propertyStore, propertyLoader( propertyStore ) );
    }

    // Writing
    @Override
    public long set( long id, final int key, Value value )
    {
        Owner owner = new Owner( id );
        propertyCreator.primitiveSetProperty( owner, key, value, recordAccess );
        recordAccess.commit();
        return owner.getNextProp();
    }

    @Override
    public boolean remove( long id, int key )
    {
        Owner owner = new Owner( id );
        propertyDeletor.removeProperty( owner, key, recordAccess );
        return owner.getNextProp() == Record.NULL_REFERENCE.intValue();
    }

    // Reading
    @Override
    public boolean has( long id, final int key )
    {
        NodeRecord owner = new NodeRecord( -1 );
        owner.setNextProp( id );
        return propertyTraverser.findPropertyRecordContaining( owner, key, recordAccess, false ) !=
                NULL_REFERENCE.intValue();
    }

    @Override
    public boolean getAlthoughNotReally( long id, int key )
    {
        NodeRecord node = new NodeRecord( 0 ).initialize( true, id, false, -1, 0 );
        long foundRecordId = propertyTraverser.findPropertyRecordContaining( node, key, recordAccess, false );
        return !NULL_REFERENCE.is( foundRecordId );
    }

    @Override
    public int all( final long id, final PropertyVisitor visitor )
    {
        PropertyListener collector = new PropertyListener( id, visitor );
        propertyTraverser.getPropertyChain( id, recordAccess, collector );
        return collector.count;
    }

    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

    private static class PropertyListener implements Listener<PropertyBlock>
    {
        private final long id;
        private final PropertyVisitor visitor;
        int count;

        PropertyListener( long id, PropertyVisitor visitor )
        {
            this.id = id;
            this.visitor = visitor;
        }

        @Override
        public void receive( PropertyBlock notification )
        {
            // TODO using id here ain't correct though
            visitor.accept( id, notification.getKeyIndexId() );
            count++;
        }
    }

    private static class Owner extends NodeRecord implements RecordProxy<NodeRecord,Void>
    {
        public Owner( long firstProp )
        {
            super( -1 );
            setNextProp( firstProp );
        }

        @Override
        public long getKey()
        {
            return getId();
        }

        @Override
        public NodeRecord forChangingLinkage()
        {
            return this;
        }

        @Override
        public NodeRecord forChangingData()
        {
            return this;
        }

        @Override
        public NodeRecord forReadingLinkage()
        {
            return this;
        }

        @Override
        public NodeRecord forReadingData()
        {
            return this;
        }

        @Override
        public Void getAdditionalData()
        {
            return null;
        }

        @Override
        public NodeRecord getBefore()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isChanged()
        {
            return false;
        }
    }

    @Override
    public void close() throws IOException
    {
        neoStores.close();
    }
}
