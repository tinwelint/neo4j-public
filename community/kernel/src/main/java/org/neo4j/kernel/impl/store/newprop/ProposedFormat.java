package org.neo4j.kernel.impl.store.newprop;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.newprop.Store.RecordVisitor;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.Value;

import static java.lang.Integer.bitCount;
import static java.lang.Integer.highestOneBit;
import static java.lang.Integer.max;

public class ProposedFormat implements SimplePropertyStoreAbstraction
{
    private final Store store;

    public ProposedFormat( PageCache pageCache, File directory ) throws IOException
    {
        this.store = new Store( pageCache, directory, "main", 64 );
    }

    @Override
    public long set( long id, int key, Value value )
    {
        if ( Record.NULL_REFERENCE.is( id ) )
        {
            // Allocate room, 2 units to start off with and then binary increase when growing.
            // In a real scenario we'd probably have a method setting multiple properties and so
            // we'd know how big our record would be right away. This is just to prototype the design
            id = store.allocate( 2 );
        }
        // Read header and see if property by the given key already exists
        // For now let's store the number of header entries as a 2B entry first
        store.accessForWriting( id, new Visitor()
        {
            @Override
            public long accept( PageCursor cursor )
            {
                if ( seek( cursor, key ) )
                {
                    // Change property value
                    throw new UnsupportedOperationException( "TODO implement changing a property value" );
                }
                // OK, so we'd like to add this property
                Type type = Type.fromValue( value );
                int newNumberOfHeaderEntries = numberOfHeaderEntries + type.numberOfHeaderEntries();
                if ( newNumberOfHeaderEntries > numberOfHeaderEntriesRoom )
                {
                    // If the bit count in the header entries count is one it means that we're at the edge
                    // and the addition of this new property must grow the header, i.e. the record as well
                    // Also, update the id and offsets here so that they can get properly used below
                    throw new UnsupportedOperationException( "TODO implement record relocation/growth" );
                }
                // Here assume that we're at the correct position to add a new header
                type.putHeader( cursor, key, relativeValueOffset );
                cursor.setOffset( pivotOffset );
                cursor.putShort( (short) newNumberOfHeaderEntries ); // TODO safe cast
                // Now jump to the correct value offset and write the value
                placeCursorAtValueStart( cursor );
                type.putValue( cursor, value );
                return -1; // TODO support records spanning multiple pages
            }
        } );
        return id;
    }

    @Override
    public boolean remove( long id, int key )
    {
        return false;
    }

    @Override
    public boolean has( long id, int key )
    {
        Visitor visitor = new Visitor()
        {
            @Override
            public long accept( PageCursor cursor )
            {
                booleanState = seek( cursor, key );
                return -1;
            }
        };
        store.accessForReading( id, visitor );
        return visitor.booleanState;
    }

    @Override
    public boolean getAlthoughNotReally( long id, int key )
    {
        return false;
    }

    @Override
    public int all( long id, PropertyVisitor visitor )
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {
        store.close();
    }

    abstract class Visitor implements RecordVisitor
    {
        protected int pivotOffset, numberOfHeaderEntries, numberOfHeaderEntriesRoom, relativeValueOffset, valueLength;
        protected Type type;
        protected boolean booleanState;

        boolean seek( PageCursor cursor, int key )
        {
            pivotOffset = cursor.getOffset();
            numberOfHeaderEntries = cursor.getShort();
            numberOfHeaderEntriesRoom = calculateMaxNumberOfHeaderEntries( numberOfHeaderEntries );
            relativeValueOffset = 0; // relative to the first value
            valueLength = 0;
            for ( int i = 0; i < numberOfHeaderEntries; i++ )
            {
                long headerEntry = getUnsignedInt( cursor );
                type = Type.fromHeader( headerEntry );
                int thisKey = type.keyOf( headerEntry );
                if ( thisKey == key )
                {
                    return true;
                }
                valueLength = type.valueLength();
                relativeValueOffset += valueLength;
            }
            return false;
        }

        void placeCursorAtValueStart( PageCursor cursor )
        {
            cursor.setOffset( pivotOffset + 2/*2B number of header entries*/ + numberOfHeaderEntriesRoom * 4
                    + relativeValueOffset );
        }

        // There must be a cleaner way of implementing this calculation
        private int calculateMaxNumberOfHeaderEntries( int numberOfHeaderEntries )
        {
            int highBit = 0;
            switch ( bitCount( numberOfHeaderEntries ) )
            {
            case 1:
                break;
            default:
                highBit = highestOneBit( numberOfHeaderEntries << 1 );
                break;
            }
            return 1 << max( 2, highBit );
        }
    }

    private enum Type
    {
        TRUE( 0 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            { // No need, the type is the value
            }
        },
        FALSE( 0 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            { // No need, the type is the value
            }
        },
        INT8( 1 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            {
                cursor.putByte( (byte) ((IntegralValue)value).longValue() );
            }
        },
        INT16( 2 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            {
                cursor.putShort( (short) ((IntegralValue)value).longValue() );
            }
        },
        INT32( 4 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            {
                cursor.putInt( (int) ((IntegralValue)value).longValue() );
            }
        },
        INT64( 8 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            {
                cursor.putLong( ((IntegralValue)value).longValue() );
            }
        };
        // ...TODO more types here

        public static Type[] ALL = values(); // to avoid clone() every time
        private final int valueLength;

        public static Type fromHeader( long headerEntryUnsignedInt )
        {
            int ordinal = (int) ((headerEntryUnsignedInt & 0xFF000000) >>> 24);
            return ALL[ordinal];
        }

        public abstract void putValue( PageCursor cursor, Value value );

        public int numberOfHeaderEntries()
        {
            return 1;
        }

        public static Type fromValue( Value value )
        {
            if ( value == null )
            {
                throw new IllegalArgumentException( "null value" );
            }
            if ( value instanceof BooleanValue )
            {
                return ((BooleanValue)value).booleanValue() ? Type.TRUE : Type.FALSE;
            }
            if ( value instanceof IntegralValue )
            {
                long longValue = ((IntegralValue)value).longValue();
                if ( (longValue & ~0xFF) == 0 )
                {
                    return INT8;
                }
                if ( (longValue & ~0xFFFF) == 0 )
                {
                    return INT16;
                }
                if ( (longValue & ~0xFFFFFFFF) == 0 )
                {
                    return INT32;
                }
                return INT64;
            }
            throw new UnsupportedOperationException( "Unfortunately values like " + value + " which is of type "
                    + value.getClass() + " aren't supported a.t.m." );
        }

        private Type( int valueLength )
        {
            this.valueLength = valueLength;
        }

        public int valueLength()
        {
            return valueLength;
        }

        public void putHeader( PageCursor cursor, int key, int relativeValueOffset )
        {
            int keyAndType = ordinal() << 24 | key;
            cursor.putInt( keyAndType );
            // TODO a variable length type would also put an additional header entry containing the offset
        }

        public int keyOf( long headerEntryUnsignedInt )
        {
            return (int) (headerEntryUnsignedInt & 0xFFFFFF);
        }
    }

    static long getUnsignedInt( PageCursor cursor )
    {
        return cursor.getInt() & 0xFFFFFFFFL;
    }
}
