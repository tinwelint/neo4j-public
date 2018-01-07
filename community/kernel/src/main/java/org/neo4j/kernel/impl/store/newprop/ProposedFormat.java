/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.values.storable.Values;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNIT_SIZE;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;

public class ProposedFormat implements SimplePropertyStoreAbstraction
{
    static final int HEADER_ENTRY_SIZE = Integer.BYTES;

    private final Store store;

    public ProposedFormat( PageCache pageCache, File directory ) throws IOException
    {
        this.store = new Store( pageCache, directory, "main" );
    }

    @Override
    public long set( long id, int key, Value value ) throws IOException
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
            public long accept( PageCursor cursor, int units )
            {
                if ( seek( cursor, key ) )
                {
                    // Change property value
                    throw new UnsupportedOperationException( "TODO implement changing a property value" );
                }

                // OK, so we'd like to add this property.
                // How many bytes do we have available in this record?
                // Formula is: start of left-most value  -  end of right-most header
                int freeBytesInRecord = (units * UNIT_SIZE - relativeValueOffset) - numberOfHeaderEntries * HEADER_ENTRY_SIZE;
                Type type = Type.fromValue( value );
                if ( type.numberOfHeaderEntries() * HEADER_ENTRY_SIZE + type.valueLength() > freeBytesInRecord )
                {
                    throw new UnsupportedOperationException( "TODO implement grow/relocate" );
                }

                int newNumberOfHeaderEntries = numberOfHeaderEntries + type.numberOfHeaderEntries();
                // Here assume that we're at the correct position to add a new header
                type.putHeader( cursor, key, relativeValueOffset );
                writeNumberOfHeaderEntries( cursor, newNumberOfHeaderEntries );
                // Back up valueLength bytes so that we start writing the value in the correct place
                relativeValueOffset += type.valueLength();
                // Now jump to the correct value offset and write the value
                placeCursorAtValueStart( cursor, units );
                type.putValue( cursor, value );
                return -1; // TODO support records spanning multiple pages
            }
        } );
        return id;
    }

    @Override
    public boolean remove( long id, int key )
    {
        Visitor visitor = new Visitor()
        {
            @Override
            public long accept( PageCursor cursor, int units )
            {
                if ( booleanState = seek( cursor, key ) )
                {   // It exists
                    // Everything after current headerIndex should be moved back the number of header units this property has
                    int headerEntriesToMove = numberOfHeaderEntries - headerEntryIndex - type.numberOfHeaderEntries();
                    int gapSize = type.numberOfHeaderEntries() * HEADER_ENTRY_SIZE;
                    placeCursorAtHeaderEntry( cursor, headerEntryIndex );
                    for ( int i = 0; i < headerEntriesToMove; i++ )
                    {
                        // Assumption that header entry is integer size
                        cursor.putInt( cursor.getInt( cursor.getOffset() + gapSize ) );
                    }
                    writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries - type.numberOfHeaderEntries() );

                    // TODO also implement compacting of the value data
                }
                return -1;
            }
        };
        store.accessForWriting( id, visitor );
        return visitor.booleanState;
    }

    @Override
    public boolean has( long id, int key )
    {
        Visitor visitor = new Visitor()
        {
            @Override
            public long accept( PageCursor cursor, int units )
            {
                booleanState = seek( cursor, key );
                return -1;
            }
        };
        store.accessForReading( id, visitor );
        return visitor.booleanState;
    }

    @Override
    public Value get( long id, int key )
    {
        Visitor visitor = new Visitor()
        {
            @Override
            public long accept( PageCursor cursor, int units )
            {
                booleanState = seek( cursor, key );
                if ( booleanState )
                {   // found
                    placeCursorAtValueStart( cursor, units );
                    readValue = type.getValue( cursor );
                }
                return -1;
            }
        };
        store.accessForReading( id, visitor );
        return visitor.readValue != null ? visitor.readValue : Values.NO_VALUE;
    }

    @Override
    public int getWithoutDeserializing( long id, int key )
    {
        Visitor visitor = new Visitor()
        {
            @Override
            public long accept( PageCursor cursor, int units )
            {
                booleanState = seek( cursor, key );
                if ( booleanState )
                {   // found
                    placeCursorAtValueStart( cursor, units );
                    intState = type.valueLength();
                }
                return -1;
            }
        };
        store.accessForReading( id, visitor );
        return visitor.intState;
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
        protected int pivotOffset, numberOfHeaderEntries, relativeValueOffset, headerEntryIndex;
        protected Type type;
        protected boolean booleanState;
        protected int intState;
        protected Value readValue;

        boolean seek( PageCursor cursor, int key )
        {
            pivotOffset = cursor.getOffset();
            numberOfHeaderEntries = cursor.getShort();
            relativeValueOffset = 0; // relative to size of this whole record, all units (since values start from the end)
            headerEntryIndex = 0;
            while ( headerEntryIndex < numberOfHeaderEntries )
            {
                long headerEntry = getUnsignedInt( cursor );
                type = Type.fromHeader( headerEntry );
                int thisKey = type.keyOf( headerEntry );

                // TODO don't rely on keys being ordered... matters much? We have to look at all of them anyway to figure out free space
                // (unless we keep free space as a dedicated field or something)
//                if ( thisKey > key )
//                {
//                    // We got too far, i.e. this key doesn't exist
//                    // We leave offsets at the start of this header entry so that insert can insert right there
//                    break;
//                }

                relativeValueOffset += type.valueLength();
                if ( thisKey == key )
                {
                    // valueLength == length of found value
                    // relativeValueOffset == relative start of this value, i.e.
                    // actual page offset == pivotOffset + recordSize - relativeValueOffset
                    return true;
                }

                headerEntryIndex += type.numberOfHeaderEntries();
            }
            return false;
        }

        void placeCursorAtValueStart( PageCursor cursor, int units )
        {
            cursor.setOffset( pivotOffset + (units * UNIT_SIZE) - relativeValueOffset );
        }

        void placeCursorAtHeaderEntry( PageCursor cursor, int headerEntryIndex )
        {
            cursor.setOffset( pivotOffset + Short.BYTES + HEADER_ENTRY_SIZE * headerEntryIndex );
        }

        void writeNumberOfHeaderEntries( PageCursor cursor, int newNumberOfHeaderEntries )
        {
            cursor.putShort( pivotOffset, (short) newNumberOfHeaderEntries ); // TODO safe cast
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

            @Override
            public Value getValue( PageCursor cursor )
            {
                return booleanValue( true );
            }
        },
        FALSE( 0 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            { // No need, the type is the value
            }

            @Override
            public Value getValue( PageCursor cursor )
            {
                return booleanValue( false );
            }
        },
        INT8( 1 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            {
                cursor.putByte( (byte) ((IntegralValue)value).longValue() );
            }

            @Override
            public Value getValue( PageCursor cursor )
            {
                return byteValue( cursor.getByte() );
            }
        },
        INT16( 2 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            {
                cursor.putShort( (short) ((IntegralValue)value).longValue() );
            }

            @Override
            public Value getValue( PageCursor cursor )
            {
                return shortValue( cursor.getShort() );
            }
        },
        INT32( 4 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            {
                cursor.putInt( (int) ((IntegralValue)value).longValue() );
            }

            @Override
            public Value getValue( PageCursor cursor )
            {
                return intValue( cursor.getInt() );
            }
        },
        INT64( 8 )
        {
            @Override
            public void putValue( PageCursor cursor, Value value )
            {
                cursor.putLong( ((IntegralValue)value).longValue() );
            }

            @Override
            public Value getValue( PageCursor cursor )
            {
                return longValue( cursor.getLong() );
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

        public abstract Value getValue( PageCursor cursor );

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

    static long getUnsignedInt( PageCursor cursor, int offset )
    {
        return cursor.getInt( offset ) & 0xFFFFFFFFL;
    }

    @Override
    public long storeSize()
    {
        try
        {
            return store.storeFile.getLastPageId() * kibiBytes( 8 );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
