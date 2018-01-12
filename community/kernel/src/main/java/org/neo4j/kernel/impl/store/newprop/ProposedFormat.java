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
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.UTF8StringValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNIT_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.offsetForId;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.pageIdForRecord;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;

public class ProposedFormat implements SimplePropertyStoreAbstraction
{
    static final int HEADER_ENTRY_SIZE = Integer.BYTES;
    static final int RECORD_HEADER_SIZE = Short.BYTES;

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
        Visitor visitor = new Visitor()
        {
            @Override
            public long accept( PageCursor cursor, long startId, int units ) throws IOException
            {
                longState = startId;
                int recordLength = units * UNIT_SIZE;
                if ( seek( cursor, key ) )
                {
                    // Change property value
                    Type oldType = currentType;
                    int oldValueLength = currentValueLength;
                    int oldValueLengthSum = sumValueLength;
                    int hitHeaderEntryIndex = headerEntryIndex;
                    seekToEnd( cursor );
                    int freeBytesInRecord = recordLength - sumValueLength - headerLength;

                    Type type = Type.fromValue( value );
                    Object preparedValue = type.prepare( value );
                    int headerDiff = type.numberOfHeaderEntries() - oldType.numberOfHeaderEntries();
                    int newValueLength = type.valueLength( preparedValue );
                    int diff = newValueLength - oldValueLength;
                    int growth = headerDiff * HEADER_ENTRY_SIZE + diff;
                    while ( growth > freeBytesInRecord )
                    {
                        // TODO grow once instead
                        units = growRecord( cursor, startId, units );
                        startId = longState;
                        seek( cursor, -1 );
                        recordLength = units * UNIT_SIZE;
                        freeBytesInRecord = recordLength - sumValueLength - headerLength;
                    }

                    if ( headerDiff > 0 )
                    {
                        // Grow, i.e. move the other header entries diff entries to the right (headers are written from the start)
                        int leftOffset = headerStart( hitHeaderEntryIndex + oldType.numberOfHeaderEntries() );
                        int rightOffset = headerStart( numberOfHeaderEntries );
                        for ( int i = rightOffset - HEADER_ENTRY_SIZE; i >= leftOffset; i -= HEADER_ENTRY_SIZE )
                        {
                            cursor.copyTo( i, cursor, i + HEADER_ENTRY_SIZE * headerDiff, HEADER_ENTRY_SIZE );
                        }
                    }
                    else if ( headerDiff < 0 )
                    {
                        // Shrink, i.e. move the other header entries diff entries to the left
                        int leftOffset = headerStart( hitHeaderEntryIndex + oldType.numberOfHeaderEntries() );
                        int rightOffset = headerStart( numberOfHeaderEntries );
                        for ( int i = leftOffset; i < rightOffset; i += HEADER_ENTRY_SIZE )
                        {
                            cursor.copyTo( i, cursor, i + HEADER_ENTRY_SIZE * headerDiff, HEADER_ENTRY_SIZE );
                        }
                    }
                    if ( headerDiff != 0 )
                    {
                        numberOfHeaderEntries += headerDiff;
                        writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries );
                    }

                    // Back up to the beginning of the header
                    if ( type != oldType || newValueLength != oldValueLength )
                    {
                        cursor.setOffset( headerStart( hitHeaderEntryIndex ) );
                        type.putHeader( cursor, key, oldValueLengthSum, preparedValue );
                    }

                    // Grow/shrink the space for the new value
                    if ( diff > 0 )
                    {
                        // Grow, i.e. move the other values diff bytes to the left (values are written from the end)
                        int leftOffset = valueStart( units, sumValueLength );
                        int rightOffset = valueStart( units, oldValueLengthSum );
                        for ( int i = leftOffset; i < rightOffset; i++ )
                        {
                            // TODO move in bigger pieces
                            cursor.copyTo( i, cursor, i - diff, 1 );
                        }
                    }
                    else if ( diff < 0 )
                    {
                        // Shrink, i.e. move the other values diff bytes to the right (values are written from the end)
                        int leftOffset = valueStart( units, sumValueLength );
                        int rightOffset = valueStart( units, oldValueLengthSum );
                        for ( int i = rightOffset - 1; i >= leftOffset; i-- )
                        {
                            // TODO move in bigger pieces
                            cursor.copyTo( i, cursor, i - diff, 1 );
                        }
                    }
                    oldValueLengthSum += diff;

                    // Go the the correct value position and write the value
                    cursor.setOffset( valueStart( units, oldValueLengthSum ) );
                    type.putValue( cursor, preparedValue );
                }
                else
                {
                    // OK, so we'd like to add this property.
                    // How many bytes do we have available in this record?
                    // Formula is: start of left-most value  -  end of right-most header
                    int freeBytesInRecord = recordLength - sumValueLength - headerLength;
                    Type type = Type.fromValue( value );
                    Object preparedValue = type.prepare( value );
                    if ( type.numberOfHeaderEntries() * HEADER_ENTRY_SIZE + type.valueLength( preparedValue ) > freeBytesInRecord )
                    {   // Grow/relocate record
                        units = growRecord( cursor, startId, units );
                        // Perhaps unnecessary to call seek again, the point of it is to leave the cursor in the
                        // expected place for insert, just as it would have been if we wouldn't have grown the record
                        // -- code simplicity
                        boolean found = seek( cursor, key );
                        assert !found;
                    }

                    // Here assume that we're at the correct position to add a new header
                    int newNumberOfHeaderEntries = numberOfHeaderEntries + type.numberOfHeaderEntries();
                    type.putHeader( cursor, key, sumValueLength, preparedValue );
                    cursor.setOffset( valueStart( units, sumValueLength + type.valueLength( preparedValue ) ) );
                    type.putValue( cursor, preparedValue );
                    writeNumberOfHeaderEntries( cursor, newNumberOfHeaderEntries );
                }

                return -1; // TODO support records spanning multiple pages
            }
        };
        store.accessForWriting( id, visitor );
        return visitor.longState;
    }

    @Override
    public boolean remove( long id, int key )
    {
        Visitor visitor = new Visitor()
        {
            @Override
            public long accept( PageCursor cursor, long startId, int units )
            {
                if ( booleanState = seek( cursor, key ) )
                {   // It exists
                    // Everything after current headerIndex should be moved back the number of header units this property has
                    int headerEntriesToMove = numberOfHeaderEntries - headerEntryIndex - currentType.numberOfHeaderEntries();
                    int gapSize = currentType.numberOfHeaderEntries() * HEADER_ENTRY_SIZE;
                    placeCursorAtHeaderEntry( cursor, headerEntryIndex );
                    for ( int i = 0; i < headerEntriesToMove; i++ )
                    {
                        // Assumption that header entry is integer size
                        cursor.putInt( cursor.getInt( cursor.getOffset() + gapSize ) );
                    }
                    writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries - currentType.numberOfHeaderEntries() );

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
            public long accept( PageCursor cursor, long startId, int units )
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
            public long accept( PageCursor cursor, long startId, int units )
            {
                if ( seek( cursor, key ) )
                {   // found
                    cursor.setOffset( valueStart( units, sumValueLength ) );
                    readValue = currentType.getValue( cursor, currentValueLength );
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
            public long accept( PageCursor cursor, long startId, int units )
            {
                if ( seek( cursor, key ) )
                {   // found
                    longState = currentValueLength;
                }
                return -1;
            }
        };
        store.accessForReading( id, visitor );
        return (int) visitor.longState;
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
        protected int pivotOffset;
        protected int numberOfHeaderEntries;
        protected int sumValueLength;
        protected int currentValueLength;
        protected int headerEntryIndex;
        protected int headerLength;
        protected Type currentType;
        protected boolean booleanState;
        protected long longState;
        protected Value readValue;

        boolean seek( PageCursor cursor, int key )
        {
            pivotOffset = cursor.getOffset();
            numberOfHeaderEntries = cursor.getShort();
            sumValueLength = 0;
            currentValueLength = 0;
            headerEntryIndex = 0;
            headerLength = RECORD_HEADER_SIZE + numberOfHeaderEntries * HEADER_ENTRY_SIZE;
            return seekTo( cursor, key );
        }

        private boolean seekTo( PageCursor cursor, int key )
        {
            while ( headerEntryIndex < numberOfHeaderEntries )
            {
                long headerEntry = getUnsignedInt( cursor );
                currentType = Type.fromHeader( headerEntry );
                int thisKey = currentType.keyOf( headerEntry );

                // TODO don't rely on keys being ordered... matters much? We have to look at all of them anyway to figure out free space
                // (unless we keep free space as a dedicated field or something)
//                if ( thisKey > key )
//                {
//                    // We got too far, i.e. this key doesn't exist
//                    // We leave offsets at the start of this header entry so that insert can insert right there
//                    break;
//                }

                currentValueLength = currentType.valueLength( cursor );
                sumValueLength += currentValueLength;
                if ( thisKey == key )
                {
                    // valueLength == length of found value
                    // relativeValueOffset == relative start of this value, i.e.
                    // actual page offset == pivotOffset + recordSize - relativeValueOffset
                    return true;
                }

                headerEntryIndex += currentType.numberOfHeaderEntries();
            }
            return false;
        }

        void seekToEnd( PageCursor cursor )
        {
            // TODO hacky thing here with the headerEntryIndex, better to increment before return in seek?
            headerEntryIndex += currentType.numberOfHeaderEntries();
            seekTo( cursor, -1 );
        }

        int growRecord( PageCursor cursor, long startId, int units ) throws IOException
        {
            // TODO Special case: can we grow in-place?

            // Normal case: find new bigger place and move there.
            int newUnits = units * 2;
            long newStartId = longState = store.allocate( newUnits );
            long newPageId = pageIdForRecord( newStartId );
            int newOffset = offsetForId( newStartId );
            try ( PageCursor newCursor = cursor.openLinkedCursor( newPageId ) )
            {
                newCursor.next();
                newCursor.setOffset( newOffset );
                // Copy header
                int fromBase = pivotOffset;
                int toBase = newCursor.getOffset();
                cursor.copyTo( fromBase, newCursor, toBase, headerLength );
                // Copy values
                cursor.copyTo(
                        fromBase + units * UNIT_SIZE - sumValueLength, newCursor,
                        toBase + newUnits * UNIT_SIZE - sumValueLength, sumValueLength );
            }
            Header.mark( cursor, startId, units, false );
            cursor.next( newPageId );
            cursor.setOffset( newOffset );
            return newUnits;
        }

        int valueStart( int units, int valueOffset )
        {
            return pivotOffset + (units * UNIT_SIZE) - valueOffset;
        }

        int headerStart( int headerEntryIndex )
        {
            return pivotOffset + RECORD_HEADER_SIZE + headerEntryIndex * HEADER_ENTRY_SIZE;
        }

        void placeCursorAtHeaderEntry( PageCursor cursor, int headerEntryIndex )
        {
            cursor.setOffset( pivotOffset + RECORD_HEADER_SIZE + HEADER_ENTRY_SIZE * headerEntryIndex );
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
            public void putValue( PageCursor cursor, Object value )
            { // No need, the type is the value
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return booleanValue( true );
            }
        },
        FALSE( 0 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value )
            { // No need, the type is the value
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return booleanValue( false );
            }
        },
        INT8( 1 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value )
            {
                cursor.putByte( (byte) ((IntegralValue)value).longValue() );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return byteValue( cursor.getByte() );
            }
        },
        INT16( 2 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value )
            {
                cursor.putShort( (short) ((IntegralValue)value).longValue() );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return shortValue( cursor.getShort() );
            }
        },
        // INT24?
        INT32( 4 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value )
            {
                cursor.putInt( (int) ((IntegralValue)value).longValue() );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return intValue( cursor.getInt() );
            }
        },
        // INT40?
        // INT48?
        // INT56?
        INT64( 8 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value )
            {
                cursor.putLong( ((IntegralValue)value).longValue() );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return longValue( cursor.getLong() );
            }
        },
        UTF8_STRING( -1 )
        {
            // TODO introduce a way to stream the value
            @Override
            public Object prepare( Value value )
            {
                if ( value instanceof UTF8StringValue )
                {
                    return ((UTF8StringValue)value).bytes();
                }
                return UTF8.encode( ((TextValue)value).stringValue() );
            }

            @Override
            public int valueLength( PageCursor cursor )
            {
                return cursor.getInt();
            }

            @Override
            public int valueLength( Object preparedValue )
            {
                return ((byte[])preparedValue).length;
            }

            @Override
            public void putValue( PageCursor cursor, Object preparedValue )
            {
                // TODO this implementation is only suitable for the first iteration of this property store where
                // there's a limit that all property data for an entity must fit in one page, one page being 8192 - 64 max size
                // Therefore this format doesn't add any sort of chunking or value header saying how many bytes are in this
                // page and potentially a link to next page or something like that.

                cursor.putBytes( (byte[]) preparedValue );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                byte[] bytes = new byte[valueLength];
                cursor.getBytes( bytes );
                return Values.utf8Value( bytes );
            }

            @Override
            public int numberOfHeaderEntries()
            {
                return 2;
            }

            @Override
            public void putHeader( PageCursor cursor, int key, int valueOffset, Object preparedValue )
            {
                // Put normal header entry
                super.putHeader( cursor, key, valueOffset, preparedValue );
                // Put value length header entry
                cursor.putInt( valueLength( preparedValue ) );
            }
        }
        ;
        // ...TODO more types here
        // General thoughts on array values: User probably expects the same type of array back as was sent in,
        // i.e. more strongly typed than simple numeric values. It would be great with specific types for each
        // type of array, like BOOLEAN_ARRAY, BYTE_ARRAY, INT_ARRAY and so forth. Internally they could take advantage
        // of INT8/16/32 whatever compression anyway.

        public static Type[] ALL = values(); // to avoid clone() every time
        private final int valueLength;

        public static Type fromHeader( long headerEntryUnsignedInt )
        {
            int ordinal = (int) ((headerEntryUnsignedInt & 0xFF000000) >>> 24);
            return ALL[ordinal];
        }

        public Object prepare( Value value )
        {
            return value;
        }

        public abstract void putValue( PageCursor cursor, Object preparedValue );

        public abstract Value getValue( PageCursor cursor, int valueLength );

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
            if ( value instanceof TextValue )
            {
                // TODO match string with short-string encodings before falling back to UTF-8
                return UTF8_STRING;
            }
            throw new UnsupportedOperationException( "Unfortunately values like " + value + " which is of type "
                    + value.getClass() + " aren't supported a.t.m." );
        }

        private Type( int valueLength )
        {
            this.valueLength = valueLength;
        }

        public int valueLength( PageCursor cursor )
        {
            return valueLength;
        }

        public int valueLength( Object preparedValue )
        {
            return valueLength;
        }

        public void putHeader( PageCursor cursor, int key, int valueOffset, Object preparedValue )
        {
            int keyAndType = ordinal() << 24 | key;
            cursor.putInt( keyAndType );
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
