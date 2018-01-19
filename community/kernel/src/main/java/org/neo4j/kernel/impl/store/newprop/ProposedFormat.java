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
import org.neo4j.values.storable.BooleanArray;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.UTF8StringValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.lang.Math.toIntExact;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNIT_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.offsetForId;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.pageIdForRecord;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatArray;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortArray;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.Values.stringArray;

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
                    if ( growth > freeBytesInRecord )
                    {
                        units = growRecord( cursor, startId, units, growth );
                        seek( cursor, -1 );
                    }

                    // Shrink whichever part that needs shrinking first, otherwise we risk writing into the other part,
                    // i.e. parts being header and value
                    if ( headerDiff < 0 )
                    {
                        changeHeaderSize( cursor, headerDiff, oldType, hitHeaderEntryIndex );
                    }
                    if ( diff < 0 )
                    {
                        changeValueSize( cursor, diff, units, oldValueLengthSum );
                    }
                    if ( headerDiff > 0 )
                    {
                        changeHeaderSize( cursor, headerDiff, oldType, hitHeaderEntryIndex );
                    }
                    if ( diff > 0 )
                    {
                        changeValueSize( cursor, diff, units, oldValueLengthSum );
                    }

                    if ( headerDiff != 0 )
                    {
                        numberOfHeaderEntries += headerDiff;
                        writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries );
                    }
                    if ( diff != 0 )
                    {
                        oldValueLengthSum += diff;
                    }

                    if ( type != oldType || newValueLength != oldValueLength )
                    {
                        cursor.setOffset( headerStart( hitHeaderEntryIndex ) );
                        type.putHeader( cursor, key, oldValueLengthSum, preparedValue );
                    }

                    // Go the the correct value position and write the value
                    cursor.setOffset( valueStart( units, oldValueLengthSum ) );
                    type.putValue( cursor, preparedValue, newValueLength );
                }
                else
                {
                    // OK, so we'd like to add this property.
                    // How many bytes do we have available in this record?
                    // Formula is: start of left-most value  -  end of right-most header
                    int freeBytesInRecord = recordLength - sumValueLength - headerLength;
                    Type type = Type.fromValue( value );
                    Object preparedValue = type.prepare( value );
                    int valueLength = type.valueLength( preparedValue );
                    int size = type.numberOfHeaderEntries() * HEADER_ENTRY_SIZE + valueLength;
                    if ( size > freeBytesInRecord )
                    {   // Grow/relocate record
                        units = growRecord( cursor, startId, units, size - freeBytesInRecord );
                        // Perhaps unnecessary to call seek again, the point of it is to leave the cursor in the
                        // expected place for insert, just as it would have been if we wouldn't have grown the record
                        // -- code simplicity
                        boolean found = seek( cursor, key );
                        assert !found;
                    }

                    // Here assume that we're at the correct position to add a new header
                    int newNumberOfHeaderEntries = numberOfHeaderEntries + type.numberOfHeaderEntries();
                    type.putHeader( cursor, key, sumValueLength, preparedValue );
                    cursor.setOffset( valueStart( units, sumValueLength + valueLength ) );
                    type.putValue( cursor, preparedValue, valueLength );
                    writeNumberOfHeaderEntries( cursor, newNumberOfHeaderEntries );
                }

                return -1; // TODO support records spanning multiple pages
            }

            private void changeValueSize( PageCursor cursor, int diff, int units, int valueLengthSum )
            {
                int leftOffset = valueStart( units, sumValueLength );
                int rightOffset = valueStart( units, valueLengthSum );
                if ( diff > 0 )
                {
                    // Grow, i.e. move the other values diff bytes to the left (values are written from the end)
                    moveBytesLeft( cursor, leftOffset, rightOffset - leftOffset, diff );
                }
                else if ( diff < 0 )
                {
                    // Shrink, i.e. move the other values diff bytes to the right (values are written from the end)
                    moveBytesRight( cursor, leftOffset, rightOffset - leftOffset, - diff );
                }
            }

            private void changeHeaderSize( PageCursor cursor, int headerDiff, Type oldType, int hitHeaderEntryIndex )
            {
                int leftOffset = headerStart( hitHeaderEntryIndex + oldType.numberOfHeaderEntries() );
                int rightOffset = headerStart( numberOfHeaderEntries );
                if ( headerDiff > 0 )
                {
                    // Grow, i.e. move the other header entries diff entries to the right (headers are written from the start)
                    moveBytesRight( cursor, leftOffset, rightOffset - leftOffset, headerDiff * HEADER_ENTRY_SIZE );
                }
                else if ( headerDiff < 0 )
                {
                    // Shrink, i.e. move the other header entries diff entries to the left
                    moveBytesLeft( cursor, leftOffset, rightOffset - leftOffset, - headerDiff * HEADER_ENTRY_SIZE );
                }
            }
        };
        store.accessForWriting( id, visitor );
        return visitor.longState;
    }

    @Override
    public long remove( long id, int key )
    {
        Visitor visitor = new Visitor()
        {
            @Override
            public long accept( PageCursor cursor, long startId, int units )
            {
                if ( booleanState = seek( cursor, key ) )
                {   // It exists

                    int currentHeaderEntryIndex = headerEntryIndex;
                    int currentNumberOfHeaderEntries = currentType.numberOfHeaderEntries();
                    int headerEntriesToMove = numberOfHeaderEntries - currentHeaderEntryIndex - currentNumberOfHeaderEntries;
                    int headerDistance = currentNumberOfHeaderEntries * HEADER_ENTRY_SIZE;
                    int currentSumValueLength = sumValueLength;
                    int valueDistance = currentValueLength;

                    // Seek to the end so that we get the total value length, TODO could be done better in some way, right?
                    seekToEnd( cursor );

                    int valueSize = sumValueLength - currentSumValueLength;
                    int valueLowOffset = valueStart( units, sumValueLength );

                    // Move header entries
                    moveBytesLeft( cursor, headerStart( currentHeaderEntryIndex ) + headerDistance,
                            headerEntriesToMove * HEADER_ENTRY_SIZE, headerDistance );
                    writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries - currentNumberOfHeaderEntries );

                    // Move data entries
                    if ( valueDistance > 0 ) // distance == 0 for e.g. boolean values
                    {
                        moveBytesRight( cursor, valueLowOffset, valueSize, valueDistance );
                    }
                }
                return -1;
            }
        };
        store.accessForWriting( id, visitor );
        return id;
    }

    private void moveBytesLeft( PageCursor cursor, int lowOffset, int size, int distance )
    {
        // TODO obviously optimize this to use the good approach of an optimal chunk size to read into intermediate byte[]
        // TODO for now just do the silly move-by-gapSize
        int moved = 0;
        while ( moved < size )
        {
            int toMoveThisTime = min( size - moved, distance );
            int sourceOffset = lowOffset + moved;
            cursor.copyTo( sourceOffset, cursor, sourceOffset - distance, toMoveThisTime );
            moved += toMoveThisTime;
        }
    }

    private void moveBytesRight( PageCursor cursor, int lowOffset, int size, int distance )
    {
        // TODO obviously optimize this to use the good approach of an optimal chunk size to read into intermediate byte[]
        // TODO for now just do the silly move-by-gapSize
        int moved = 0;
        while ( moved < size )
        {
            int toMoveThisTime = min( size - moved, distance );
            int sourceOffset = lowOffset + size - moved - toMoveThisTime;
            cursor.copyTo( sourceOffset, cursor, sourceOffset + distance, toMoveThisTime );
            moved += toMoveThisTime;
        }
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

        int growRecord( PageCursor cursor, long startId, int units, int bytesNeeded ) throws IOException
        {
            // TODO Special case: can we grow in-place?

            // Normal case: find new bigger place and move there.
            int unitsNeeded = max( units, (bytesNeeded - 1) / 64 + 1 );
            int newUnits = units + unitsNeeded;
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
            cursor.setOffset( headerStart( headerEntryIndex ) );
        }

        void writeNumberOfHeaderEntries( PageCursor cursor, int newNumberOfHeaderEntries )
        {
            cursor.putShort( pivotOffset, (short) newNumberOfHeaderEntries ); // TODO safe cast
        }
    }

    private enum Type
    {
        TRUE( true, 0 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            { // No need, the type is the value
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return booleanValue( true );
            }
        },
        FALSE( true, 0 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            { // No need, the type is the value
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return booleanValue( false );
            }
        },
        INT8( true, Byte.BYTES )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            {
                cursor.putByte( (byte) ((IntegralValue)value).longValue() );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return byteValue( cursor.getByte() );
            }
        },
        INT16( true, Short.BYTES )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
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
        INT32( true, Integer.BYTES )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
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
        INT64( true, Long.BYTES )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            {
                cursor.putLong( ((IntegralValue)value).longValue() );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return longValue( cursor.getLong() );
            }
        },
        FLOAT( true, Float.BYTES )
        {
            @Override
            public void putValue( PageCursor cursor, Object preparedValue, int valueLength )
            {
                putValue( cursor, ((FloatValue)preparedValue).value() );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return floatValue( Float.intBitsToFloat( cursor.getInt() ) );
            }

            void putValue( PageCursor cursor, float value )
            {
                cursor.putInt( Float.floatToIntBits( value ) );
            }
        },
        DOUBLE( true, Double.BYTES )
        {
            @Override
            public void putValue( PageCursor cursor, Object preparedValue, int valueLength )
            {
                cursor.putLong( Double.doubleToLongBits( ((DoubleValue)preparedValue).value() ) );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                return doubleValue( Double.longBitsToDouble( cursor.getLong() ) );
            }
        },
        UTF8_STRING( false, -1 )
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
            public int valueLength( Object preparedValue )
            {
                return ((byte[])preparedValue).length;
            }

            @Override
            public void putValue( PageCursor cursor, Object preparedValue, int valueLength )
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
        },
        // FORMAT:
        // - 1 byte to say how many values are used in the last byte
        // - 1 bit per boolean
        BOOLEAN_ARRAY( false, -1 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            {
                BooleanArray array = (BooleanArray) value;
                int length = array.length();
                byte rest = (byte) (length % Byte.SIZE);
                cursor.putByte( rest == 0 ? 8 : rest );
                for ( int offset = 0; offset < length; )
                {
                    byte currentByte = 0;
                    for ( int bit = 0; bit < Byte.SIZE && offset < length; bit++, offset++ )
                    {
                        if ( array.booleanValue( offset ) )
                        {
                            currentByte |= 1 << bit;
                        }
                    }
                    cursor.putByte( currentByte );
                }
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                byte valuesInLastByte = cursor.getByte();
                int length = (valueLength - 2 /*the first header byte we just read*/) * Byte.SIZE + valuesInLastByte;
                boolean[] array = new boolean[length];
                for ( int offset = 0; offset < length; )
                {
                    byte currentByte = cursor.getByte();
                    for ( int bit = 0; bit < Byte.SIZE && offset < length; bit++, offset++ )
                    {
                        if ( (currentByte & (1 << bit)) != 0 )
                        {
                            array[offset] = true;
                        }
                    }
                }
                return booleanArray( array );
            }

            @Override
            public int valueLength( Object value )
            {
                BooleanArray array = (BooleanArray) value;
                return 1 +                                   // the byte saying how many bits are used in the last byte
                       (array.length() - 1) / Byte.SIZE + 1; // all the boolean bits
            }
        },
        BYTE_ARRAY( false, -1 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            {
                ByteArray array = (ByteArray) value;
                int length = toIntExact( array.length() );
                for ( int offset = 0; offset < length; offset++ )
                {
                    cursor.putByte( (byte) array.longValue( offset ) );
                }
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                byte[] array = new byte[valueLength];
                cursor.getBytes( array );
                return byteArray( array );
            }

            @Override
            public int valueLength( Object value )
            {
                return toIntExact( ((ByteArray) value).length() );
            }
        },
        // TODO Perhaps use some famous number array compression library out there instead?
        SHORT_ARRAY( false, -1 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            {
                putIntegralArray( cursor, (IntegralArray) value, INT16.ordinal() );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                int minimalType = cursor.getByte();
                int length = integralArrayLength( valueLength, minimalType );
                short[] array = new short[length];
                for ( int offset = 0; offset < length; offset++ )
                {
                    array[offset] = (short) getMinimalIntegralValue( cursor, minimalType );
                }
                return shortArray( array );
            }

            @Override
            public int valueLength( Object value )
            {
                return integralArrayValueLength( (IntegralArray) value, INT16.ordinal() );
            }
        },
        INT_ARRAY( false, -1 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            {
                putIntegralArray( cursor, (IntegralArray) value, INT32.ordinal() );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                int minimalType = cursor.getByte();
                int length = integralArrayLength( valueLength, minimalType );
                int[] array = new int[length];
                for ( int offset = 0; offset < length; offset++ )
                {
                    array[offset] = (int) getMinimalIntegralValue( cursor, minimalType );
                }
                return intArray( array );
            }

            @Override
            public int valueLength( Object value )
            {
                return integralArrayValueLength( (IntegralArray) value, INT32.ordinal() );
            }
        },
        LONG_ARRAY( false, -1 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            {
                putIntegralArray( cursor, (IntegralArray) value, INT64.ordinal() );
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                int minimalType = cursor.getByte();
                int length = integralArrayLength( valueLength, minimalType );
                long[] array = new long[length];
                for ( int offset = 0; offset < length; offset++ )
                {
                    array[offset] = getMinimalIntegralValue( cursor, minimalType );
                }
                return longArray( array );
            }

            @Override
            public int valueLength( Object value )
            {
                return integralArrayValueLength( (IntegralArray) value, INT64.ordinal() );
            }
        },
        FLOAT_ARRAY( false, -1 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            {
                FloatingPointArray array = (FloatingPointArray) value;
                int length = array.length();
                for ( int offset = 0; offset < length; offset++ )
                {
                    cursor.putInt( Float.floatToIntBits( (float) array.doubleValue( offset ) ) );
                }
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                int length = valueLength / Float.BYTES;
                float[] array = new float[length];
                for ( int offset = 0; offset < length; offset++ )
                {
                    array[offset] = Float.intBitsToFloat( cursor.getInt() );
                }
                return floatArray( array );
            }

            @Override
            public int valueLength( Object preparedValue )
            {
                return ((FloatingPointArray)preparedValue).length() * Float.BYTES;
            }
        },
        DOUBLE_ARRAY( false, -1 )
        {
            @Override
            public void putValue( PageCursor cursor, Object value, int valueLength )
            {
                FloatingPointArray array = (FloatingPointArray) value;
                int length = array.length();
                for ( int offset = 0; offset < length; offset++ )
                {
                    cursor.putLong( Double.doubleToLongBits( array.doubleValue( offset ) ) );
                }
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                int length = valueLength / Double.BYTES;
                double[] array = new double[length];
                for ( int offset = 0; offset < length; offset++ )
                {
                    array[offset] = Double.longBitsToDouble( cursor.getLong() );
                }
                return doubleArray( array );
            }

            @Override
            public int valueLength( Object preparedValue )
            {
                return ((FloatingPointArray)preparedValue).length() * Double.BYTES;
            }

        },
        UTF8_STRING_ARRAY( false, -1 )
        {
            @Override
            public void putValue( PageCursor cursor, Object preparedValue, int valueLength )
            {
                byte[][] bytes = (byte[][]) preparedValue;
                putLengthEfficiently( cursor, bytes.length );
                for ( byte[] stringBytes : bytes )
                {
                    cursor.putInt( stringBytes.length );
                    cursor.putBytes( stringBytes );
                }
            }

            private void putLengthEfficiently( PageCursor cursor, int length )
            {
                // TODO instead of fixed 4 bytes then have some custom field size, run-length encoding or something
                cursor.putInt( length );
            }

            private int getLengthEfficiently( PageCursor cursor )
            {
                // TODO instead of fixed 4 bytes then have some custom field size, run-length encoding or something
                return cursor.getInt();
            }

            @Override
            public Value getValue( PageCursor cursor, int valueLength )
            {
                // TODO if there were a TextArray w/ utf8... that would be juuust great
                int arrayLength = getLengthEfficiently( cursor );
                String[] strings = new String[arrayLength];
                byte[] bytes = null;
                for ( int i = 0; i < arrayLength; i++ )
                {
                    int length = cursor.getInt();
                    if ( bytes == null || bytes.length < length )
                    {
                        bytes = new byte[length];
                    }
                    cursor.getBytes( bytes, 0, length );
                    strings[i] = UTF8.decode( bytes, 0, length );
                }
                return stringArray( strings );
            }

            @Override
            public Object prepare( Value value )
            {
                TextArray array = (TextArray) value;
                int length = array.length();
                byte[][] bytes = new byte[length][];
                for ( int offset = 0; offset < length; offset++ )
                {
                    bytes[offset] = UTF8.encode( array.stringValue( offset ) );
                }
                return bytes;
            }

            @Override
            public int valueLength( Object preparedValue )
            {
                // TODO instead of 4 bytes for individual string length here then be able to have custom field size per string
                int length = Integer.BYTES;
                byte[][] bytes = (byte[][]) preparedValue;
                for ( byte[] stringBytes : bytes )
                {
                    length += Integer.BYTES + stringBytes.length;
                }
                return length;
            }
        }
        ;
        // ...TODO more types here, like point/geo?

        public static Type[] ALL = values(); // to avoid clone() every time

        private final boolean fixedSize;
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

        public abstract void putValue( PageCursor cursor, Object preparedValue, int valueLength );

        public abstract Value getValue( PageCursor cursor, int valueLength );

        public int numberOfHeaderEntries()
        {
            return fixedSize ? 1 : 2;
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
                // TODO don't let negative values use bigger INTXX than the type actually is
                long longValue = ((IntegralValue)value).longValue();
                if ( (longValue & ~0x7F) == 0 )
                {
                    return INT8;
                }
                if ( (longValue & ~0x7FFF) == 0 )
                {
                    return INT16;
                }
                if ( (longValue & ~0x7FFFFFFFL) == 0 )
                {
                    return INT32;
                }
                return INT64;
            }
            if ( value instanceof FloatValue )
            {
                return FLOAT;
            }
            if ( value instanceof DoubleValue )
            {
                return DOUBLE;
            }
            if ( value instanceof TextValue )
            {
                // TODO match string with more compressed encodings before falling back to UTF-8
                return UTF8_STRING;
            }
            if ( value instanceof BooleanArray )
            {
                return BOOLEAN_ARRAY;
            }
            if ( value instanceof ByteArray )
            {
                return BYTE_ARRAY;
            }
            if ( value instanceof ShortArray )
            {
                return SHORT_ARRAY;
            }
            if ( value instanceof IntArray )
            {
                return INT_ARRAY;
            }
            if ( value instanceof LongArray )
            {
                return LONG_ARRAY;
            }
            if ( value instanceof FloatArray )
            {
                return FLOAT_ARRAY;
            }
            if ( value instanceof DoubleArray )
            {
                return DOUBLE_ARRAY;
            }
            if ( value instanceof TextArray )
            {
                return UTF8_STRING_ARRAY;
            }
            throw new UnsupportedOperationException( "Unfortunately values like " + value + " which is of type "
                    + value.getClass() + " aren't supported a.t.m." );
        }

        private Type( boolean fixedSize, int valueLength )
        {
            this.fixedSize = fixedSize;
            this.valueLength = valueLength;
        }

        public int valueLength( PageCursor cursor )
        {
            return fixedSize ? valueLength : cursor.getInt();
        }

        public int valueLength( Object preparedValue )
        {
            return valueLength;
        }

        public void putHeader( PageCursor cursor, int key, int valueOffset, Object preparedValue )
        {
            int keyAndType = ordinal() << 24 | key;
            cursor.putInt( keyAndType );

            if ( !fixedSize )
            {
                cursor.putInt( valueLength( preparedValue ) );
            }
        }

        public int keyOf( long headerEntryUnsignedInt )
        {
            return (int) (headerEntryUnsignedInt & 0xFFFFFF);
        }

        int minimalIntegralArrayType( IntegralArray array, int maxType )
        {
            int length = array.length();
            int minimalType = 0;
            for ( int offset = 0; offset < length && minimalType < maxType; offset++ )
            {
                long candidate = array.longValue( offset );
                if ( minimalType < INT8.ordinal() && candidate != 0 )
                {
                    minimalType = INT8.ordinal();
                }
                if ( minimalType < INT16.ordinal() && (candidate & ~0xFF) != 0 )
                {
                    minimalType = INT16.ordinal();
                }
                if ( minimalType < INT32.ordinal() && (candidate & ~0xFFFF) != 0 )
                {
                    minimalType = INT32.ordinal();
                }
                if ( minimalType < INT64.ordinal() && (candidate & ~0xFFFFFFFFL) != 0 )
                {
                    minimalType = INT64.ordinal();
                }
            }
            return minimalType;
        }

        void putIntegralArray( PageCursor cursor, IntegralArray array, int maxType )
        {
            int length = toIntExact( array.length() );
            int minimalType = minimalIntegralArrayType( array, maxType );
            cursor.putByte( (byte) minimalType );
            for ( int offset = 0; offset < length; offset++ )
            {
                long candidate = array.longValue( offset );
                if ( minimalType == -1 )
                {
                    cursor.putByte( (byte) candidate );
                }
                else if ( minimalType == INT8.ordinal() )
                {
                    cursor.putByte( (byte) candidate );
                }
                else if ( minimalType == INT16.ordinal() )
                {
                    cursor.putShort( (short) candidate );
                }
                else if ( minimalType == INT32.ordinal() )
                {
                    cursor.putInt( (int) candidate );
                }
                else if ( minimalType == INT64.ordinal() )
                {
                    cursor.putLong( candidate );
                }
            }
        }

        int integralArrayValueLength( IntegralArray array, int maxType )
        {
            int minimalType = minimalIntegralArrayType( array, maxType );
            int itemSize = ALL[minimalType].valueLength( null );

            return 1 + /*header byte saying which minimal type the values are stored in*/
                   toIntExact( array.length() ) * itemSize;
        }

        long getMinimalIntegralValue( PageCursor cursor, int minimalType )
        {
            if ( minimalType == 0 )
            {
                return 0;
            }
            else if ( minimalType == INT8.ordinal() )
            {
                return cursor.getByte() & 0xFF;
            }
            else if ( minimalType == INT16.ordinal() )
            {
                return cursor.getShort() & 0xFFFF;
            }
            else if ( minimalType == INT32.ordinal() )
            {
                return cursor.getInt() & 0xFFFFFFFFL;
            }
            else if ( minimalType == INT64.ordinal() )
            {
                return cursor.getLong();
            }
            else
            {
                throw new IllegalArgumentException( String.valueOf( minimalType ) );
            }
        }

        int integralArrayLength( int valueLength, int minimalType )
        {
            int itemSize = ALL[minimalType].valueLength( null );
            return (valueLength - 1) / itemSize;
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
