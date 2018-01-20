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
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.store.newprop.Store.SPECIAL_ID_SHOULD_RETRY;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNIT_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.offsetForId;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.pageIdForRecord;

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
            // Allocate room, some number of units to start off with and then grow from there.
            // In a real scenario we'd probably have a method setting multiple properties and so
            // we'd know how big our record would be right away. This is just to prototype the design
            id = store.allocate( 1 );
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
            public long accept( PageCursor cursor, long startId, int units ) throws IOException
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
            public long accept( PageCursor cursor, long startId, int units ) throws IOException
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
            public long accept( PageCursor cursor, long startId, int units ) throws IOException
            {
                boolean found = seek( cursor, key );

                // Check consistency because we just read data which affects how we're going to continue reading.
                if ( cursor.shouldRetry() )
                {
                    return SPECIAL_ID_SHOULD_RETRY;
                }

                if ( found )
                {
                    cursor.setOffset( valueStart( units, sumValueLength ) );
                    boolean valueStructureRead = currentType.getValueStructure( cursor, currentValueLength, this );

                    // Check consistency because we just read data which affects how we're going to continue reading.
                    if ( !valueStructureRead || cursor.shouldRetry() )
                    {
                        return SPECIAL_ID_SHOULD_RETRY;
                    }

                    readValue = currentType.getValue( cursor, currentValueLength, this );
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
            public long accept( PageCursor cursor, long startId, int units ) throws IOException
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

    abstract class Visitor implements RecordVisitor, ValueStructure
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

        // value structure stuff
        protected long integralStructureValue;
        protected Object objectStructureValue;

        boolean seek( PageCursor cursor, int key ) throws IOException
        {
            pivotOffset = cursor.getOffset();
            cursor.setOffset( pivotOffset );
            numberOfHeaderEntries = cursor.getShort();
            sumValueLength = 0;
            currentValueLength = 0;
            headerEntryIndex = 0;
            boolean found = seekTo( cursor, key );
            headerLength = RECORD_HEADER_SIZE + numberOfHeaderEntries * HEADER_ENTRY_SIZE;
            return found;
        }

        private boolean seekTo( PageCursor cursor, int key )
        {
            while ( headerEntryIndex < numberOfHeaderEntries )
            {
                long headerEntry = getUnsignedInt( cursor );
                currentType = Type.fromHeader( headerEntry, cursor );
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
                // Copy header
                cursor.copyTo( pivotOffset, newCursor, newOffset, headerLength );
                // Copy values
                cursor.copyTo(
                        pivotOffset + units * UNIT_SIZE - sumValueLength, newCursor,
                        newOffset + newUnits * UNIT_SIZE - sumValueLength, sumValueLength );
            }
            // TODO don't mark as unused right here because that may leave a reader stranded. This should be done
            // at some point later, like how buffered id freeing happens in neo4j, where we can be certain that
            // no reader is in there when doing this marking.

//            Header.mark( cursor, startId, units, false );
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

        @Override
        public void integralValue( long value )
        {
            this.integralStructureValue = value;
        }

        @Override
        public long integralValue()
        {
            return integralStructureValue;
        }

        @Override
        public void value( Object value )
        {
            this.objectStructureValue = value;
        }

        @Override
        public Object value()
        {
            return objectStructureValue;
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
    public long storeSize() throws IOException
    {
        return store.storeFile.getLastPageId() * kibiBytes( 8 );
    }
}
