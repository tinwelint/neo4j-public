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

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.newprop.Store.RecordVisitor;
import org.neo4j.values.storable.Value;

import static java.lang.Integer.max;
import static java.lang.Integer.min;

import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNIT_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.offsetForId;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.pageIdForRecord;

abstract class Visitor implements RecordVisitor, ValueStructure
{
    private final Store store;
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

    Visitor( Store store )
    {
        this.store = store;
    }

    boolean seek( PageCursor cursor, int key )
    {
        pivotOffset = cursor.getOffset();
        cursor.setOffset( pivotOffset );
        numberOfHeaderEntries = cursor.getShort();
        sumValueLength = 0;
        currentValueLength = 0;
        headerEntryIndex = 0;
        boolean found = seekTo( cursor, key );
        headerLength = ProposedFormat.RECORD_HEADER_SIZE + numberOfHeaderEntries * ProposedFormat.HEADER_ENTRY_SIZE;
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
        return pivotOffset + ProposedFormat.RECORD_HEADER_SIZE + headerEntryIndex * ProposedFormat.HEADER_ENTRY_SIZE;
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

    void moveBytesLeft( PageCursor cursor, int lowOffset, int size, int distance )
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

    void moveBytesRight( PageCursor cursor, int lowOffset, int size, int distance )
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

    static long getUnsignedInt( PageCursor cursor )
    {
        return cursor.getInt() & 0xFFFFFFFFL;
    }

    static long getUnsignedInt( PageCursor cursor, int offset )
    {
        return cursor.getInt( offset ) & 0xFFFFFFFFL;
    }
}
