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

import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_ORDERED_BY_KEY;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_VALUES_FROM_END;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.HEADER_ENTRY_SIZE;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.RECORD_HEADER_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNIT_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.offsetForId;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.pageIdForRecord;

abstract class Visitor implements RecordVisitor, ValueStructure
{
    private static final int FLAG_UNUSED = 0x80000000;

    private final Store store;
    protected int pivotOffset;
    protected int numberOfHeaderEntries;
    protected int sumValueLength;
    protected int currentValueLength;
    protected int headerEntryIndex;
    protected Type currentType;
    protected boolean booleanState;
    protected long longState;
    protected Value readValue;
    protected int key = -1;
    protected int unusedHeaderEntries;
    protected int unusedValueLength;

    // value structure stuff
    private long integralStructureValue;
    private Object objectStructureValue;
    private byte[] byteArray; // grows on demand

    Visitor( Store store )
    {
        this.store = store;
    }

    void setKey( int key )
    {
        this.key = key;
    }

    boolean seek( PageCursor cursor )
    {
        assert key != -1;
        pivotOffset = cursor.getOffset();
        numberOfHeaderEntries = cursor.getShort();
        sumValueLength = 0;
        currentValueLength = 0;
        headerEntryIndex = 0;
        unusedHeaderEntries = 0;
        unusedValueLength = 0;
        return seekTo( cursor, key );
    }

    int headerLength()
    {
        return RECORD_HEADER_SIZE + numberOfHeaderEntries * HEADER_ENTRY_SIZE;
    }

    private boolean seekTo( PageCursor cursor, int key )
    {
        while ( headerEntryIndex < numberOfHeaderEntries )
        {
            long headerEntry = getUnsignedInt( cursor );
            boolean isUsed = isUsed( headerEntry );
            currentType = Type.fromHeader( headerEntry, cursor );
            currentValueLength = currentType.valueLength( cursor );
            sumValueLength += currentValueLength;
            int currentNumberOfHeaderEntries = currentType.numberOfHeaderEntries();
            if ( isUsed )
            {
                int thisKey = currentType.keyOf( headerEntry );

                if ( BEHAVIOUR_ORDERED_BY_KEY )
                {
                    if ( thisKey > key )
                    {
                        // We got too far, i.e. this key doesn't exist
                        // We leave offsets at the start of this header entry so that insert can insert right there
                        return false;
                    }
                }

                if ( thisKey == key )
                {
                    // valueLength == length of found value
                    // relativeValueOffset == relative start of this value, i.e.
                    // actual page offset == pivotOffset + recordSize - relativeValueOffset
                    return true;
                }
            }
            else
            {
                unusedValueLength += currentValueLength;
                unusedHeaderEntries += currentNumberOfHeaderEntries;
            }

            headerEntryIndex += currentNumberOfHeaderEntries;
        }
        return false;
    }

    /**
     * This is needed to figure out the sum of all value lengths, an alternative would be to store that in
     * a header field, but should we?
     * @param cursor {@link PageCursor} which should by mid-way through seeking the header entries from a
     * previous call to {@link #seek(PageCursor)}.
     */
    void continueSeekUntilEnd( PageCursor cursor )
    {
        assert headerEntryIndex < numberOfHeaderEntries;

        // TODO hacky thing here with the headerEntryIndex, better to increment before return in seek?
        headerEntryIndex += currentType.numberOfHeaderEntries();
        seekTo( cursor, -1 );
    }

    boolean isUsed( long headerEntry )
    {
        return (headerEntry & FLAG_UNUSED) == 0;
    }

    long setUnused( long headerEntry )
    {
        return headerEntry | FLAG_UNUSED;
    }

    int relocateRecord( PageCursor cursor, long startId, int units, int totalRecordBytesRequired ) throws IOException
    {
        // TODO Special case: can we grow in-place?

        // Normal case: find new bigger place and move there.
        int unusedBytes = unusedValueLength + unusedHeaderEntries * HEADER_ENTRY_SIZE;
        int newUnits = max( 1, (totalRecordBytesRequired - unusedBytes - 1) / 64 + 1 );
        long newStartId = longState = store.allocate( newUnits );
        long newPageId = pageIdForRecord( newStartId );
        int newPivotOffset = offsetForId( newStartId );
        try ( PageCursor newCursor = cursor.openLinkedCursor( newPageId ) )
        {
            newCursor.next();
            if ( unusedHeaderEntries == 0 )
            {
                // Copy header as one chunk
                cursor.copyTo( pivotOffset, newCursor, newPivotOffset, headerLength() );
                // Copy values as one chunk
                cursor.copyTo(
                        pivotOffset + units * UNIT_SIZE - sumValueLength, newCursor,
                        newPivotOffset + newUnits * UNIT_SIZE - sumValueLength, sumValueLength );
            }
            else
            {
                // Copy live properties, one by one
                cursor.setOffset( headerStart( 0 ) );
                newCursor.setOffset( headerStart( newPivotOffset, 0 ) );
                int liveNumberOfHeaderEntries = 0;
                int targetValueOffset = 0;
                for ( int i = 0, sourceValueOffset = 0; i < numberOfHeaderEntries; )
                {
                    long headerEntry = getUnsignedInt( cursor );
                    Type type = Type.fromHeader( headerEntry, cursor );
                    int valueLength = type.valueLength( cursor );
                    int numberOfHeaderEntries = type.numberOfHeaderEntries();
                    if ( isUsed( headerEntry ) )
                    {
                        int key = type.keyOf( headerEntry );

                        // Copy key
                        type.putHeader( newCursor, key, i, valueLength );
                        // Copy value
                        if ( valueLength > 0 )
                        {
                            cursor.copyTo( valueStart( units, sourceValueOffset ) - valueLength, newCursor,
                                    valueStart( newUnits, targetValueOffset, newPivotOffset ) - valueLength, valueLength );
                            targetValueOffset += valueLength;
                        }
                        liveNumberOfHeaderEntries += numberOfHeaderEntries;
//                        debug( "Copied " + key + " w/ value length " + valueLength + " from page " + cursor.getCurrentPageId() + " at " + cursor.getOffset() + " to page " + newCursor.getCurrentPageId() + " at " + newCursor.getOffset() );
                    }
                    i += numberOfHeaderEntries;
                    sourceValueOffset += valueLength;
                }

                numberOfHeaderEntries = liveNumberOfHeaderEntries;
                headerEntryIndex = liveNumberOfHeaderEntries;
                sumValueLength = targetValueOffset;
                unusedValueLength = 0;
                unusedHeaderEntries = 0;

                writeNumberOfHeaderEntries( newCursor, liveNumberOfHeaderEntries, newPivotOffset );
            }
        }
        // TODO don't mark as unused right here because that may leave a reader stranded. This should be done
        // at some point later, like how buffered id freeing happens in neo4j, where we can be certain that
        // no reader is in there when doing this marking.

//            Header.mark( cursor, startId, units, false );
        cursor.next( newPageId );
        cursor.setOffset( newPivotOffset );
        pivotOffset = newPivotOffset;
        return newUnits;
    }

    int valueStart( int units, int valueOffset )
    {
        return valueStart( units, valueOffset, pivotOffset );
    }

    int valueStart( int units, int valueOffset, int pivotOffset )
    {
        if ( BEHAVIOUR_VALUES_FROM_END )
        {
            return pivotOffset + (units * UNIT_SIZE) - valueOffset;
        }
        else
        {
            throw new UnsupportedOperationException( "Not supported a.t.m." );
        }
    }

    int headerStart( int headerEntryIndex )
    {
        return headerStart( pivotOffset, headerEntryIndex );
    }

    int headerStart( int pivotOffset, int headerEntryIndex )
    {
        return pivotOffset + RECORD_HEADER_SIZE + headerEntryIndex * HEADER_ENTRY_SIZE;
    }

    void placeCursorAtHeaderEntry( PageCursor cursor, int headerEntryIndex )
    {
        cursor.setOffset( headerStart( headerEntryIndex ) );
    }

    void writeNumberOfHeaderEntries( PageCursor cursor, int newNumberOfHeaderEntries )
    {
        writeNumberOfHeaderEntries( cursor, newNumberOfHeaderEntries, pivotOffset );
    }

    void writeNumberOfHeaderEntries( PageCursor cursor, int newNumberOfHeaderEntries, int pivotOffset )
    {
        cursor.putShort( pivotOffset, (short) newNumberOfHeaderEntries ); // TODO safe cast
    }

    protected void markHeaderAsUnused( PageCursor cursor, int headerEntryIndex )
    {
        int offset = headerStart( headerEntryIndex );
        long headerEntry = getUnsignedInt( cursor, offset );
        headerEntry = setUnused( headerEntry );
        cursor.putInt( offset, (int) headerEntry );
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

    @Override
    public byte[] byteArray( int length )
    {
        if ( byteArray == null || length > byteArray.length )
        {
            byteArray = new byte[length * 2];
        }
        return byteArray;
    }

    protected void debug( String message )
    {
//        debug( message );
    }
}
