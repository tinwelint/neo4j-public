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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.newprop.Store.RecordVisitor;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_ORDERED_BY_KEY;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_VALUES_FROM_END;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.HEADER_ENTRY_SIZE;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.RECORD_HEADER_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNIT_SIZE;

abstract class Visitor implements RecordVisitor
{
    private static final int FLAG_UNUSED = 0x80000000;

    protected final Store store;

    // internal mutable state
    protected long recordId;
    protected int units;
    protected int pivotOffset;
    protected int numberOfHeaderEntries;
    protected int sumValueLength;
    protected int currentValueLength;
    protected int currentHeaderEntryIndex;
    protected Type currentType;
    protected int unusedNumberOfHeaderEntries;
    protected int unusedValueLength;

    // user state
    protected int key = -1;
    protected boolean propertyExisted;

    Visitor( Store store )
    {
        this.store = store;
    }

    void setKey( int key )
    {
        this.key = key;
    }

    @Override
    public void initialize( PageCursor cursor, long startId, int units )
    {
        this.recordId = startId;
        this.units = units;
        pivotOffset = cursor.getOffset();
        numberOfHeaderEntries = cursor.getShort();
        sumValueLength = 0;
        currentValueLength = 0;
        currentHeaderEntryIndex = 0;
        unusedNumberOfHeaderEntries = 0;
        unusedValueLength = 0;
        currentType = null;
        propertyExisted = false;
    }

    boolean seek( PageCursor cursor )
    {
        assert key != -1;
        return seekTo( cursor, key );
    }

    int headerLength()
    {
        return RECORD_HEADER_SIZE + numberOfHeaderEntries * HEADER_ENTRY_SIZE;
    }

    private boolean seekTo( PageCursor cursor, int key )
    {
        while ( currentHeaderEntryIndex < numberOfHeaderEntries )
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
                unusedNumberOfHeaderEntries += currentNumberOfHeaderEntries;
                if ( BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE )
                {
                    skippedUnused( currentNumberOfHeaderEntries );
                }
            }

            currentHeaderEntryIndex += currentNumberOfHeaderEntries;
        }
        return false;
    }

    protected void skippedUnused( int skippedNumberOfHeaderEntries )
    {
    }

    /**
     * This is needed to figure out the sum of all value lengths, an alternative would be to store that in
     * a header field, but should we?
     * @param cursor {@link PageCursor} which should by mid-way through seeking the header entries from a
     * previous call to {@link #seek(PageCursor)}.
     */
    void continueSeekUntilEnd( PageCursor cursor )
    {
        assert currentHeaderEntryIndex < numberOfHeaderEntries;

        // TODO hacky thing here with the headerEntryIndex, better to increment before return in seek?
        currentHeaderEntryIndex += currentType.numberOfHeaderEntries();
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

    int valueStart( int valueOffset )
    {
        return valueStart( valueOffset, units, pivotOffset );
    }

    int valueStart( int valueOffset, int units, int pivotOffset )
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
        int length = Type.fromHeader( headerEntry, cursor ).valueLength( cursor );
        assert debug( "Marking header entry %d as unused key %d valueOffset length %d", headerEntryIndex, key, valueStart( sumValueLength ), length );
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

    static boolean debug( String message, Object... values )
    {
//        System.out.println( String.format( message, values ) );
        return true;
    }
}
