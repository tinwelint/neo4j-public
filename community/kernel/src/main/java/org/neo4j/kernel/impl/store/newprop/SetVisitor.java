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
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_CHANGE_DIFFERENT_SIZE_VALUE_IN_PLACE;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.HEADER_ENTRY_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNIT_SIZE;

class SetVisitor extends Visitor
{
    // internal mutable state
    private int freeHeaderEntryIndex;
    private int freeSumValueLength;

    // user supplied state
    private Type type;
    private Object preparedValue;
    private int valueLength;

    SetVisitor( Store store )
    {
        super( store );
    }

    void setValue( Value value )
    {
        type = Type.fromValue( value );
        preparedValue = type.prepare( value );
        valueLength = type.valueLength( preparedValue );
    }

    @Override
    public void initialize( PageCursor cursor )
    {
        super.initialize( cursor );
        freeHeaderEntryIndex = -1;
        freeSumValueLength = 0;
    }

    @Override
    protected void skippedUnused( int skippedNumberOfHeaderEntries )
    {
        if ( freeHeaderEntryIndex == -1 && skippedNumberOfHeaderEntries == type.numberOfHeaderEntries() && currentValueLength == valueLength )
        {
            freeHeaderEntryIndex = headerEntryIndex;
            freeSumValueLength = sumValueLength;
        }
    }

    @Override
    public long accept( PageCursor cursor, long startId, int units ) throws IOException
    {
        longState = startId;
        int recordLength = units * UNIT_SIZE;
        if ( seek( cursor ) )
        {
            // Change property value
            Type oldType = currentType;
            int oldValueLength = currentValueLength;
            int oldValueLengthSum = sumValueLength;
            int hitHeaderEntryIndex = headerEntryIndex;
            continueSeekUntilEnd( cursor );
            int freeBytesInRecord = recordLength - sumValueLength - headerLength();
            int headerDiff = type.numberOfHeaderEntries() - oldType.numberOfHeaderEntries();
            int diff = valueLength - oldValueLength;
            if ( diff != 0 || headerDiff != 0 )
            {
                // Value/key size changed
                if ( BEHAVIOUR_CHANGE_DIFFERENT_SIZE_VALUE_IN_PLACE )
                {
                    units = relocateRecordIfNeeded( cursor, startId, units, freeBytesInRecord, headerDiff, diff );
                    makeRoomForPropertyInPlace( cursor, units, oldType, oldValueLengthSum, hitHeaderEntryIndex, headerDiff, diff );
                    writeValue( cursor, units, oldValueLengthSum + diff, type, preparedValue, valueLength );
                    if ( type != oldType || valueLength != oldValueLength )
                    {
                        writeHeader( cursor, valueLength, hitHeaderEntryIndex, type );
                    }
                    writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries + headerDiff );
                }
                else
                {
                    markHeaderAsUnused( cursor, hitHeaderEntryIndex );
                    units = relocateRecordIfNeeded( cursor, startId, units, freeBytesInRecord, type.numberOfHeaderEntries(), valueLength );
                    writeValue( cursor, units, sumValueLength + valueLength, type, preparedValue, valueLength );
                    writeHeader( cursor, valueLength, numberOfHeaderEntries, type );
                    writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries + type.numberOfHeaderEntries() );
                }
            }
            else
            {
                // Both value and key size are the same
                if ( BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE )
                {
                    writeValue( cursor, units, oldValueLengthSum, type, preparedValue, oldValueLength );
                    if ( type != oldType )
                    {
                        writeHeader( cursor, valueLength, hitHeaderEntryIndex, type );
                    }
                }
                else
                {
                    markHeaderAsUnused( cursor, hitHeaderEntryIndex );
                    writeValue( cursor, units, sumValueLength, type, preparedValue, valueLength );
                    writeHeader( cursor, valueLength, hitHeaderEntryIndex, type );
                }
            }
        }
        else
        {
            if ( BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE && freeHeaderEntryIndex != -1 )
            {
                writeValue( cursor, units, freeSumValueLength, type, preparedValue, valueLength );
                writeHeader( cursor, valueLength, freeHeaderEntryIndex, type );
            }
            else
            {
                // Add property
                int freeBytesInRecord = recordLength - sumValueLength - headerLength();
                units = relocateRecordIfNeeded( cursor, startId, units, freeBytesInRecord, type.numberOfHeaderEntries(), valueLength );
                writeValue( cursor, units, sumValueLength + valueLength, type, preparedValue, valueLength );
                writeHeader( cursor, valueLength, numberOfHeaderEntries, type );
                int newNumberOfHeaderEntries = numberOfHeaderEntries + type.numberOfHeaderEntries();
                writeNumberOfHeaderEntries( cursor, newNumberOfHeaderEntries );
            }
        }

        return -1; // TODO support records spanning multiple pages
    }

    private int relocateRecordIfNeeded( PageCursor cursor, long startId, int units, int freeBytesInRecord, int headerDiff, int diff )
            throws IOException
    {
        int growth = headerDiff * HEADER_ENTRY_SIZE + diff;
        if ( growth > freeBytesInRecord )
        {
            units = relocateRecord( cursor, startId, units, units * UNIT_SIZE + growth );
        }
        return units;
    }

    private void writeHeader( PageCursor cursor, int valueLength, int headerEntryIndex, Type type )
    {
        cursor.setOffset( headerStart( headerEntryIndex ) );
        type.putHeader( cursor, key, valueLength );
    }

    private void writeValue( PageCursor cursor, int units, int valueLengthSum, Type type, Object preparedValue, int valueLength )
    {
        cursor.setOffset( valueStart( units, valueLengthSum ) );
//        debug( "Writing " + key + " " + value + " of length " + valueLength + " in page " + cursor.getCurrentPageId() + " at " + cursor.getOffset() );
        type.putValue( cursor, preparedValue, valueLength );
    }

    private void makeRoomForPropertyInPlace( PageCursor cursor, int units, Type oldType, int oldValueLengthSum, int hitHeaderEntryIndex,
            int headerDiff, int diff )
    {
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
    }

    private void changeValueSize( PageCursor cursor, int diff, int units, int valueLengthSum )
    {
        int leftOffset = valueStart( units, sumValueLength );
        int rightOffset = valueStart( units, valueLengthSum );
        cursor.shiftBytes( leftOffset, rightOffset - leftOffset, - diff );
    }

    private void changeHeaderSize( PageCursor cursor, int headerDiff, Type oldType, int hitHeaderEntryIndex )
    {
        int leftOffset = headerStart( hitHeaderEntryIndex + oldType.numberOfHeaderEntries() );
        int rightOffset = headerStart( numberOfHeaderEntries );
        cursor.shiftBytes( leftOffset, rightOffset - leftOffset, headerDiff * HEADER_ENTRY_SIZE );
    }
}
