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
    private Value value;

    SetVisitor( Store store )
    {
        super( store );
    }

    void setValue( Value value )
    {
        this.value = value;
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
            seekToEnd( cursor );
            int freeBytesInRecord = recordLength - sumValueLength - headerLength;

            Type type = Type.fromValue( value );
            Object preparedValue = type.prepare( value );
            int headerDiff = type.numberOfHeaderEntries() - oldType.numberOfHeaderEntries();
            int newValueLength = type.valueLength( preparedValue );
            int diff = newValueLength - oldValueLength;
            if ( diff != 0 || headerDiff != 0 )
            {
                // Value/key size changed
                if ( BEHAVIOUR_CHANGE_DIFFERENT_SIZE_VALUE_IN_PLACE )
                {
                    units = growIfNeeded( cursor, startId, units, freeBytesInRecord, headerDiff, diff, false );
                    makeRoomForPropertyInPlace( cursor, units, oldType, oldValueLengthSum, hitHeaderEntryIndex, headerDiff, diff );
                    writeValue( cursor, units, oldValueLengthSum + diff, type, preparedValue, newValueLength );
                    if ( type != oldType || newValueLength != oldValueLength )
                    {
                        writeHeader( cursor, oldValueLengthSum, newValueLength, hitHeaderEntryIndex, type );
                    }
                    writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries + headerDiff );
                }
                else
                {
                    units = growIfNeeded( cursor, startId, units, freeBytesInRecord, type.numberOfHeaderEntries(), newValueLength, false );
                    markHeaderAsUnused( cursor, hitHeaderEntryIndex );
                    writeValue( cursor, units, sumValueLength, type, preparedValue, newValueLength );
                    if ( type != oldType )
                    {
                        writeHeader( cursor, sumValueLength, newValueLength, hitHeaderEntryIndex, type );
                    }
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
                        writeHeader( cursor, oldValueLengthSum, newValueLength, hitHeaderEntryIndex, type );
                    }
                }
                else
                {
                    markHeaderAsUnused( cursor, hitHeaderEntryIndex );
                    writeValue( cursor, units, sumValueLength, type, preparedValue, newValueLength );
                    if ( type != oldType )
                    {
                        writeHeader( cursor, sumValueLength, newValueLength, hitHeaderEntryIndex, type );
                    }
                }
            }
        }
        else
        {
            // Add property
            int freeBytesInRecord = recordLength - sumValueLength - headerLength;
            Type type = Type.fromValue( value );
            Object preparedValue = type.prepare( value );
            int valueLength = type.valueLength( preparedValue );
            units = growIfNeeded( cursor, startId, units, freeBytesInRecord, type.numberOfHeaderEntries(), valueLength, true );
            writeValue( cursor, units, sumValueLength + valueLength, type, preparedValue, valueLength );
            writeHeader( cursor, sumValueLength, valueLength, numberOfHeaderEntries, type );
            int newNumberOfHeaderEntries = numberOfHeaderEntries + type.numberOfHeaderEntries();
            writeNumberOfHeaderEntries( cursor, newNumberOfHeaderEntries );
        }

        return -1; // TODO support records spanning multiple pages
    }

    private int growIfNeeded( PageCursor cursor, long startId, int units, int freeBytesInRecord, int headerDiff, int diff, boolean forSet )
            throws IOException
    {
        int growth = headerDiff * HEADER_ENTRY_SIZE + diff;
        if ( growth > freeBytesInRecord )
        {
            units = growRecord( cursor, startId, units, growth - freeBytesInRecord );
            // TODO This is a silly hack, to have this difference like this
            if ( forSet )
            {
                seek( cursor );
            }
            else
            {
                seekToEnd( cursor );
            }
        }
        return units;
    }

    private void writeHeader( PageCursor cursor, int valueOffset, int valueLength, int headerEntryIndex, Type type )
    {
        cursor.setOffset( headerStart( headerEntryIndex ) );
        type.putHeader( cursor, key, valueOffset, valueLength );
    }

    private void writeValue( PageCursor cursor, int units, int valueLengthSum, Type type, Object preparedValue, int valueLength )
    {
        cursor.setOffset( valueStart( units, valueLengthSum ) );
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
