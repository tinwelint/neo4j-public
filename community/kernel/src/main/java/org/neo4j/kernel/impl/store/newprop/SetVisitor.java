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
            int growth = headerDiff * ProposedFormat.HEADER_ENTRY_SIZE + diff;
            if ( growth > freeBytesInRecord )
            {
                units = growRecord( cursor, startId, units, growth );
                seekToEnd( cursor );
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
            int size = type.numberOfHeaderEntries() * ProposedFormat.HEADER_ENTRY_SIZE + valueLength;
            if ( size > freeBytesInRecord )
            {   // Grow/relocate record
                units = growRecord( cursor, startId, units, size - freeBytesInRecord );
                // Perhaps unnecessary to call seek again, the point of it is to leave the cursor in the
                // expected place for insert, just as it would have been if we wouldn't have grown the record
                // -- code simplicity
                boolean found = seek( cursor );
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
            moveBytesRight( cursor, leftOffset, rightOffset - leftOffset, headerDiff * ProposedFormat.HEADER_ENTRY_SIZE );
        }
        else if ( headerDiff < 0 )
        {
            // Shrink, i.e. move the other header entries diff entries to the left
            moveBytesLeft( cursor, leftOffset, rightOffset - leftOffset, - headerDiff * ProposedFormat.HEADER_ENTRY_SIZE );
        }
    }
}
