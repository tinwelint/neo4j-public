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

import static java.lang.Integer.max;

import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_CHANGE_DIFFERENT_SIZE_VALUE_IN_PLACE;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.HEADER_ENTRY_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNIT_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.offsetForId;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.pageIdForRecord;

class SetVisitor extends Visitor
{
    // internal mutable state
    private int freeHeaderEntryIndex;
    private int freeSumValueLength;
    private int relocateHeaderEntryIndex;

    // user supplied state
    private Type type;
    private Object preparedValue;
    private int valueLength;
    private Value value;

    SetVisitor( Store store )
    {
        super( store );
    }

    void setValue( Value value )
    {
        this.value = value;
        type = Type.fromValue( value );
        preparedValue = type.prepare( value );
        valueLength = type.valueLength( preparedValue );
    }

    @Override
    public void initialize( PageCursor cursor, long startId, int units )
    {
        super.initialize( cursor, startId, units );
        freeHeaderEntryIndex = -1;
        freeSumValueLength = 0;
        relocateHeaderEntryIndex = -1;
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
    public long accept( PageCursor cursor ) throws IOException
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
                    relocateRecordIfNeeded( cursor, freeBytesInRecord, headerDiff, diff );
                    makeRoomForPropertyInPlace( cursor, oldType, oldValueLengthSum, hitHeaderEntryIndex, headerDiff, diff );
                    writeValue( cursor, oldValueLengthSum + diff, type, preparedValue, valueLength );
                    if ( type != oldType || valueLength != oldValueLength )
                    {
                        writeHeader( cursor, valueLength, hitHeaderEntryIndex, type );
                    }
                    writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries + headerDiff );
                }
                else if ( freeHeaderEntryIndex != -1 )
                {
                    markHeaderAsUnused( cursor, hitHeaderEntryIndex );
                    writeValue( cursor, freeSumValueLength, type, preparedValue, valueLength );
                    writeHeader( cursor, valueLength, freeHeaderEntryIndex, type );
                }
                else
                {
                    // TODO Marking this property as unused BEFORE relocating may leave this record in a state
                    // where this particular property is unused and its new value ends up on the new record... sparks a discussion
                    // about generally leaving retrying readers stranded on a record that doesn't have this property.
                    // Reader will finally do a consistent read and report that the property of that key doesn't exist.
                    // The reader would see it on the next attempt (after getting hold of the new record id), but would see
                    // this temporary incorrect state. Instead marking this property as unused AFTER relocating will move the
                    // to-by-unused bytes to the new record where it will occupy space, which is wasted space, but will avoid this issue.
                    //
                    // Otherwise a relocated record, the origin, would point forwards to its newer version so that readers could
                    // get to the newer record and read the property there. This requires an added pointer in the record header.

                    relocateRecordIfNeeded( cursor, freeBytesInRecord, type.numberOfHeaderEntries(), valueLength );
                    // Since the previous value currently is marked as unused AFTER relocation then we need to get hold of its
                    // header entry index and value offset here so that we're marking the correct property
                    if ( relocateHeaderEntryIndex != -1 )
                    {
                        // There was a relocation and the property location within the record changed.
                        hitHeaderEntryIndex = relocateHeaderEntryIndex;
                    }

                    markHeaderAsUnused( cursor, hitHeaderEntryIndex );
                    writeValue( cursor, sumValueLength + valueLength, type, preparedValue, valueLength );
                    writeHeader( cursor, valueLength, numberOfHeaderEntries, type );
                    writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries + type.numberOfHeaderEntries() );
                }
            }
            else
            {
                // Both value and key size are the same
                if ( BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE )
                {
                    writeValue( cursor, oldValueLengthSum, type, preparedValue, oldValueLength );
                    if ( type != oldType )
                    {
                        writeHeader( cursor, valueLength, hitHeaderEntryIndex, type );
                    }
                }
                else
                {
                    markHeaderAsUnused( cursor, hitHeaderEntryIndex );
                    writeValue( cursor, sumValueLength, type, preparedValue, valueLength );
                    writeHeader( cursor, valueLength, hitHeaderEntryIndex, type );
                }
            }
        }
        else
        {
            if ( BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE && freeHeaderEntryIndex != -1 )
            {
                writeValue( cursor, freeSumValueLength, type, preparedValue, valueLength );
                writeHeader( cursor, valueLength, freeHeaderEntryIndex, type );
            }
            else
            {
                // Add property
                int freeBytesInRecord = recordLength - sumValueLength - headerLength();
                relocateRecordIfNeeded( cursor, freeBytesInRecord, type.numberOfHeaderEntries(), valueLength );
                writeValue( cursor, sumValueLength + valueLength, type, preparedValue, valueLength );
                writeHeader( cursor, valueLength, numberOfHeaderEntries, type );
                int newNumberOfHeaderEntries = numberOfHeaderEntries + type.numberOfHeaderEntries();
                writeNumberOfHeaderEntries( cursor, newNumberOfHeaderEntries );
            }
        }

        return -1; // TODO support records spanning multiple pages
    }

    private void relocateRecordIfNeeded( PageCursor cursor, int freeBytesInRecord, int headerDiff, int diff )
            throws IOException
    {
        int growth = headerDiff * HEADER_ENTRY_SIZE + diff;
        if ( growth > freeBytesInRecord )
        {
            relocateRecord( cursor, units * UNIT_SIZE + growth );
        }
    }

    void relocateRecord( PageCursor cursor, int totalRecordBytesRequired ) throws IOException
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
                        if ( key == this.key )
                        {
                            relocateHeaderEntryIndex = liveNumberOfHeaderEntries;
                            assert debug( "Relocate key %d", relocateHeaderEntryIndex );
                        }

                        // Copy key
                        type.putHeader( newCursor, key, valueLength );
                        // Copy value
                        if ( valueLength > 0 )
                        {
                            cursor.copyTo( valueStart( sourceValueOffset ) - valueLength, newCursor,
                                    valueStart( targetValueOffset, newUnits, newPivotOffset ) - valueLength, valueLength );
                            targetValueOffset += valueLength;
                        }
                        assert debug( "Copied %d w/ value length %d from page %d at %d to page %d at %d from header index %d to %d",
                                key, valueLength, cursor.getCurrentPageId(), cursor.getOffset(), newCursor.getCurrentPageId(),
                                newCursor.getOffset(), i, liveNumberOfHeaderEntries );
                        liveNumberOfHeaderEntries += numberOfHeaderEntries;
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
        units = newUnits;
    }

    private void writeHeader( PageCursor cursor, int valueLength, int headerEntryIndex, Type type )
    {
        cursor.setOffset( headerStart( headerEntryIndex ) );
        type.putHeader( cursor, key, valueLength );
    }

    private void writeValue( PageCursor cursor, int valueLengthSum, Type type, Object preparedValue, int valueLength )
    {
        cursor.setOffset( valueStart( valueLengthSum ) );
        assert debug( "Writing %d %s of length %d in page %d at %d", key, value, valueLength, cursor.getCurrentPageId(), cursor.getOffset() );
        type.putValue( cursor, preparedValue, valueLength );
    }

    private void makeRoomForPropertyInPlace( PageCursor cursor, Type oldType, int oldValueLengthSum, int hitHeaderEntryIndex,
            int headerDiff, int diff )
    {
        if ( headerDiff < 0 )
        {
            changeHeaderSize( cursor, headerDiff, oldType, hitHeaderEntryIndex );
        }
        if ( diff < 0 )
        {
            changeValueSize( cursor, diff, oldValueLengthSum );
        }
        if ( headerDiff > 0 )
        {
            changeHeaderSize( cursor, headerDiff, oldType, hitHeaderEntryIndex );
        }
        if ( diff > 0 )
        {
            changeValueSize( cursor, diff, oldValueLengthSum );
        }
    }

    private void changeValueSize( PageCursor cursor, int diff, int valueLengthSum )
    {
        int leftOffset = valueStart( sumValueLength );
        int rightOffset = valueStart( valueLengthSum );
        cursor.shiftBytes( leftOffset, rightOffset - leftOffset, - diff );
    }

    private void changeHeaderSize( PageCursor cursor, int headerDiff, Type oldType, int hitHeaderEntryIndex )
    {
        int leftOffset = headerStart( hitHeaderEntryIndex + oldType.numberOfHeaderEntries() );
        int rightOffset = headerStart( numberOfHeaderEntries );
        cursor.shiftBytes( leftOffset, rightOffset - leftOffset, headerDiff * HEADER_ENTRY_SIZE );
    }
}
