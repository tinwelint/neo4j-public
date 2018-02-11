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
import static org.neo4j.kernel.impl.store.newprop.Utils.debug;

class SetVisitor extends BaseVisitor
{
    // internal mutable state
    private int freeHeaderEntryIndex;
    private int freeValueOffset;
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
        freeValueOffset = 0;
        relocateHeaderEntryIndex = -1;
        // TODO consider "large" values where the valueLength in this main record and how it's written may be affected
    }

    @Override
    protected void skippedUnused( int skippedNumberOfHeaderEntries )
    {
        if ( freeHeaderEntryIndex == -1 && skippedNumberOfHeaderEntries == type.numberOfHeaderEntries() && currentValueLength == valueLength )
        {
            freeHeaderEntryIndex = currentHeaderEntryIndex;
            freeValueOffset = valueOffset;
        }
    }

    @Override
    public long accept( PageCursor cursor ) throws IOException
    {
        // We're going to write somewhere. Depending on configured behaviour and existing data that place is going to be different,
        // both for key and for value. This code is set up such that first comes a series of checks and preparations to write key/value
        // and finally the actual writing. Preparation may include relocation, growing/shrinking and what not.
        // The variables defined below are updated with key/value locations and other state for deciding where to write after
        // preparations have been made.
        int targetHeaderEntryIndex;
        int targetValueOffset;
        int targetNumberOfHeaderEntries = -1;

        if ( seek( cursor ) )
        {
            // Change property value
            Type oldType = currentType;
            int oldValueLength = currentValueLength;
            int oldValueOffset = valueOffset;
            int hitHeaderEntryIndex = currentHeaderEntryIndex;
            continueSeekUntilEnd( cursor );
            int headerDiff = type.numberOfHeaderEntries() - oldType.numberOfHeaderEntries();
            int valueDiff = valueLength - oldValueLength;
            if ( valueDiff != 0 || headerDiff != 0 )
            {
                // Key/value size changed
                if ( BEHAVIOUR_CHANGE_DIFFERENT_SIZE_VALUE_IN_PLACE )
                {
                    // Make room in-place by moving other properties our of the way
                    relocateRecordIfNeeded( cursor, headerDiff, valueDiff );
                    makeRoomForPropertyInPlace( cursor, oldType, oldValueOffset, hitHeaderEntryIndex, headerDiff, valueDiff );
                    targetHeaderEntryIndex = hitHeaderEntryIndex;
                    targetValueOffset = oldValueOffset + valueDiff;
                    targetNumberOfHeaderEntries = numberOfHeaderEntries + headerDiff;
                }
                else if ( BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE && freeHeaderEntryIndex != -1 )
                {
                    // There's an open space with just the right size
                    markHeaderAsUnused( cursor, hitHeaderEntryIndex );
                    targetHeaderEntryIndex = freeHeaderEntryIndex;
                    targetValueOffset = freeValueOffset;
                }
                else
                {
                    // Append the new version of this property at the end and mark the old one as unused

                    // TODO Marking this property as unused BEFORE relocating may leave this record in a state
                    // where this particular property is unused and its new value ends up in the new record... sparks a discussion
                    // about generally leaving retrying readers stranded on a record that doesn't have this property.
                    // Reader will finally do a consistent read and report that the property of that key doesn't exist.
                    // The reader would see it on the next attempt (after getting hold of the new record id), but would see
                    // this temporary incorrect state. Instead marking this property as unused AFTER relocating will move the
                    // to-be-unused bytes to the new record where it will occupy space, which is wasted space, but will avoid this issue.
                    //
                    // Otherwise a relocated record, the origin, could point forwards to its newer version so that readers could
                    // get to the newer record and read the property there. This requires an added pointer in the record header.

                    relocateRecordIfNeeded( cursor, type.numberOfHeaderEntries(), valueLength );
                    // Since the previous value currently is marked as unused AFTER relocation then we need to get hold of its
                    // header entry index and value offset here so that we're marking the correct property
                    if ( relocateHeaderEntryIndex != -1 )
                    {
                        // There was a relocation and the property location within the record changed.
                        hitHeaderEntryIndex = relocateHeaderEntryIndex;
                    }

                    markHeaderAsUnused( cursor, hitHeaderEntryIndex );
                    targetHeaderEntryIndex = numberOfHeaderEntries;
                    targetValueOffset = valueOffset + valueLength;
                    targetNumberOfHeaderEntries = numberOfHeaderEntries + type.numberOfHeaderEntries();
                }
            }
            else
            {
                // Both value and key sizes are the same
                if ( BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE )
                {
                    // Overwrite the value in-place
                    targetHeaderEntryIndex = hitHeaderEntryIndex;
                    targetValueOffset = oldValueOffset;
                }
                else
                {
                    // Append the new version of this property at the end and mark the old one as unused
                    markHeaderAsUnused( cursor, hitHeaderEntryIndex );
                    targetHeaderEntryIndex = numberOfHeaderEntries;
                    targetValueOffset = valueOffset + valueLength;
                    targetNumberOfHeaderEntries = numberOfHeaderEntries + type.numberOfHeaderEntries();
                }
            }
        }
        else
        {
            // Add property
            if ( BEHAVIOUR_CHANGE_SAME_SIZE_VALUE_IN_PLACE && freeHeaderEntryIndex != -1 )
            {
                // There's an open space with just the right size
                targetHeaderEntryIndex = freeHeaderEntryIndex;
                targetValueOffset = freeValueOffset;
            }
            else
            {
                // Append this property at the end
                relocateRecordIfNeeded( cursor, type.numberOfHeaderEntries(), valueLength );
                targetHeaderEntryIndex = numberOfHeaderEntries;
                targetValueOffset = valueOffset + valueLength;
                targetNumberOfHeaderEntries = numberOfHeaderEntries + type.numberOfHeaderEntries();
            }
        }

        // The "target" variables have now been initialized so it's time to write the data to this record
        writeValue( cursor, targetValueOffset, type, preparedValue, valueLength );
        writeHeader( cursor, valueLength, targetHeaderEntryIndex, type );
        if ( targetNumberOfHeaderEntries != -1 )
        {
            writeNumberOfHeaderEntries( cursor, targetNumberOfHeaderEntries );
        }

        return -1; // TODO support records spanning multiple pages
    }

    private void relocateRecordIfNeeded( PageCursor cursor, int headerDiff, int diff )
            throws IOException
    {
        int freeBytesInRecord = recordLength - valueOffset - headerLength();
        int growth = headerDiff * HEADER_ENTRY_SIZE + diff;
        if ( growth > freeBytesInRecord )
        {
            relocateRecord( cursor, units * UNIT_SIZE + growth );
        }
    }

    private void relocateRecord( PageCursor cursor, int totalRecordBytesRequired ) throws IOException
    {
        int unusedBytes = unusedValueLength + unusedNumberOfHeaderEntries * HEADER_ENTRY_SIZE;
        int newUnits = max( 1, (totalRecordBytesRequired - unusedBytes - 1) / 64 + 1 );
        long newRecordId = store.allocate( newUnits );
        assert debug( "Relocating %d (%d units) --> %d (%d units)", recordId, units, newRecordId, newUnits );
        long newPageId = pageIdForRecord( newRecordId );
        int newPivotOffset = offsetForId( newRecordId );
        try ( PageCursor newCursor = cursor.openLinkedCursor( newPageId ) )
        {
            newCursor.next();
            store.markAsUsed( newCursor, newRecordId, newUnits );
            if ( unusedNumberOfHeaderEntries == 0 )
            {
                // Copy header as one chunk
                cursor.copyTo( pivotOffset, newCursor, newPivotOffset, headerLength() );
                // Copy values as one chunk
                cursor.copyTo( pivotOffset + units * UNIT_SIZE - valueOffset, newCursor, newPivotOffset + newUnits * UNIT_SIZE - valueOffset, valueOffset );
            }
            else
            {
                // Copy live properties, one by one
                cursor.setOffset( headerRecordOffset( 0 ) );
                newCursor.setOffset( headerRecordOffset( newPivotOffset, 0 ) );
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
                            cursor.copyTo( valueRecordOffset( sourceValueOffset ) - valueLength, newCursor,
                                    valueRecordOffset( targetValueOffset, newUnits, newPivotOffset ) - valueLength, valueLength );
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
                currentHeaderEntryIndex = liveNumberOfHeaderEntries;
                valueOffset = targetValueOffset;
                unusedValueLength = 0;
                unusedNumberOfHeaderEntries = 0;

                writeNumberOfHeaderEntries( newCursor, liveNumberOfHeaderEntries, newPivotOffset );
            }
        }
        store.markAsUnused( cursor, recordId, units );
        cursor.next( newPageId );
        cursor.setOffset( newPivotOffset );
        pivotOffset = newPivotOffset;
        units = newUnits;
        recordId = newRecordId;
    }

    private void writeHeader( PageCursor cursor, int valueLength, int headerEntryIndex, Type type )
    {
        cursor.setOffset( headerRecordOffset( headerEntryIndex ) );
        type.putHeader( cursor, key, valueLength );
    }

    private void writeValue( PageCursor cursor, int valueOffset, Type type, Object preparedValue, int valueLength )
    {
        cursor.setOffset( valueRecordOffset( valueOffset ) );
        assert debug( "Writing %d %s of length %d in page %d at %d", key, value, valueLength, cursor.getCurrentPageId(), cursor.getOffset() );
        type.putValue( cursor, preparedValue, valueLength );
    }

    private void makeRoomForPropertyInPlace( PageCursor cursor, Type oldType, int valueOffset, int headerEntryIndex, int headerDiff, int valueDiff )
    {
        if ( headerDiff < 0 )
        {
            changeHeaderSize( cursor, headerDiff, oldType, headerEntryIndex );
        }
        if ( valueDiff < 0 )
        {
            changeValueSize( cursor, valueDiff, valueOffset );
        }
        if ( headerDiff > 0 )
        {
            changeHeaderSize( cursor, headerDiff, oldType, headerEntryIndex );
        }
        if ( valueDiff > 0 )
        {
            changeValueSize( cursor, valueDiff, valueOffset );
        }
    }

    private void changeValueSize( PageCursor cursor, int diff, int valueOffset )
    {
        int leftOffset = valueRecordOffset( this.valueOffset );
        int rightOffset = valueRecordOffset( valueOffset );
        cursor.shiftBytes( leftOffset, rightOffset - leftOffset, - diff );
    }

    private void changeHeaderSize( PageCursor cursor, int diff, Type oldType, int headerEntryIndex )
    {
        int leftOffset = headerRecordOffset( headerEntryIndex + oldType.numberOfHeaderEntries() );
        int rightOffset = headerRecordOffset( numberOfHeaderEntries );
        cursor.shiftBytes( leftOffset, rightOffset - leftOffset, diff * HEADER_ENTRY_SIZE );
    }
}
