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

import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_CHANGE_DIFFERENT_SIZE_VALUE_IN_PLACE;

class RemoveVisitor extends Visitor
{
    RemoveVisitor( Store store )
    {
        super( store );
    }

    @Override
    public long accept( PageCursor cursor, long startId, int units ) throws IOException
    {
        if ( booleanState = seek( cursor ) )
        {   // It exists
            if ( BEHAVIOUR_CHANGE_DIFFERENT_SIZE_VALUE_IN_PLACE )
            {
                int currentHeaderEntryIndex = headerEntryIndex;
                int currentNumberOfHeaderEntries = currentType.numberOfHeaderEntries();
                int headerEntriesToMove = numberOfHeaderEntries - currentHeaderEntryIndex - currentNumberOfHeaderEntries;
                int headerDistance = currentNumberOfHeaderEntries * ProposedFormat.HEADER_ENTRY_SIZE;
                int currentSumValueLength = sumValueLength;
                int valueDistance = currentValueLength;

                // Seek to the end so that we get the total value length, TODO could be done better in some way, right?
                continueSeekUntilEnd( cursor );

                int valueSize = sumValueLength - currentSumValueLength;
                int valueLowOffset = valueStart( units, sumValueLength );

                // Move header entries
                cursor.shiftBytes( headerStart( currentHeaderEntryIndex ) + headerDistance,
                        headerEntriesToMove * ProposedFormat.HEADER_ENTRY_SIZE, - headerDistance );
                writeNumberOfHeaderEntries( cursor, numberOfHeaderEntries - currentNumberOfHeaderEntries );

                // Move data entries
                if ( valueDistance > 0 ) // distance == 0 for e.g. boolean values
                {
                    cursor.shiftBytes( valueLowOffset, valueSize, valueDistance );
                }
            }
            else
            {
                markHeaderAsUnused( cursor, headerEntryIndex );
            }
        }
        return -1;
    }
}
