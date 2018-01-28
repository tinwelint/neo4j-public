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

import static org.neo4j.kernel.impl.store.newprop.Store.SPECIAL_ID_SHOULD_RETRY;

class GetVisitor extends Visitor
{
    GetVisitor( Store store )
    {
        super( store );
    }

    @Override
    public long accept( PageCursor cursor ) throws IOException
    {
        boolean found = seek( cursor );

        // Check consistency because we just read data which affects how we're going to continue reading.
        if ( cursor.shouldRetry() )
        {
            return SPECIAL_ID_SHOULD_RETRY;
        }

        if ( found )
        {
            cursor.setOffset( valueStart( sumValueLength ) );
            int valueStructureRead = currentType.getValueStructure( cursor, currentValueLength, this );

            // Check consistency because we just read data which affects how we're going to continue reading.
            if ( valueStructureRead == READ_INCONSISTENT || (valueStructureRead == READ && cursor.shouldRetry()) )
            {
                return SPECIAL_ID_SHOULD_RETRY;
            }

            assert debug( "Reading value of %d and length %d from page %d at %d", key, currentValueLength, cursor.getCurrentPageId(), cursor.getOffset() );
            readValue = currentType.getValue( cursor, currentValueLength, this );
        }
        return -1;
    }
}
