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

class GetVisitor extends Visitor implements ValueStructure
{
    // grows on demand
    private byte[] byteArray = new byte[8];
    private int byteArrayCursor;

    GetVisitor( Store store )
    {
        super( store );
    }

    @Override
    public long accept( PageCursor cursor, long startId, int units ) throws IOException
    {
        boolean found = seek( cursor );

        // Check consistency because we just read data which affects how we're going to continue reading.
        if ( cursor.shouldRetry() )
        {
            return SPECIAL_ID_SHOULD_RETRY;
        }

        if ( found )
        {
            cursor.setOffset( valueStart( units, sumValueLength ) );
            boolean valueStructureRead = currentType.getValueStructure( cursor, currentValueLength, this );

            // Check consistency because we just read data which affects how we're going to continue reading.
            if ( !valueStructureRead || cursor.shouldRetry() )
            {
                return SPECIAL_ID_SHOULD_RETRY;
            }

            readValue = currentType.getValue( cursor, currentValueLength, this );
        }
        return -1;
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
        if ( length > byteArray.length )
        {
            byteArray = new byte[length * 2];
        }
        byteArrayCursor = length;
        return byteArray;
    }
}
