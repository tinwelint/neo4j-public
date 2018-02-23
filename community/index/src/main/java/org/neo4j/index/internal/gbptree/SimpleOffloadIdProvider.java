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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Disclaimer: This id provider doesn't work exactly like the real one does (at the time of writing the real one doesn't exist tho).
 * This one focuses on simplicity and may skip features like having multiple records per page a.s.o.
 */
class SimpleOffloadIdProvider implements OffloadIdProvider
{
    private final int pageSize;

    SimpleOffloadIdProvider( int pageSize )
    {
        this.pageSize = pageSize;
    }

    @Override
    public long allocate( long stableGeneration, long unstableGeneration, int length )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void release( long stableGeneration, long unstableGeneration, long recordId ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int maxDataInRecord()
    {
        return pageSize - Long.BYTES;
    }

    @Override
    public long placeAt( PageCursor cursor, long recordId )
    {
        throw new UnsupportedOperationException();
    }
}
