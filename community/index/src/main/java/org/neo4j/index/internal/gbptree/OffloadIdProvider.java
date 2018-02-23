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

// todo Can this interface be collapsed?
interface OffloadIdProvider
{
    int RECORDS_PER_PAGE = 8;

    /**
     * Allocates offload record ids capable of storing {@code length} number of bytes.
     * Potentially this call can allocate multiple offload records, where the first one is returned as result.
     * The rest of the record ids can be accessed using {@link #placeAt(PageCursor, long)}, since the allocated
     * records will be linked together before this method returns.
     *
     *
     * @param stableGeneration stable generation.
     * @param unstableGeneration unstable generation.
     * @param length number of bytes to allocate offload records for.
     * @return allocated offload record ids.
     */
    long allocate( long stableGeneration, long unstableGeneration, int length ) throws IOException;

    void release( long stableGeneration, long unstableGeneration, long recordId ) throws IOException;

    /**
     * @return max number of bytes that can be written into an offload record.
     */
    int maxDataInRecord();

    /**
     * Place the provided {@link PageCursor} at the given {@code recordId} such that after this method returns
     * the cursor is at a page and offset where {@link #maxDataInRecord()} bytes can be written before needning
     * to move over to the next record and write more data. The return value from this method is a recordId
     * which is meant to be the recordId of the next call to {@link #placeAt(PageCursor, long)}.
     *
     * @param cursor {@link PageCursor} to place at the correct location for writing data into this record.
     * @param recordId the record to place the cursor at.
     * @return the next record id of this chain, or {@link TreeNode#NO_NODE_FLAG} if this will be the last record.
     */
    long placeAt( PageCursor cursor, long recordId ) throws IOException;
}
