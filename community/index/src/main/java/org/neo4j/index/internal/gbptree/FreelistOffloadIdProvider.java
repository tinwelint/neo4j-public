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
import org.neo4j.io.pagecache.PagedFile;

import static java.lang.Integer.min;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.goTo;
import static org.neo4j.index.internal.gbptree.TreeNode.NO_NODE_FLAG;

class FreelistOffloadIdProvider implements OffloadIdProvider
{
    private static final int SIZE_NEXT_POINTER = Long.BYTES;

    private final PagedFile pagedFile;
    private final int pageSize;
    private final IdProvider idProvider;
    private final int recordSize;

    FreelistOffloadIdProvider( PagedFile pagedFile, int pageSize, IdProvider idProvider )
    {
        this.pagedFile = pagedFile;
        this.pageSize = pageSize;
        this.idProvider = idProvider;
        this.recordSize = pageSize / RECORDS_PER_PAGE;
    }

    @Override
    public long allocate( long stableGeneration, long unstableGeneration, int length ) throws IOException
    {
        int recordsRequired = ((length - 1) / maxDataInRecord()) + 1;
        long firstRecordId = NO_NODE_FLAG;
        long lastRecordId = NO_NODE_FLAG;

        // TODO Look in the free-list, a couple of items ahead if worst comes to worst, for an as good match as possible.
        //      If this fulfills our needs then just go for it, otherwise potentially pick some of the records seen here.
        //      Remaining records will have to be allocated from the IdProvider, which can either steal tree-node pages
        //      or allocate at the end of the file, growing the file.

        // TODO Also any items that were read and not considered a good match must be appended to the end of this free-list/
        //      This also goes for free-list items that we decided to use only parts of.

        // TODO Potentially consider an optimization where multiple consecutive records on a page doesn't need to be linked together,
        //      but that would make the algorithm and record id management more complex and we're talking about a ~0.7% storage gain.

        // Allocate remaining by acquiring new pages from IdProvider
        if ( recordsRequired > 0 )
        {
            try ( PageCursor offloadCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                while ( recordsRequired > 0 )
                {
                    long pageId = idProvider.acquireNewId( stableGeneration, unstableGeneration );
                    firstRecordId = firstRecordId == NO_NODE_FLAG ? pageIdToRecordId( pageId ) : firstRecordId;
                    int recordsToAllocateOnThisPage = min( recordsRequired, RECORDS_PER_PAGE );

                    // Stitch the records together
                    for ( int i = 0; i < recordsToAllocateOnThisPage; i++ )
                    {
                        long recordId = pageIdToRecordId( pageId ) + i;
                        if ( lastRecordId != NO_NODE_FLAG )
                        {
                            goTo( offloadCursor, "Stitch offload records", pageId );
                            offloadCursor.putLong( recordOffset( lastRecordId ), recordId );
                        }
                        lastRecordId = recordId;
                    }

                    if ( recordsToAllocateOnThisPage < RECORDS_PER_PAGE )
                    {
                        // TODO place the remaining records on this acquired page on the free-list
                    }

                    recordsRequired -= recordsToAllocateOnThisPage;
                }
            }
        }

        return firstRecordId;
    }

    private int recordOffset( long recordId )
    {
        return recordInPage( recordId ) * recordSize;
    }

    private long pageIdToRecordId( long pageId )
    {
        return pageId * RECORDS_PER_PAGE;
    }

    @Override
    public int maxDataInRecord()
    {
        return recordSize - SIZE_NEXT_POINTER;
    }

    @Override
    public long placeAt( PageCursor cursor, long recordId ) throws IOException
    {
        long pageId = recordId / RECORDS_PER_PAGE;
        int recordInPage = recordInPage( recordId );
        int offset = recordInPage * recordSize;
        goTo( cursor, "OffloadIdProvider.placeAt", pageId );
        cursor.setOffset( offset );
        long next = cursor.getLong();
        return next;
    }

    private int recordInPage( long recordId )
    {
        return (int) (recordId % RECORDS_PER_PAGE);
    }

    // TODO accessors for freelist pointers so that checkpoint can write them into state
}
