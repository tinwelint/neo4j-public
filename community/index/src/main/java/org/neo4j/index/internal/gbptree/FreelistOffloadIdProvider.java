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
import static org.neo4j.index.internal.gbptree.OffloadFreelistNode.NO_RECORD;
import static org.neo4j.index.internal.gbptree.OffloadFreelistNode.extractBitSet;
import static org.neo4j.index.internal.gbptree.OffloadFreelistNode.extractPageId;
import static org.neo4j.index.internal.gbptree.OffloadFreelistNode.setNext;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.checkOutOfBounds;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.goTo;

class FreelistOffloadIdProvider implements OffloadIdProvider
{
    private static final int SIZE_NEXT_POINTER = Long.BYTES;
    private static final int SIZE_HEADER = RECORDS_PER_PAGE;
    public static final byte FULL_BIT_SET = (byte) 0xFF;

    private final PagedFile pagedFile;
    private final IdProvider idProvider;
    private final OffloadFreelistNode freelistNode;
    private final int pageSize;
    private final int recordSize;

    private volatile long writePageId;
    private volatile long readPageId;
    private volatile int writePos;
    private volatile int readPos;

    FreelistOffloadIdProvider( PagedFile pagedFile, int pageSize, IdProvider idProvider )
    {
        this.pagedFile = pagedFile;
        this.pageSize = pageSize;
        this.idProvider = idProvider;
        this.recordSize = (pageSize - SIZE_HEADER) / RECORDS_PER_PAGE;
        freelistNode = new OffloadFreelistNode( pageSize );
    }

    @Override
    public long allocate( long stableGeneration, long unstableGeneration, int length ) throws IOException
    {
        int recordsRequired = ((length - 1) / maxDataInRecord()) + 1;
        long firstRecordId = NO_RECORD;
        long lastRecordId = NO_RECORD;

        try ( PageCursor freelistCursor = pagedFile.io( readPageId, PagedFile.PF_SHARED_WRITE_LOCK );
              PageCursor recordCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ))
        {
            long pageId = 0;
            byte bitSet = 0;
            while ( recordsRequired > 0 )
            {
                goTo( freelistCursor, "Offload freelist read", readPageId );
                long readResult = freelistNode.read( freelistCursor, stableGeneration, readPos );

                if ( readResult == NO_RECORD )
                {
                    break;
                }

                advanceReadPos( stableGeneration, unstableGeneration, freelistCursor );

                pageId = extractPageId( readResult );
                bitSet = extractBitSet( readResult );
                // TODO optimize to not loop through it 8 times always, but just the number of 1's in it, see HopScotchHashingAlgorithm
                for ( int recordInPage = 0; recordInPage < Byte.SIZE && recordsRequired > 0; recordInPage++ )
                {
                    int recordBitSetMask = 1 << recordInPage;
                    if ( (bitSet & recordBitSetMask) != 0 )
                    {
                        bitSet &= ~recordBitSetMask;
                        long recordId = asRecordId( pageId, recordInPage );
                        zapRecord( recordCursor, recordId );
                        firstRecordId = firstRecordId == NO_RECORD ? recordId : firstRecordId;
                        if ( lastRecordId != NO_RECORD )
                        {
                            writeHeader( recordCursor, lastRecordId, recordId );
                        }
                        lastRecordId = recordId;
                        recordsRequired--;
                    }
                }
            }

            // TODO release the remaining in the last bitset
            if ( bitSet > 0 )
            {
                appendEntry( stableGeneration, unstableGeneration, freelistCursor, pageId, bitSet );
            }
            checkOutOfBounds( freelistCursor );
            checkOutOfBounds( recordCursor );
        }
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
            try ( PageCursor recordCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                long pageId;
                byte bitSet;
                while ( recordsRequired > 0 )
                {
                    pageId = idProvider.acquireNewId( stableGeneration, unstableGeneration );
                    bitSet = FULL_BIT_SET;
                    firstRecordId = firstRecordId == NO_RECORD ? pageIdToRecordId( pageId ) : firstRecordId;
                    int recordsToAllocateOnThisPage = min( recordsRequired, RECORDS_PER_PAGE );

                    // Stitch the records together

                    for ( int recordInPage = 0; recordInPage < recordsToAllocateOnThisPage; recordInPage++ )
                    {
                        long recordId = asRecordId( pageId, recordInPage );
                        if ( lastRecordId != NO_RECORD )
                        {
                            writeHeader( recordCursor, lastRecordId, recordId );
                        }
                        bitSet &= ~(1 << recordInPage);
                        lastRecordId = recordId;
                    }

                    if ( recordsToAllocateOnThisPage < RECORDS_PER_PAGE )
                    {
                        try ( PageCursor freelistCursor = pagedFile.io( writePageId, PagedFile.PF_SHARED_WRITE_LOCK ) )
                        {
                            goTo( freelistCursor, "Append remaining", writePageId );
                            appendEntry( stableGeneration, unstableGeneration, freelistCursor, pageId, bitSet );
                        }
                    }

                    recordsRequired -= recordsToAllocateOnThisPage;
                }
                writeHeader( recordCursor, lastRecordId, NO_RECORD );
                checkOutOfBounds( recordCursor );
            }
        }

        return firstRecordId;
    }

    static long asRecordId( long pageId, int recordInPage )
    {
        return pageIdToRecordId( pageId ) + recordInPage;
    }

    private void zapRecord( PageCursor cursor, long recordId ) throws IOException
    {
        goTo( cursor, "", recordIdToPageId( recordId ) );
        cursor.setOffset( recordOffset( recordId ) );
        cursor.putBytes( recordSize, (byte) 0 ); // poor man's zap
    }

    @Override
    public void release( long stableGeneration, long unstableGeneration, long recordId ) throws IOException
    {
        // Traverse record chain
        try ( PageCursor recordCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK );
              PageCursor freelistCursor = pagedFile.io( writePageId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            goTo( freelistCursor, "Offload free-list release", writePageId );

            byte bitSet = 0;
            long pageId = recordIdToPageId( recordId );
            while ( recordId != NO_RECORD )
            {
                long nextRecord = placeAt( recordCursor, recordId );
                bitSet |= 1 << recordInPage( recordId );
                recordId = nextRecord;
                if ( recordIdToPageId( recordId ) != pageId || recordId == NO_RECORD )
                {
                    appendEntry( stableGeneration, unstableGeneration, freelistCursor, pageId, bitSet );
                    bitSet = 0;
                    pageId = recordIdToPageId( recordId );
                }
            }

            checkOutOfBounds( freelistCursor );
            checkOutOfBounds( recordCursor );
        }
    }

    // Visible for testing only
    void appendEntry( long stableGeneration, long unstableGeneration, long pageId, byte bitSet ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( writePageId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            appendEntry( stableGeneration, unstableGeneration, cursor, pageId, bitSet );
        }
    }

    private void appendEntry( long stableGeneration, long unstableGeneration, PageCursor cursor, long pageId, byte bitSet ) throws IOException
    {
        goTo( cursor, "Offload append entry", writePageId );
        freelistNode.write( cursor, unstableGeneration, pageId, bitSet, writePos );
        advanceWritePos( stableGeneration, unstableGeneration );
    }

    private void advanceReadPos( long stableGeneration, long unstableGeneration, PageCursor cursor ) throws IOException
    {
        readPos++;
        if ( readPos >= freelistNode.maxEntries() )
        {
            // The current reader page is exhausted, go to the next free-list page.
            long nextReadPageId = OffloadFreelistNode.next( cursor );

            // Put the exhausted free-list page id itself on the free-list
            assert cursor.getCurrentPageId() == readPageId;
            idProvider.releaseId( stableGeneration, unstableGeneration, readPageId );
            readPageId = nextReadPageId;
            readPos = 0;
        }
    }

    private void advanceWritePos( long stableGeneration, long unstableGeneration ) throws IOException
    {
        writePos++;
        if ( writePos >= freelistNode.maxEntries() )
        {
            // Current free-list write page is full, allocate a new one.
            long nextFreelistPage = idProvider.acquireNewId( stableGeneration, unstableGeneration );
            try ( PageCursor cursor = pagedFile.io( writePageId, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                // Link previous --> new writer page
                goTo( cursor, "free-list write page", writePageId );
                setNext( cursor, nextFreelistPage );

                // Initialize newly created
                goTo( cursor, "free-list write page", nextFreelistPage );
                OffloadFreelistNode.initialize( cursor );

                checkOutOfBounds( cursor );
            }
            writePageId = nextFreelistPage;
            writePos = 0;
        }
    }

    private void writeHeader( PageCursor offloadCursor, long recordId, long forwardLink ) throws IOException
    {
        goTo( offloadCursor, "Stitch offload records", recordIdToPageId( recordId ) );
        offloadCursor.putLong( recordOffset( recordId ), forwardLink );
    }

    private int recordOffset( long recordId )
    {
        return SIZE_HEADER + recordInPage( recordId ) * recordSize;
    }

    private static long pageIdToRecordId( long pageId )
    {
        return pageId * RECORDS_PER_PAGE;
    }

    static long recordIdToPageId( long recordId )
    {
        return recordId / RECORDS_PER_PAGE;
    }

    @Override
    public int maxDataInRecord()
    {
        return recordSize - SIZE_NEXT_POINTER;
    }

    @Override
    public long placeAt( PageCursor cursor, long recordId ) throws IOException
    {
        long pageId = recordIdToPageId( recordId );
        int offset = recordOffset( recordId );
        goTo( cursor, "OffloadIdProvider.placeAt", pageId );
        cursor.setOffset( offset );
        return cursor.getLong();
    }

    static int recordInPage( long recordId )
    {
        return (int) (recordId % RECORDS_PER_PAGE);
    }

    void initialize( long writePageId, long readPageId, int writePos, int readPos )
    {
        this.writePageId = writePageId;
        this.readPageId = readPageId;
        this.writePos = writePos;
        this.readPos = readPos;
    }

    void initializeAfterCreation( long stableGeneration, long unstableGeneration ) throws IOException
    {
        // Allocate a new free-list page id and set both write/read free-list page id to it.
        writePageId = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        readPageId = writePageId;

        try ( PageCursor cursor = pagedFile.io( writePageId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            goTo( cursor, "offload free-list", writePageId );
            OffloadFreelistNode.initialize( cursor );
            checkOutOfBounds( cursor );
        }
    }
    // TODO accessors for freelist pointers so that checkpoint can write them into state
}
