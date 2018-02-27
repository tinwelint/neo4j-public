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

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongArrayQueue;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.kernel.impl.store.newprop.Freelist.NULL_MARKER;
import static org.neo4j.kernel.impl.store.newprop.ProposedFormat.BEHAVIOUR_REUSE_IDS;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.EFFECTIVE_UNITS_PER_PAGE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.PAGE_SIZE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNITS_PER_PAGE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.pageIdForRecord;
import static org.neo4j.kernel.impl.store.newprop.Utils.debug;

/**
 * Assume unit size 64, just for simplicity sake.
 * So the traverseCursor starts at page 0 and moved forwards until hitting the end,
 * at which point it will restart from 0 again.
 * TODO thought: there could be a higher level bitset in memory, one bit per page that has gotten a free() call to it.
 * This way we could optimize the scan for scenarios where there are very little calls to free(). Quick calculation: for a 1TiB store,
 * where the freelist will occupy 2GiB then such a high-level bitset will occupy 32k memory
 */
class BitsetFreelist implements Freelist
{
    private static final int BITS_PER_PAGE = PAGE_SIZE * Byte.SIZE;
    private static final int BEHAVIOUR_NUMBER_OF_PAGES_TO_SEARCH = 5;
    private static final long NO_ID = -1;
    private static final int SCAN_THRESHOLD = 1_000;

    private final PagedFile pagedFile;
    private final PageCursor traverseCursor;
    private boolean traverseHitEnd = true;
    // 1, 2, 4, 8, 16, 32, 64, 128
    private final CachedIds[] cachedIds = new CachedIds[8];
    private int globalEstimatedUncollectedSlots = SCAN_THRESHOLD;
    private final byte[] readBuffer;
    private final SlotVisitor slotCacher = this::cacheFreeId;

    private long highId;

    BitsetFreelist( PagedFile pagedFile ) throws IOException
    {
        assert pagedFile.pageSize() == PAGE_SIZE;

        this.pagedFile = pagedFile;
        this.traverseCursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK );
        for ( int i = 0; i < cachedIds.length; i++ )
        {
            cachedIds[i] = new CachedIds();
        }
        this.readBuffer = new byte[PAGE_SIZE];

        if ( traverseCursor.next( pagedFile.getLastPageId() ) )
        {
            HighIdScanner scanner = new HighIdScanner();
            scanFreelistPage( scanner );
            this.highId = scanner.foundHighId;
        }
    }

    @Override
    public synchronized long allocate( int slots ) throws IOException
    {
        if ( slots > EFFECTIVE_UNITS_PER_PAGE )
        {
            throw new UnsupportedOperationException( "TODO implement support for records spanning multiple pages" );
        }

        if ( BEHAVIOUR_REUSE_IDS )
        {
            long startId = getCachedSlot( slots );
            if ( startId == NO_ID  && cacheSomeMore() )
            {
                startId = getCachedSlot( slots );
            }
            if ( startId != NO_ID )
            {
                System.out.println( "reusing startId = " + startId );
                assert debug( "CACHE: Found cached slot id %d matching units %d", startId, slots );
                return startId;
            }
        }

        long startId = highId;
        long pageIdForEnd = pageIdForRecord( startId + slots - 1 );
        if ( pageIdForRecord( startId ) != pageIdForEnd )
        {
            // Would have crossed page boundary, go to the next page
            // TODO could add the skipped ids here to the freelist?
            highId = pageIdForEnd * EFFECTIVE_UNITS_PER_PAGE;
        }
        startId = highId;
        highId += slots;

        // Don't mark the unit as used right here, the caller will handle that.
        return startId;
    }

    private boolean worthTheEffortOfScanningFreelistPages()
    {
        return !traverseHitEnd || (traverseHitEnd && globalEstimatedUncollectedSlots >= SCAN_THRESHOLD);
    }

    private long getCachedSlot( int units )
    {
        for ( int slot = slotIndexForGet( units ); slot < cachedIds.length; slot++ )
        {
            long candidate = cachedIds[slot].poll();
            if ( candidate != NO_ID )
            {
                return candidate;
            }
        }

        return NO_ID;
    }

    private boolean cacheSomeMore() throws IOException
    {
        if ( traverseHitEnd )
        {
            if ( worthTheEffortOfScanningFreelistPages() )
            {
                clearCachedIds();
                System.out.println( "Starting new freelist scan" );
            }
            else
            {
                return false;
            }
        }

        for ( int i = 0; i < BEHAVIOUR_NUMBER_OF_PAGES_TO_SEARCH; i++ )
        {
            long nextPageId = traverseHitEnd ? 0 : traverseCursor.getCurrentPageId() + 1;
            traverseHitEnd = !traverseCursor.next( nextPageId );
            if ( traverseHitEnd )
            {
                System.out.println( "Ending freelist scan" );
                break;
            }

            if ( scanFreelistPage( slotCacher ) )
            {
                return true;
            }
        }
        return false;
    }

    private void clearCachedIds()
    {
        for ( CachedIds ids : cachedIds )
        {
            ids.clear();
        }
        globalEstimatedUncollectedSlots = 0;
        assert debug( "CACHE: clear" );
    }

    private boolean scanFreelistPage( SlotVisitor visitor ) throws IOException
    {
        int retries = 0;
        do
        {
            traverseCursor.getBytes( readBuffer );
            if ( retries++ > 5 )
            {
                return false;
            }
        }
        while ( traverseCursor.shouldRetry() );
        sanityCheckCursorRead( traverseCursor );

        // We read the page consistently, now on to caching the ids from it
        long baseId = BITS_PER_PAGE * traverseCursor.getCurrentPageId();
        int startBit = -1;
        boolean changed = false;
        int bit = 0;
        for ( int byteIndex = 0; byteIndex < PAGE_SIZE && baseId + bit < highId; byteIndex++ )
        {
            byte currentByte = readBuffer[byteIndex];
            for ( int i = 0; i < Byte.SIZE && baseId + bit < highId; i++, bit++ )
            {
                int mask = 1 << i;
                boolean inUse = (currentByte & mask) != 0;
                assert debug( "SCAN %d %b", bit, inUse );
                if ( inUse )
                {
                    if ( startBit != -1 )
                    {
                        int slotSize = bit - startBit;
                        if ( slotSize > 0 && visitor.freeSlot( baseId + startBit, slotSize ) )
                        {
                            changed = true;
                        }
                        startBit = -1;
                    }
                }
                else // free
                {
                    if ( startBit == -1 )
                    {
                        startBit = bit;
                    }
                    else if ( bit - startBit + 1 == UNITS_PER_PAGE )
                    {
                        if ( cacheFreeId( baseId + startBit, UNITS_PER_PAGE ) )
                        {
                            changed = true;
                        }
                        startBit = -1;
                    }
                }
            }
        }

        if ( startBit != -1 )
        {
            int slotSize = bit - startBit;
            if ( slotSize > 0 && cacheFreeId( baseId + startBit, slotSize ) )
            {
                changed = true;
            }
        }
        return changed;
    }

    private void sanityCheckCursorRead( PageCursor cursor ) throws CursorException
    {
        if ( cursor.checkAndClearBoundsFlag() )
        {
            throw new CursorException( "Out of bounds" );
        }
        cursor.checkAndClearCursorException();
    }

    private boolean cacheFreeId( long id, int slotSize )
    {
        // TODO oh and check if this id is already cached... should be enough to compare with the first id (of each slot size???)
        // or perhaps clear all ids when wrapping around the traverseCursor?
        assert slotSize <= UNITS_PER_PAGE : slotSize;
        int slotIndex = slotIndexForPut( slotSize );
        assert debug( "CACHE: Caching %d of size %d into slot index %d", id, slotSize, slotIndex );
        return cachedIds[slotIndex].add( id );
    }

    private static int slotIndexForPut( int slotSize )
    {
        int high = Integer.highestOneBit( slotSize );
        return Integer.numberOfTrailingZeros( high );
    }

    private static int slotIndexForGet( int slotSize )
    {
        int high = Integer.highestOneBit( slotSize );
        return Integer.numberOfTrailingZeros( high == slotSize ? high : high << 1 );
    }

    @Override
    public Marker commitMarker() throws IOException
    {
        return new TheMarker( pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) );
    }

    @Override
    public Marker reuseMarker()
    {
        // This only results in this free-list not playing nice with reuse
        return NULL_MARKER;
    }

    @Override
    public void close() throws IOException
    {
        traverseCursor.close();
        pagedFile.close();
    }

    private class CachedIds
    {
        private static final int CAPACITY = 1 << 13;

        private final PrimitiveLongArrayQueue queue = new PrimitiveLongArrayQueue( CAPACITY );
        private boolean wrappedAround;
        // updated while doing other operations, like freeing as well as scanning and helps decide whether or not it's
        // worth starting another scan ones the current one is completed.
        private long estimatedUncollectedSlots;

        boolean add( long id )
        {
            if ( wrappedAround )
            {
                wrappedAround = false;
                queue.clear();
            }
            if ( queue.size() < CAPACITY )
            {
                queue.enqueue( id );
                return true;
            }
            estimatedUncollectedSlots++;
            return false;
        }

        void clear()
        {
            estimatedUncollectedSlots = 0;
            wrappedAround = true;
        }

        long poll()
        {
            return queue.isEmpty() ? NO_ID : queue.dequeue();
        }

        void incFreeEstimate()
        {
            estimatedUncollectedSlots++;
            globalEstimatedUncollectedSlots++;
        }
    }

    class TheMarker implements Freelist.Marker
    {
        private final PageCursor cursor;

        TheMarker( PageCursor cursor )
        {
            this.cursor = cursor;
        }

        @Override
        public void mark( long id, int slotSize, boolean inUse ) throws IOException
        {
            long pageId = id / BITS_PER_PAGE;
            int bitOffset = (int) (id % BITS_PER_PAGE);
            int byteOffset = bitOffset / Byte.SIZE;
            int bitInByte = bitOffset % Byte.SIZE;
            cursor.next( pageId );
            for ( int i = 0; i < slotSize; )
            {
                byte currentByte = cursor.getByte( byteOffset );
                for ( ; bitInByte < Byte.SIZE && i < slotSize; i++, bitInByte++ )
                {
                    int mask = 1 << bitInByte;
                    if ( inUse )
                    {
                        currentByte |= mask;
                    }
                    else
                    {
                        currentByte &= ~mask;
                    }
                    assert debug( "MARK %d %b", id + i, inUse );
                }
                cursor.putByte( byteOffset, currentByte );

                if ( i < slotSize )
                {
                    byteOffset++;
                    bitInByte = 0;
                }
            }

            if ( !inUse )
            {
                // even if this id is in front of the current scan and hence will be collected in this scan then
                // still account for it because this is a best-effort optimization anyway.
                cachedIds[slotIndexForPut( slotSize )].incFreeEstimate();
            }
        }

        @Override
        public void close()
        {
            cursor.close();
        }
    }

    interface SlotVisitor
    {
        boolean freeSlot( long id, int slotSize );
    }

    class HighIdScanner implements SlotVisitor
    {
        private long foundHighId;

        @Override
        public boolean freeSlot( long id, int slotSize )
        {
            foundHighId = id + slotSize;
            return true;
        }
    }
}
