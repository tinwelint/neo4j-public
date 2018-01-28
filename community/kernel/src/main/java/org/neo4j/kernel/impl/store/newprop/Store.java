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
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.EFFECTIVE_UNITS_PER_PAGE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.offsetForId;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.pageIdForRecord;

/**
 * A very simple store abstraction on top of a {@link PagedFile}. Provides {@link PageCursor} and allocates ids
 * to other parties that wants to implement some sort of actual store.
 *
 * Access, whether it being read or write, is visitor-based to get the good parts of {@link PageCursor#shouldRetry()}
 * checking as well as bounds/cursor exception checking. Also id --> pageId/offset calculations.
 *
 * This store assumes 64 byte record units with the first unit in each page reserved for {@link Header} data,
 * which contains inUse and startUnit information about all the other records.
 *
 * What this class is really lacking is free-list management. Generally using this store you'd probably want to allocate
 * records of one or more consecutive units so a normal dumb free-list doesn't do very well. i.e. TODO free-list
 */
class Store implements Closeable
{
    static final long SPECIAL_ID_SHOULD_RETRY = -2;

    final PagedFile storeFile;
    private final int pageSize;
    private final AtomicLong nextId = new AtomicLong();

    Store( PageCache pageCache, File directory, String name ) throws IOException
    {
        this.pageSize = pageCache.pageSize();
        this.storeFile = pageCache.map( new File( directory, name ), pageSize, CREATE, WRITE, READ );
    }

    /**
     * Allocates a number of consecutive units in this store. Allocation generally prioritizes all units residing
     * on the same page.
     *
     * @param units number of 64 byte units to allocate.
     * @return the start id of this record.
     * @throws IOException on page cache allocation error.
     */
    long allocate( int units ) throws IOException
    {
        if ( units > EFFECTIVE_UNITS_PER_PAGE )
        {
            throw new UnsupportedOperationException( "TODO implement support for records spanning multiple pages" );
        }

        // TODO make thread-safe
        long startId = nextId.get();
        if ( pageIdForRecord( startId ) != pageIdForRecord( startId + units - 1 ) )
        {
            // Crossing page boundary, go to the next page
            long idOfNextPage = pageIdForRecord( startId + units - 1 );
            nextId.set( idOfNextPage * EFFECTIVE_UNITS_PER_PAGE );
        }
        startId = nextId.getAndAdd( units );

        // TODO Let's predict if we'll cross a page boundary and if so start on a new page instead.
        // TODO a bit weird to put stuff in the header here, isn't it? This is before the data has arrived
        try ( PageCursor cursor = storeFile.io( pageIdForRecord( startId ), PF_SHARED_WRITE_LOCK ) )
        {
            cursor.next();
            Header.mark( cursor, startId, units, true );
        }
        return startId;
    }

    private PageCursor cursor( int flags ) throws IOException
    {
        return storeFile.io( 0, flags );
    }

    /**
     * Allocates a {@link PageCursor} capable of making changes to this store. The returned instances should be passed
     * into {@link #access(long, PageCursor, RecordVisitor)} when reading from and/or writing to this store.
     *
     * @return a {@link PageCursor} capable of reading from and writing to pages.
     * @throws IOException on page cursor I/O error.
     */
    PageCursor writeCursor() throws IOException
    {
        return cursor( PagedFile.PF_SHARED_WRITE_LOCK );
    }

    /**
     * Allocates a {@link PageCursor} only capable of reading data from this store. The returned instances should be passed
     * into {@link #access(long, PageCursor, RecordVisitor)} when reading from this store.
     *
     * @return a {@link PageCursor} capable of reading from pages in this store.
     * @throws IOException on page cursor I/O error.
     */
    PageCursor readCursor() throws IOException
    {
        return cursor( PagedFile.PF_SHARED_READ_LOCK );
    }

    /**
     * Accesses this store using an allocated {@link PageCursor}, with one of the {@link #writeCursor()} or {@link #readCursor()}
     * methods. The access is driven by the {@link RecordVisitor} which can (a) carry data into the store
     * and (b) retrieve data from the stare and carry it back to the caller.
     *
     * Consistency, i.e. {@link PageCursor#shouldRetry()} and bounds checking is managed in this call and mostly invisible
     * to the visitor.
     *
     * {@link Header} is consulted to get number of units for the given record id, information which is passed into
     * {@link RecordVisitor#accept(PageCursor)}, after visitor has been {@link RecordVisitor#initialize(PageCursor, long, int) initialized}.
     *
     * @param id record id, as handed out by {@link #allocate(int)}.
     * @param cursor {@link PageCursor} as handed out by {@link #writeCursor()} or {@link #readCursor()}.
     * @param visitor {@link RecordVisitor} with user-specific logic accessing this store.
     * @throws IOException on {@link PageCursor} I/O error.
     */
    void access( long id, PageCursor cursor, RecordVisitor visitor ) throws IOException
    {
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        if ( cursor.next( pageId ) )
        {
            boolean forceRetry;
            do
            {
                forceRetry = false;
                int units = Header.numberOfUnits( cursor, id );
                cursor.setOffset( offset );
                visitor.initialize( cursor, id, units );
                long nextId = visitor.accept( cursor );
                if ( nextId == SPECIAL_ID_SHOULD_RETRY )
                {
                    forceRetry = true;
                }
                else if ( nextId != -1 )
                {
                    // TODO this will actually never happen yet
                    pageId = pageIdForRecord( nextId );
                    offset = offsetForId( nextId );
                    if ( !cursor.next( pageId ) )
                    {
                        break;
                    }
                    forceRetry = true;
                }
            }
            while ( forceRetry | cursor.shouldRetry() );
            cursor.checkAndClearBoundsFlag();
            if ( !cursor.isWriteLocked() )
            {
                cursor.checkAndClearCursorException();
            }
        }
    }

    /**
     * Close this store so that no more access can be served.
     *
     * @throws IOException on {@link PageCache} I/O error.
     */
    @Override
    public void close() throws IOException
    {
        storeFile.close();
    }

    /**
     * Logic for accessing this store.
     */
    interface RecordVisitor
    {
        /**
         * Initializes this visitor with cursor set at the beginning of the record.
         * @param cursor {@link PageCursor} at the beginning of the record.
         * @param startId record id retrieved from {@link Store#allocate(int)}.
         * @param units number of units that makes up this record.
         */
        void initialize( PageCursor cursor, long startId, int units );

        /**
         * Called by {@link Store#access(long, PageCursor, RecordVisitor)} after {@link #initialize(PageCursor, long, int)} have
         * been called and the proper ceremonies around {@link PageCursor} have been made. This visitor is now allowed
         * to read or write (depending on cursor of course) what ever it wants to read/write.
         * For reading {@link PageCursor#shouldRetry()} may result in this method being called multiple times for a single
         * read, so the implementation must be able to handle that.
         *
         * @param cursor {@link PageCursor} retrieved from {@link Store#readCursor()} or {@link Store#writeCursor()}.
         * @return -1 if no more pages should be accessed, non-negative if the access should
         * continue in a new place, another id.
         * @throws IOException on {@link PageCursor} I/O error.
         */
        long accept( PageCursor cursor ) throws IOException;
    }
}
