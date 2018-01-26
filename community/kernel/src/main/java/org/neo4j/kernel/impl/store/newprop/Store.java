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

    PageCursor writeCursor() throws IOException
    {
        return cursor( PagedFile.PF_SHARED_WRITE_LOCK );
    }

    PageCursor readCursor() throws IOException
    {
        return cursor( PagedFile.PF_SHARED_READ_LOCK );
    }

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
                visitor.initialize( cursor );
                long nextId = visitor.accept( cursor, id, units );
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

    @Override
    public void close() throws IOException
    {
        storeFile.close();
    }

    interface RecordVisitor
    {
        /**
         * Initializes this visitor with cursor set at the beginning of the record.
         * @param cursor {@link PageCursor} at the beginning of the record.
         */
        void initialize( PageCursor cursor );

        /**
         * @return -1 if no more pages should be accessed, non-negative if the access should
         * continue in a new place, another id.
         */
        long accept( PageCursor cursor, long startId, int units ) throws IOException;
    }
}
