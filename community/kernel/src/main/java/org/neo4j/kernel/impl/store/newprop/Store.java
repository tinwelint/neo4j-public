package org.neo4j.kernel.impl.store.newprop;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

public class Store implements Closeable
{
    private final PagedFile storeFile;
    private final int recordSize;
    private final int pageSize;
    private final AtomicLong nextId = new AtomicLong();

    public Store( PageCache pageCache, File directory, String name, int recordSize ) throws IOException
    {
        this.recordSize = recordSize;
        this.pageSize = pageCache.pageSize();
        int storeFilePageSize = pageSize - pageSize % recordSize;
        this.storeFile = pageCache.map( new File( directory, name ), storeFilePageSize,
                CREATE, WRITE, READ );
    }

    protected long pageIdForRecord( long id )
    {
        return id * recordSize / pageSize;
    }

    protected int offsetForId( long id )
    {
        return (int) (id * recordSize % pageSize);
    }

    public long allocate( int units )
    {
        // Let's predict if we'll cross a page boundary and if so start on a new page instead.
        return nextId.getAndAdd( units );
    }

    protected void access( long id, int flags, RecordVisitor visitor )
    {
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        try ( PageCursor cursor = storeFile.io( pageId, flags ) )
        {
            if ( cursor.next() )
            {
                boolean moved = false;
                do
                {
                    cursor.setOffset( offset );
                    long nextId = visitor.accept( cursor );
                    if ( nextId != -1 )
                    {
                        pageId = pageIdForRecord( nextId );
                        offset = offsetForId( nextId );
                        if ( !cursor.next( pageId ) )
                        {
                            break;
                        }
                        moved = true;
                    }
                }
                while ( moved || cursor.shouldRetry() );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    // Just convenience
    protected void accessForWriting( long id, RecordVisitor visitor )
    {
        access( id, PF_SHARED_WRITE_LOCK, visitor );
    }

    protected void accessForReading( long id, RecordVisitor visitor )
    {
        access( id, PF_SHARED_READ_LOCK, visitor );
    }

    @Override
    public void close() throws IOException
    {
        storeFile.close();
    }

    interface RecordVisitor
    {
        /**
         * @return -1 if no more pages should be accessed, non-negative if the access should
         * continue in a new place, another id.
         */
        long accept( PageCursor cursor );
    }
}
