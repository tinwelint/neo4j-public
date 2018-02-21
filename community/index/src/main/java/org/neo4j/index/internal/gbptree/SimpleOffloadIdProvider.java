package org.neo4j.index.internal.gbptree;

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
    public long allocate( int length )
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
