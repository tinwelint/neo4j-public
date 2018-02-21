package org.neo4j.index.internal.gbptree;

import org.neo4j.io.pagecache.PageCursor;

interface OffloadIdProvider
{
    /**
     * Allocates offload record ids capable of storing {@code length} number of bytes.
     *
     * TODO potentially the returned data structure could look a bit different, but long[] sort of does the trick and is very generic
     *
     * @param length number of bytes to allocate offload records for,
     * @return allocated offload record ids.
     */
    long allocate( int length );

    int maxDataInRecord();

    long placeAt( PageCursor cursor, long recordId );
}
