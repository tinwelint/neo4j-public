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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Integer.min;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.TreeNode.NO_NODE_FLAG;

public class FreelistOffloadIdProviderTest
{
    private static final int PAGE_SIZE = 256;
    private static final long STABLE_GENERATION = 1;
    private static final long UNSTABLE_GENERATION = 2;

    private PageAwareByteArrayCursor cursor;
    private final PagedFile pagedFile = mock( PagedFile.class );
    private final SimpleIdProvider idProvider = new SimpleIdProvider( () -> cursor );
    private final FreelistOffloadIdProvider freelist = new FreelistOffloadIdProvider( pagedFile, PAGE_SIZE, idProvider );

    @Rule
    public final RandomRule random = new RandomRule();

    @Before
    public void setUpPagedFile() throws IOException
    {
        cursor = new PageAwareByteArrayCursor( PAGE_SIZE );
        when( pagedFile.io( anyLong(), anyInt() ) ).thenAnswer(
                invocation -> cursor.duplicate( invocation.getArgument( 0 ) ) );
        // TODO initialize?
    }

    @Test
    public void shouldAllocateOneRecordFromIdProvider() throws IOException
    {
        // given
        byte[] bytes = randomBytes( 10 );

        // when
        long recordId = freelist.allocate( STABLE_GENERATION, UNSTABLE_GENERATION, bytes.length );
        long next = freelist.placeAt( cursor, recordId );
        cursor.putBytes( bytes );

        // then
        long nextAgain = freelist.placeAt( cursor, recordId );
        assertEquals( next, nextAgain );
        assertEquals( NO_NODE_FLAG, next );
        byte[] readBytes = new byte[bytes.length];
        cursor.getBytes( readBytes );
        assertArrayEquals( bytes, readBytes );
    }

    @Test
    public void shouldAllocateMultipleRecordsOnOnePageFromIdProvider() throws IOException
    {
        // given
        long lastPageIdBefore = idProvider.lastId();
        byte[] bytes = randomBytes( freelist.maxDataInRecord() * 3 + freelist.maxDataInRecord() / 2 ); // 3.5 records

        // when
        long recordId = freelist.allocate( STABLE_GENERATION, UNSTABLE_GENERATION, bytes.length );
        write( bytes, recordId );

        // then
        byte[] readBytes = read( recordId, bytes.length );
        assertArrayEquals( bytes, readBytes );
        assertThat( idProvider.lastId(), is( lastPageIdBefore + 1 ) );
    }

    @Test
    public void shouldAllocateMultipleRecordsOnMultiplePagesFromIdProvider() throws Exception
    {
        // given
        long lastPageIdBefore = idProvider.lastId();
        byte[] bytes = randomBytes( freelist.maxDataInRecord() * OffloadIdProvider.RECORDS_PER_PAGE + 1 + freelist.maxDataInRecord() / 2 ); // 9.5 records

        // when
        long recordId = freelist.allocate( STABLE_GENERATION, UNSTABLE_GENERATION, bytes.length );
        write( bytes, recordId );

        // then
        byte[] readBytes = read( recordId, bytes.length );
        assertArrayEquals( bytes, readBytes );
        assertThat( idProvider.lastId(), is( lastPageIdBefore + 2 ) );
    }

    private byte[] read( long recordId, int length ) throws IOException
    {
        byte[] bytes = new byte[length];
        long current = recordId;
        for ( int i = 0; i < length; )
        {
            current = freelist.placeAt( cursor, current );
            int thisRead = min( length - i, freelist.maxDataInRecord() );
            cursor.getBytes( bytes, i, thisRead );
            i += thisRead;
        }
        return bytes;
    }

    private void write( byte[] bytes, long recordId ) throws IOException
    {
        long current = recordId;
        for ( int i = 0; i < bytes.length; )
        {
            current = freelist.placeAt( cursor, current );
            int thisWrite = min( bytes.length - i, freelist.maxDataInRecord() );
            cursor.putBytes( bytes, i, thisWrite );
            i += thisWrite;
        }
        assertEquals( NO_NODE_FLAG, current );
    }

    // TODO shouldAllocateOneRecordFromFreelistPerfectMatch
    // TODO shouldAllocateOneRecordFromFreelistBadMatch
    // TODO shouldAllocateMultipleRecordsFromFreelistPerfectMatch
    // TODO shouldAllocateMultipleRecordsFromFreelistBadMatchSamePage
    // TODO shouldAllocateMultipleRecordsFromFreelistBadMatchScattered
    // TODO shouldAllocateSomeRecordsFromFreelistAndRestFromIdProvider

    private byte[] randomBytes( int length )
    {
        byte[] bytes = new byte[length];
        random.nextBytes( bytes );
        return bytes;
    }
}
