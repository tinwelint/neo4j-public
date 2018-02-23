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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongList;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Integer.min;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.FreelistOffloadIdProvider.asRecordId;
import static org.neo4j.index.internal.gbptree.IdSpace.MIN_TREE_NODE_ID;
import static org.neo4j.index.internal.gbptree.TreeNode.NO_NODE_FLAG;

public class FreelistOffloadIdProviderTest
{
    private static final int PAGE_SIZE = 256;
    private long stableGeneration = 1;
    private long unstableGeneration = 2;

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
        freelist.initializeAfterCreation( stableGeneration, unstableGeneration );
    }

    @Test
    public void shouldAllocateOneRecordFromIdProvider() throws IOException
    {
        // given
        byte[] bytes = randomBytes( 10 );

        // when
        long recordId = freelist.allocate( stableGeneration, unstableGeneration, bytes.length );
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
        long recordId = freelist.allocate( stableGeneration, unstableGeneration, bytes.length );
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
        long recordId = freelist.allocate( stableGeneration, unstableGeneration, bytes.length );
        write( bytes, recordId );

        // then
        byte[] readBytes = read( recordId, bytes.length );
        assertArrayEquals( bytes, readBytes );
        assertThat( idProvider.lastId(), is( lastPageIdBefore + 2 ) );
    }

    @Test
    public void shouldReuseOneRecordFromFreelist() throws Exception
    {
        // given
        long toReuse = asRecordId( MIN_TREE_NODE_ID, 0 );
        appendFreelistEntry( pageId( toReuse ), bitSet( toReuse ) );
        long lastPageIdBefore = idProvider.lastId();

        // when
        checkpoint();
        long allocated = freelist.allocate( stableGeneration, unstableGeneration, 1 );

        // then
        assertThat( idProvider.lastId(), is( lastPageIdBefore ) );
        assertThat( allocated, is( toReuse ) );
    }

    @Test
    public void shouldReuseMultipleRecordsFromSamePage() throws Exception
    {
        // given
        appendFreelistEntry( MIN_TREE_NODE_ID, (byte) 0b11 );
        long lastPageIdBefore = idProvider.lastId();

        // when
        checkpoint();
        long firstAllocated = freelist.allocate( stableGeneration, unstableGeneration, freelist.maxDataInRecord() * 2 );
        long secondAllocated = freelist.placeAt( cursor, firstAllocated );

        // then
        assertThat( idProvider.lastId(), is( lastPageIdBefore ) );
        assertThat( firstAllocated, is( asRecordId( MIN_TREE_NODE_ID, 0 ) ) );
        assertThat( secondAllocated, is( asRecordId( MIN_TREE_NODE_ID, 1 ) ) );
    }

    @Test
    public void shouldReuseMultipleRecordsFromMultiplePages() throws IOException
    {
        // given
        long firstFreePage = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        long secondFreePage = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        appendFreelistEntry( firstFreePage, (byte) 0b1 );
        appendFreelistEntry( secondFreePage + 1, (byte) 0b1 );
        long lastPageIdBefore = idProvider.lastId();

        // when
        checkpoint();
        long firstAllocated = freelist.allocate( stableGeneration, unstableGeneration, freelist.maxDataInRecord() * 2 );
        long secondAllocated = freelist.placeAt( cursor, firstAllocated );

        // then
        assertThat( idProvider.lastId(), is( lastPageIdBefore ) );
        assertThat( firstAllocated, is( asRecordId( firstFreePage, 0 ) ) );
        assertThat( secondAllocated, is( asRecordId( secondFreePage + 1, 0 ) ) );
    }

    @Test
    public void shouldReuseSomeRecordsFromFreelistAndRestFromIdProvider() throws IOException
    {
        // given
        long freePage = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        appendFreelistEntry( freePage, (byte) 0b11 );
        long lastPageIdBefore = idProvider.lastId();

        // when
        checkpoint();
        byte[] data = randomBytes( freelist.maxDataInRecord() * 4 );
        long firstAllocated = freelist.allocate( stableGeneration, unstableGeneration, data.length );
        write( data, firstAllocated );

        // then
        assertThat( idProvider.lastId(), is( lastPageIdBefore + 1 ) );
        assertArrayEquals( data, read( firstAllocated, data.length ) );
    }

    @Test
    public void shouldAddUnusedReusedRecordsToFreelistOnAllocate() throws IOException
    {
        // given
        long freePage = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        appendFreelistEntry( freePage, (byte) 0b1111 );
        long lastPageIdBefore = idProvider.lastId();
        checkpoint();
        byte[] firstData = randomBytes( freelist.maxDataInRecord() * 2 );
        long firstAllocated = freelist.allocate( stableGeneration, unstableGeneration, firstData.length );
        write( firstData, firstAllocated );

        // when
        checkpoint();
        byte[] secondData = randomBytes( freelist.maxDataInRecord() * 2 );
        long secondAllocated = freelist.allocate( stableGeneration, unstableGeneration, secondData.length );
        write( secondData, secondAllocated );

        // then
        assertThat( idProvider.lastId(), is( lastPageIdBefore ) );
        assertArrayEquals( firstData, read( firstAllocated, firstData.length ) );
        assertArrayEquals( secondData, read( secondAllocated, secondData.length ) );
    }

    @Test
    public void shouldAddUnusedAcquiredRecordsToFreelistOnAllocate() throws IOException
    {
        // given
        byte[] firstData = randomBytes( freelist.maxDataInRecord() * 2 );
        long firstAllocated = freelist.allocate( stableGeneration, unstableGeneration, firstData.length );
        long lastPageIdBefore = idProvider.lastId();
        write( firstData, firstAllocated );

        // when
        checkpoint();
        byte[] secondData = randomBytes( freelist.maxDataInRecord() * 2 );
        long secondAllocated = freelist.allocate( stableGeneration, unstableGeneration, secondData.length );
        write( secondData, secondAllocated );

        // then
        assertThat( idProvider.lastId(), is( lastPageIdBefore ) );
        assertArrayEquals( firstData, read( firstAllocated, firstData.length ) );
        assertArrayEquals( secondData, read( secondAllocated, secondData.length ) );
    }

    @Test
    public void shouldStayBoundUnderStress() throws Exception
    {
        // GIVEN
        PrimitiveLongSet acquired = Primitive.longSet();
        PrimitiveLongList acquiredList = Primitive.longList(); // for quickly finding random to remove
        long stableGeneration = GenerationSafePointer.MIN_GENERATION;
        long unstableGeneration = stableGeneration + 1;
        int iterations = 100;

        // WHEN
        for ( int i = 0; i < iterations; i++ )
        {
            for ( int j = 0; j < 10; j++ )
            {
                if ( random.nextBoolean() )
                {
                    // allocate
                    int recordLength = random.intBetween( 5, 10 );
                    long recordId = freelist.allocate( stableGeneration, unstableGeneration, recordLength * freelist.maxDataInRecord() );
                    assertTrue( acquired.add( recordId ) );
                    acquiredList.add( recordId );
                }
                else if ( !acquired.isEmpty() )
                {
                    // release
                    long recordId = acquiredList.remove( random.nextInt( acquiredList.size() ) );
                    assertTrue( acquired.remove( recordId ) );
                    freelist.release( stableGeneration, unstableGeneration, recordId );
                }
            }

            for ( int j = 0; j < acquiredList.size(); j++ )
            {
                freelist.release( stableGeneration, unstableGeneration, acquiredList.get( j ) );
            }
            acquiredList.clear();
            acquired.clear();

            // checkpoint, sort of
            stableGeneration = unstableGeneration;
            unstableGeneration++;
        }

        // THEN
        assertTrue( String.valueOf( idProvider.lastId() ), idProvider.lastId() < 50 );
    }

    private long pageId( long recordId )
    {
        return FreelistOffloadIdProvider.recordIdToPageId( recordId );
    }

    private byte bitSet( long recordId )
    {
        return (byte) (1 << (FreelistOffloadIdProvider.recordInPage( recordId )));
    }

    private void appendFreelistEntry( long pageId, byte bitSet ) throws IOException
    {
        freelist.appendEntry( stableGeneration, unstableGeneration, pageId, bitSet );
    }

    private void checkpoint()
    {
        stableGeneration = unstableGeneration;
        unstableGeneration++;
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
            long next = freelist.placeAt( cursor, current );
            int thisWrite = min( bytes.length - i, freelist.maxDataInRecord() );
            cursor.putBytes( bytes, i, thisWrite );
            i += thisWrite;
            current = next;
        }
        assertEquals( NO_NODE_FLAG, current );
    }

    private byte[] randomBytes( int length )
    {
        byte[] bytes = new byte[length];
        random.nextBytes( bytes );
        return bytes;
    }
}
