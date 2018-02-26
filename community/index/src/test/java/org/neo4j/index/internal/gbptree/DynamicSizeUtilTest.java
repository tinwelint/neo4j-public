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
import org.junit.Test;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.assertEquals;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.SIZE_TOTAL_OVERHEAD;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractHasOffload;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyValueHeader;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyValueSize;

public class DynamicSizeUtilTest
{
    private static final int KEY_ONE_BYTE_MAX = 0x1F;
    private static final int KEY_TWO_BYTE_MIN = KEY_ONE_BYTE_MAX + 1;
    private static final int KEY_TWO_BYTE_MAX = 0x1FFF;
    private static final int VAL_ONE_BYTE_MIN = 1;
    private static final int VAL_ONE_BYTE_MAX = 0x7F;
    private static final int VAL_TWO_BYTE_MIN = VAL_ONE_BYTE_MAX + 1;
    private static final int VAL_TWO_BYTE_MAX = 0x7FFF;
    private static final int OFFLOAD = 100; // the actual size of the offload doesn't quite matter

    private PageCursor cursor;

    @Before
    public void setUp()
    {
        cursor = ByteArrayPageCursor.wrap( 8192 );
    }

    @Test
    public void shouldPutAndGetDiscreteKeyValueSize() throws Exception
    {
        //                                   KEY SIZE          VALUE SIZE        OFFLOAD SIZE   EXPECTED BYTES
        shouldPutAndGetDiscreteKeyValueSize( 0,                0,                0,             1 );
        shouldPutAndGetDiscreteKeyValueSize( 0,                0,                OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( 0,                VAL_ONE_BYTE_MIN, 0,             2 );
        shouldPutAndGetDiscreteKeyValueSize( 0,                VAL_ONE_BYTE_MIN, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( 0,                VAL_ONE_BYTE_MAX, 0,             2 );
        shouldPutAndGetDiscreteKeyValueSize( 0,                VAL_ONE_BYTE_MAX, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( 0,                VAL_TWO_BYTE_MIN, 0,             3 );
        shouldPutAndGetDiscreteKeyValueSize( 0,                VAL_TWO_BYTE_MIN, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( 0,                VAL_TWO_BYTE_MAX, 0,             3 );
        shouldPutAndGetDiscreteKeyValueSize( 0,                VAL_TWO_BYTE_MAX, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_ONE_BYTE_MAX, 0,                0,             1 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_ONE_BYTE_MAX, 0,                OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_ONE_BYTE_MAX, VAL_ONE_BYTE_MIN, 0,             2 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_ONE_BYTE_MAX, VAL_ONE_BYTE_MIN, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_ONE_BYTE_MAX, VAL_ONE_BYTE_MAX, 0,             2 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_ONE_BYTE_MAX, VAL_ONE_BYTE_MAX, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_ONE_BYTE_MAX, VAL_TWO_BYTE_MIN, 0,             3 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_ONE_BYTE_MAX, VAL_TWO_BYTE_MIN, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_ONE_BYTE_MAX, VAL_TWO_BYTE_MAX, 0,             3 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_ONE_BYTE_MAX, VAL_TWO_BYTE_MAX, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MIN, 0,                0,             2 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MIN, 0,                OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MIN, VAL_ONE_BYTE_MIN, 0,             3 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MIN, VAL_ONE_BYTE_MIN, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MIN, VAL_ONE_BYTE_MAX, 0,             3 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MIN, VAL_ONE_BYTE_MAX, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MIN, VAL_TWO_BYTE_MIN, 0,             4 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MIN, VAL_TWO_BYTE_MIN, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MIN, VAL_TWO_BYTE_MAX, 0,             4 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MIN, VAL_TWO_BYTE_MAX, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MAX, 0,                0,             2 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MAX, 0,                OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MAX, VAL_ONE_BYTE_MIN, 0,             3 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MAX, VAL_ONE_BYTE_MIN, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MAX, VAL_ONE_BYTE_MAX, 0,             3 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MAX, VAL_ONE_BYTE_MAX, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MAX, VAL_TWO_BYTE_MIN, 0,             4 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MAX, VAL_TWO_BYTE_MIN, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MAX, VAL_TWO_BYTE_MAX, 0,             4 );
        shouldPutAndGetDiscreteKeyValueSize( KEY_TWO_BYTE_MAX, VAL_TWO_BYTE_MAX, OFFLOAD,       2 + SIZE_TOTAL_OVERHEAD );
    }

    @Test
    public void shouldPutAndGetDiscreteKeySize() throws Exception
    {
        //                              KEY SIZE          EXPECTED BYTES
        shouldPutAndGetDiscreteKeySize( 0,                1 );
        shouldPutAndGetDiscreteKeySize( KEY_ONE_BYTE_MAX, 1 );
        shouldPutAndGetDiscreteKeySize( KEY_TWO_BYTE_MIN, 2 );
        shouldPutAndGetDiscreteKeySize( KEY_TWO_BYTE_MAX, 2 );
    }

    private void shouldPutAndGetDiscreteKeySize( int keySize, int expectedBytes )
    {
        int size = putAndGetKey( keySize );
        assertEquals( expectedBytes, size );
    }

    private int putAndGetKey( int keySize )
    {
        int offsetBefore = cursor.getOffset();
        DynamicSizeUtil.putKeyChildHeader( cursor, keySize );
        int offsetAfter = cursor.getOffset();
        cursor.setOffset( offsetBefore );
        long readKeySize = readKeyValueSize( cursor );
        assertEquals( keySize, extractKeySize( readKeySize ) );
        return offsetAfter - offsetBefore;
    }

    private void shouldPutAndGetDiscreteKeyValueSize( int keySize, int valueSize, int offloadSize, int expectedBytes ) throws Exception
    {
        long offloadReference = offloadSize > 0 ? 1234567890L : TreeNode.NO_NODE_FLAG;
        int size = putAndGetKeyValue( keySize, valueSize, offloadSize, offloadReference );
        assertEquals( expectedBytes, size );
    }

    private int putAndGetKeyValue( int keySize, int valueSize, int offloadSize, long offloadReference )
    {
        int offsetBefore = cursor.getOffset();
        putKeyValueHeader( cursor, keySize, valueSize, offloadSize, offloadReference );
        int offsetAfter = cursor.getOffset();
        cursor.setOffset( offsetBefore );
        long readKeyValueSize = readKeyValueSize( cursor );
        boolean hasOffload = extractHasOffload( readKeyValueSize );
        assertEquals( offloadSize != 0, hasOffload );
        int readKeySize = extractKeySize( readKeyValueSize );
        int readValueSize = extractValueSize( readKeyValueSize );
        assertEquals( keySize, readKeySize );
        assertEquals( valueSize, readValueSize );
        return offsetAfter - offsetBefore;
    }
}
