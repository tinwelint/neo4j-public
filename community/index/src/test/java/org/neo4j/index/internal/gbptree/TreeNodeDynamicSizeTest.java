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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;

public class TreeNodeDynamicSizeTest extends TreeNodeTestBase<RawBytes,RawBytes>
{
    private SimpleByteArrayLayout layout = new SimpleByteArrayLayout();

    @Override
    protected TestLayout<RawBytes,RawBytes> getLayout()
    {
        return layout;
    }

    @Override
    protected TreeNodeDynamicSize<RawBytes,RawBytes> getNode( int pageSize, Layout<RawBytes,RawBytes> layout, OffloadIdProvider offloadIdProvider )
    {
        return new TreeNodeDynamicSize<>( pageSize, layout, offloadIdProvider );
    }

    @Override
    void assertAdditionalHeader( PageCursor cursor, TreeNode<RawBytes,RawBytes> node, int pageSize )
    {
        // When
        int currentAllocSpace = ((TreeNodeDynamicSize) node).getAllocOffset( cursor );

        // Then
        assertEquals("allocSpace point to end of page", pageSize, currentAllocSpace );
    }

    @Test
    public void mustCompactKeyValueSizeHeader() throws Exception
    {
        int oneByteKeyMax = DynamicSizeUtil.MASK_ONE_BYTE_KEY_SIZE;
        int oneByteValueMax = DynamicSizeUtil.MASK_ONE_BYTE_VALUE_SIZE;

        TreeNodeDynamicSize<RawBytes,RawBytes> node = getNode( PAGE_SIZE, layout, offloadIdProvider );

        verifyOverhead( node, oneByteKeyMax, 0, 1 );
        verifyOverhead( node, oneByteKeyMax, 1, 2 );
        verifyOverhead( node, oneByteKeyMax, oneByteValueMax, 2 );
        verifyOverhead( node, oneByteKeyMax, oneByteValueMax +  1, 3 );
        verifyOverhead( node, oneByteKeyMax + 1, 0, 2 );
        verifyOverhead( node, oneByteKeyMax + 1, 1, 3 );
        verifyOverhead( node, oneByteKeyMax + 1, oneByteValueMax, 3 );
        verifyOverhead( node, oneByteKeyMax + 1, oneByteValueMax +  1, 4 );
    }

    @Test
    public void shouldStoreRemainderOfLargeKeyAndWholeValueOnOffload() throws IOException
    {
        // given
        TreeNodeDynamicSize<RawBytes,RawBytes> node = getNode( PAGE_SIZE, layout, offloadIdProvider );
        int keySize = node.getKeyValueSizeCap() * 5;
        int valueSize = Long.BYTES;
        cursor.zapPage();
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // when
        RawBytes key = initializedToRandomBytes( layout.newKey(), keySize );
        RawBytes value = initializedToRandomBytes( layout.newValue(), valueSize );
        node.insertKeyValueAt( cursor, key, value, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION );

        // then
        // read together
        RawBytes readKey = layout.newKey();
        RawBytes readValue = layout.newValue();
        node.keyValueAt( cursor, readKey, readValue, 0 );
        assertArrayEquals( key.bytes, readKey.bytes );
        assertArrayEquals( value.bytes, readValue.bytes );

        // read separately
        node.keyAt( cursor, readKey, 0, LEAF );
        node.valueAt( cursor, readValue, 0 );
        assertArrayEquals( key.bytes, readKey.bytes );
        assertArrayEquals( value.bytes, readValue.bytes );
    }

    private RawBytes initializedToRandomBytes( RawBytes rawBytes, int size )
    {
        rawBytes.bytes = new byte[size];
        random.nextBytes( rawBytes.bytes );
        return rawBytes;
    }

    // TODO shouldStoreWholeValueInOffloadIfAnyOffloadNeededForKeyAndValue
    // TODO shouldLoadFullKeyIfRequested

    private void verifyOverhead( TreeNodeDynamicSize<RawBytes,RawBytes> node, int keySize, int valueSize, int expectedOverhead ) throws IOException
    {
        cursor.zapPage();
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[keySize];
        value.bytes = new byte[valueSize];

        int allocOffsetBefore = node.getAllocOffset( cursor );
        node.insertKeyValueAt( cursor, key, value, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        int allocOffsetAfter = node.getAllocOffset( cursor );
        assertEquals( allocOffsetBefore - keySize - valueSize - expectedOverhead, allocOffsetAfter );
    }
}
