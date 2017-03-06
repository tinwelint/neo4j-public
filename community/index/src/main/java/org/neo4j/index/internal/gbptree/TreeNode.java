/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;
import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.GenSafePointerPair.NO_LOGICAL_POS;
import static org.neo4j.index.internal.gbptree.GenSafePointerPair.read;

/**
 * Methods to manipulate single tree node such as set and get header fields,
 * insert and fetch keys, values and children.
 * <p>
 * DESIGN
 * <p>
 * Using Separate design the internal nodes should look like
 * <pre>
 * # = empty space
 *
 * [                            HEADER   82B                        ]|[      KEYS     ]|[     CHILDREN             ]
 * [NODETYPE][TYPE][GEN][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][NEWGEN]|[[KEY][KEY]...##]|[[CHILD][CHILD][CHILD]...##]
 *  0         1     6    10        34            58
 * </pre>
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 * <p>
 * Calc offset for child i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_INTERNAL + i * SIZE_CHILD
 * <p>
 * Using Separate design the leaf nodes should look like
 *
 * <pre>
 * [                            HEADER   82B                        ]|[      KEYS     ]|[     VALUES        ]
 * [NODETYPE][TYPE][GEN][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][NEWGEN]|[[KEY][KEY]...##]|[[VALUE][VALUE]...##]
 *  0         1     6    10        34            58
 * </pre>
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 * <p>
 * Calc offset for value i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_LEAF + i * SIZE_VALUE
 *
 * @param <KEY> type of key
 * @param <VALUE> type of value
 */
class TreeNode<KEY,VALUE>
{
    // Shared between all node types: TreeNode and FreelistNode
    static final int BYTE_POS_NODE_TYPE = 0;
    static final byte NODE_TYPE_TREE_NODE = 1;
    static final byte NODE_TYPE_FREE_LIST_NODE = 2;

    static final int SIZE_PAGE_REFERENCE = GenSafePointerPair.SIZE;
    static final int BYTE_POS_TYPE = BYTE_POS_NODE_TYPE + Byte.BYTES;
    static final int BYTE_POS_GEN = BYTE_POS_TYPE + Byte.BYTES;
    static final int BYTE_POS_KEYCOUNT = BYTE_POS_GEN + Integer.BYTES;
    static final int BYTE_POS_RIGHTSIBLING = BYTE_POS_KEYCOUNT + Integer.BYTES;
    static final int BYTE_POS_LEFTSIBLING = BYTE_POS_RIGHTSIBLING + SIZE_PAGE_REFERENCE;
    static final int BYTE_POS_NEWGEN = BYTE_POS_LEFTSIBLING + SIZE_PAGE_REFERENCE;
    static final int BYTE_POS_COMPRESSION_LEVEL = BYTE_POS_NEWGEN + SIZE_PAGE_REFERENCE;
    static final int HEADER_LENGTH = BYTE_POS_COMPRESSION_LEVEL + 1;

    private static final byte LEAF_FLAG = 1;
    static final byte INTERNAL_FLAG = 0;
    static final long NO_NODE_FLAG = 0;

    private final int pageSize;
    private final int[] internalMaxKeyCount = new int[10];
    private final int[] leafMaxKeyCount = new int[10];
    private final Layout<KEY,VALUE> layout;

    private final int uncompressedKeySize;
    private final int valueSize;

    TreeNode( int pageSize, Layout<KEY,VALUE> layout )
    {
        this.pageSize = pageSize;
        this.layout = layout;
        this.valueSize = layout.valueSize();
        this.uncompressedKeySize = layout.keySize( Layout.NO_KEY_COMPRESSION );
        for ( byte compressionLevel = Layout.NO_KEY_COMPRESSION; compressionLevel <= layout
                .maxKeyCompressionLevel(); compressionLevel++ )
        {
            int keySize = layout.keySize( compressionLevel );
            internalMaxKeyCount[compressionLevel] = Math.floorDiv( pageSize - (HEADER_LENGTH + SIZE_PAGE_REFERENCE),
                    keySize + SIZE_PAGE_REFERENCE);
            leafMaxKeyCount[compressionLevel] = Math.floorDiv( pageSize - HEADER_LENGTH, keySize + valueSize );
        }

        if ( internalMaxKeyCount[0] < 2 )
        {
            throw new MetadataMismatchException( "For layout " + layout + " a page size of " + pageSize +
                    " would only fit " + internalMaxKeyCount + " internal keys, minimum is 2" );
        }
        if ( leafMaxKeyCount[0] < 2 )
        {
            throw new MetadataMismatchException( "A page size of " + pageSize + " would only fit " +
                    leafMaxKeyCount + " leaf keys, minimum is 2" );
        }
    }

    static byte nodeType( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_NODE_TYPE );
    }

    private void initialize( PageCursor cursor, byte type, long stableGeneration, long unstableGeneration,
            KEY fromInclusive, KEY toExclusive )
    {
        cursor.putByte( BYTE_POS_NODE_TYPE, NODE_TYPE_TREE_NODE );
        cursor.putByte( BYTE_POS_TYPE, type );
        setGen( cursor, unstableGeneration );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setLeftSibling( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setNewGen( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setCompressionLevel( cursor, layout.keyCompressionLevel( fromInclusive, toExclusive ) );
    }

    private void setCompressionLevel( PageCursor cursor, byte keyCompressionLevel )
    {
        cursor.putByte( BYTE_POS_COMPRESSION_LEVEL, keyCompressionLevel );
    }

    void initializeLeaf( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        initializeLeaf( cursor, stableGeneration, unstableGeneration, null, null );
    }

    void initializeLeaf( PageCursor cursor, long stableGeneration, long unstableGeneration,
            KEY fromInclusive, KEY toExclusive )
    {
        initialize( cursor, LEAF_FLAG, stableGeneration, unstableGeneration, fromInclusive, toExclusive );
    }

    void initializeInternal( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        initializeInternal( cursor, stableGeneration, unstableGeneration, null, null );
    }

    void initializeInternal( PageCursor cursor, long stableGeneration, long unstableGeneration,
            KEY fromInclusive, KEY toExclusive )
    {
        initialize( cursor, INTERNAL_FLAG, stableGeneration, unstableGeneration, fromInclusive, toExclusive );
    }

    // HEADER METHODS

    static boolean isLeaf( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == LEAF_FLAG;
    }

    static boolean isInternal( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == INTERNAL_FLAG;
    }

    long gen( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_GEN ) & GenSafePointer.GENERATION_MASK;
    }

    int keyCount( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_KEYCOUNT );
    }

    long rightSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_RIGHTSIBLING );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    long leftSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_LEFTSIBLING );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    long newGen( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_NEWGEN );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    void setGen( PageCursor cursor, long generation )
    {
        GenSafePointer.assertGenerationOnWrite( generation );
        cursor.putInt( BYTE_POS_GEN, (int) generation );
    }

    void setKeyCount( PageCursor cursor, int count )
    {
        cursor.putInt( BYTE_POS_KEYCOUNT, count );
    }

    void setRightSibling( PageCursor cursor, long rightSiblingId, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_RIGHTSIBLING );
        long result = GenSafePointerPair.write( cursor, rightSiblingId, stableGeneration, unstableGeneration );
        GenSafePointerPair.assertSuccess( result );
    }

    void setLeftSibling( PageCursor cursor, long leftSiblingId, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_LEFTSIBLING );
        long result = GenSafePointerPair.write( cursor, leftSiblingId, stableGeneration, unstableGeneration );
        GenSafePointerPair.assertSuccess( result );
    }

    void setNewGen( PageCursor cursor, long newGenId, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_NEWGEN );
        long result = GenSafePointerPair.write( cursor, newGenId, stableGeneration, unstableGeneration );
        GenSafePointerPair.assertSuccess( result );
    }

    long pointerGen( PageCursor cursor, long readResult )
    {
        if ( !GenSafePointerPair.isRead( readResult ) )
        {
            throw new IllegalArgumentException( "Expected read result, but got " + readResult );
        }
        byte compressionLevel = compressionLevel( cursor );
        int offset = GenSafePointerPair.genOffset( readResult );
        int gsppOffset = GenSafePointerPair.isLogicalPos( readResult )
                ? childOffset( offset, compressionLevel ) : offset;
        int gspOffset = GenSafePointerPair.resultIsFromSlotA( readResult ) ?
                gsppOffset : gsppOffset + GenSafePointer.SIZE;
        cursor.setOffset( gspOffset );
        return GenSafePointer.readGeneration( cursor );
    }

    // BODY METHODS

    KEY keyAt( PageCursor cursor, KEY into, int pos )
    {
        byte compressionLevel = compressionLevel( cursor );
        cursor.setOffset( keyOffset( pos, compressionLevel ) );
        layout.readKey( cursor, into, compressionLevel );
        return into;
    }

    byte compressionLevel( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_COMPRESSION_LEVEL );
    }

    void insertKeyAt( PageCursor cursor, KEY key, int pos, int keyCount )
    {
        byte compressionLevel = compressionLevel( cursor );
        insertKeySlotsAt( cursor, pos, 1, keyCount );
        cursor.setOffset( keyOffset( pos, compressionLevel ) );
        layout.writeKey( cursor, key, compressionLevel );
    }

    void removeKeyAt( PageCursor cursor, int pos, int keyCount )
    {
        byte compressionLevel = compressionLevel( cursor );
        removeSlotAt( cursor, pos, keyCount, keyOffset( 0, compressionLevel ), keySize( compressionLevel ) );
    }

    private void removeSlotAt( PageCursor cursor, int pos, int itemCount, int baseOffset, int itemSize )
    {
        for ( int posToMoveLeft = pos + 1, offset = baseOffset + posToMoveLeft * itemSize;
                posToMoveLeft < itemCount; posToMoveLeft++, offset += itemSize )
        {
            cursor.copyTo( offset, cursor, offset - itemSize, itemSize );
        }
    }

    void setKeyAt( PageCursor cursor, KEY key, int pos )
    {
        byte compressionLevel = compressionLevel( cursor );
        cursor.setOffset( keyOffset( pos, compressionLevel ) );
        layout.writeKey( cursor, key, compressionLevel );
    }

    VALUE valueAt( PageCursor cursor, VALUE value, int pos )
    {
        byte compressionLevel = compressionLevel( cursor );
        cursor.setOffset( valueOffset( pos, compressionLevel ) );
        layout.readValue( cursor, value );
        return value;
    }

    void insertValueAt( PageCursor cursor, VALUE value, int pos, int keyCount )
    {
        insertValueSlotsAt( cursor, pos, 1, keyCount );
        setValueAt( cursor, value, pos );
    }

    void removeValueAt( PageCursor cursor, int pos, int keyCount )
    {
        byte compressionLevel = compressionLevel( cursor );
        removeSlotAt( cursor, pos, keyCount, valueOffset( 0, compressionLevel ), valueSize );
    }

    void setValueAt( PageCursor cursor, VALUE value, int pos )
    {
        byte compressionLevel = compressionLevel( cursor );
        cursor.setOffset( valueOffset( pos, compressionLevel ) );
        layout.writeValue( cursor, value );
    }

    long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
    {
        byte compressionLevel = compressionLevel( cursor );
        cursor.setOffset( childOffset( pos, compressionLevel ) );
        return read( cursor, stableGeneration, unstableGeneration, pos );
    }

    void insertChildAt( PageCursor cursor, long child, int pos, int keyCount,
            long stableGeneration, long unstableGeneration )
    {
        insertChildSlotsAt( cursor, pos, 1, keyCount );
        setChildAt( cursor, child, pos, stableGeneration, unstableGeneration );
    }

    void removeChildAt( PageCursor cursor, int pos, int keyCount )
    {
        byte compressionLevel = compressionLevel( cursor );
        removeSlotAt( cursor, pos, keyCount + 1, childOffset( 0, compressionLevel ), childSize() );
    }

    void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration )
    {
        byte compressionLevel = compressionLevel( cursor );
        cursor.setOffset( childOffset( pos, compressionLevel ) );
        writeChild( cursor, child, stableGeneration, unstableGeneration );
    }

    void writeChild( PageCursor cursor, long child, long stableGeneration, long unstableGeneration)
    {
        GenSafePointerPair.write( cursor, child, stableGeneration, unstableGeneration );
    }

    /**
     * Moves items (key/value/child) one step to the right, which means rewriting all items of the particular type
     * from pos - itemCount.
     * itemCount is keyCount for key and value, but keyCount+1 for children.
     */
    private void insertSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int itemCount, int baseOffset,
            int itemSize )
    {
        for ( int posToMoveRight = itemCount - 1, offset = baseOffset + posToMoveRight * itemSize;
              posToMoveRight >= pos; posToMoveRight--, offset -= itemSize )
        {
            cursor.copyTo( offset, cursor, offset + itemSize * numberOfSlots, itemSize );
        }
    }

    void insertKeySlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        byte compressionLevel = compressionLevel( cursor );
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount, keyOffset( 0, compressionLevel ),
                keySize( compressionLevel ) );
    }

    void insertValueSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        byte compressionLevel = compressionLevel( cursor );
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount, valueOffset( 0, compressionLevel ), valueSize );
    }

    void insertChildSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        byte compressionLevel = compressionLevel( cursor );
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount + 1, childOffset( 0, compressionLevel ), childSize() );
    }

    int internalMaxKeyCount()
    {
        return internalMaxKeyCount[Layout.NO_KEY_COMPRESSION];
    }

    int internalMaxKeyCount( PageCursor cursor )
    {
        byte compressionLevel = compressionLevel( cursor );
        return internalMaxKeyCount( compressionLevel );
    }

    int internalMaxKeyCount( byte compressionLevel )
    {
        return compressionLevel < 0 || compressionLevel >= internalMaxKeyCount.length
                ? internalMaxKeyCount() : internalMaxKeyCount[compressionLevel];
    }

    int leafMaxKeyCount()
    {
        return leafMaxKeyCount[Layout.NO_KEY_COMPRESSION];
    }

    int leafMaxKeyCount( PageCursor cursor )
    {
        byte compressionLevel = compressionLevel( cursor );
        return leafMaxKeyCount( compressionLevel );
    }

    int leafMaxKeyCount( byte compressionLevel )
    {
        return compressionLevel < 0 || compressionLevel >= leafMaxKeyCount.length
                ? leafMaxKeyCount() : leafMaxKeyCount[compressionLevel];
    }

    // HELPERS

    int keyOffset( int pos, byte compressionLevel )
    {
        return HEADER_LENGTH + pos * keySize( compressionLevel );
    }

    int valueOffset( int pos, byte compressionLevel )
    {
        return HEADER_LENGTH + leafMaxKeyCount( compressionLevel ) * keySize( compressionLevel ) + pos * valueSize;
    }

    int childOffset( int pos, byte compressionLevel )
    {
        return HEADER_LENGTH + internalMaxKeyCount( compressionLevel ) * keySize( compressionLevel ) +
                pos * SIZE_PAGE_REFERENCE;
    }

    static boolean isNode( long node )
    {
        return GenSafePointerPair.pointer( node ) != NO_NODE_FLAG;
    }

    int keySize( byte compressionLevel )
    {
        return layout.keySize( compressionLevel );
    }

    int valueSize()
    {
        return valueSize;
    }

    int childSize()
    {
        return SIZE_PAGE_REFERENCE;
    }

    Comparator<KEY> keyComparator()
    {
        return layout;
    }

    void goTo( PageCursor cursor, String messageOnError, long nodeId )
            throws IOException
    {
        PageCursorUtil.goTo( cursor, messageOnError, GenSafePointerPair.pointer( nodeId ) );
    }

    @Override
    public String toString()
    {
        return "TreeNode[pageSize:" + pageSize + ", internalMax:" + internalMaxKeyCount +
                ", leafMax:" + leafMaxKeyCount + ", keySize:" + uncompressedKeySize + ", valueSize:" + valueSize + "]";
    }
}
