/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Integer.min;

import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.NO_LOGICAL_POS;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;

/**
 * Works much like {@link TreeNodeSimple}, but with the addition of a delta section in the end of each leaf node.
 * The delta section isn't fixed sized, but instead is written from the end and backwards such that each leaf node
 * can still contain as many entries, it's just that some entries exist in the delta section instead.
 *
 * The main idea behind the delta section is to improve the bad/worst-case scenario of inserting an entry far to the "left"
 * where all the entries to its "right" will have to move one step. This is costly to do for every insert. Having a delta
 * section allows to queue up multiple such inserts and instead do a joint merge into the main section on full delta section
 * or before structural changes. This heavily reduces the cost of inserting in a leaf, regardless of where the insert happens.
 *
 * Currently removals are still done directly on the entries, i.e. the cost is still there for removals.
 *
 * @param <KEY> type of key
 * @param <VALUE> type of value
 */
class TreeNodeDelta<KEY,VALUE> extends TreeNode<KEY,VALUE>
{
    static final int SIZE_PAGE_REFERENCE = GenerationSafePointerPair.SIZE;

    private static final AtomicInteger OFFSET = new AtomicInteger( BYTE_POS_NODE_TYPE + Byte.BYTES );
    private static final int BYTE_POS_TYPE = OFFSET.getAndAdd( Byte.BYTES );
    private static final int BYTE_POS_GENERATION = OFFSET.getAndAdd( Integer.BYTES );
    private static final int BYTE_POS_KEYCOUNT = OFFSET.getAndAdd( Short.BYTES );
    private static final int BYTE_POS_DELTA_KEY_COUNT = OFFSET.getAndAdd( Byte.BYTES );
    private static final int BYTE_POS_DELTA_REMOVAL_KEY_COUNT = OFFSET.getAndAdd( Byte.BYTES );
    private static final int BYTE_POS_RIGHTSIBLING = OFFSET.getAndAdd( SIZE_PAGE_REFERENCE );
    private static final int BYTE_POS_LEFTSIBLING = OFFSET.getAndAdd( SIZE_PAGE_REFERENCE );
    private static final int BYTE_POS_SUCCESSOR = OFFSET.getAndAdd( SIZE_PAGE_REFERENCE );

    static final int HEADER_LENGTH = OFFSET.get();

    static final int DELTA_SECTION_SIZE = Integer.SIZE;
    static final byte FORMAT_IDENTIFIER = 3;
    static final byte FORMAT_VERSION = 0;

    private final int pageSize;
    private final int internalMaxKeyCount;
    private final int deltaLeafMaxKeyCount;
    private final int leafMaxKeyCount;
    private final int offsetKeyStart;
    private final int offsetKeyEnd;
    private final int offsetValueStart;
    private final int offsetValueEnd;
    private final int offsetChildStart;
    private final Layout<KEY,VALUE> layout;
    private final Section<KEY,VALUE> mainSection;
    private final Section<KEY,VALUE> deltaSection;

    private final int keySize;
    private final int valueSize;

    TreeNodeDelta( int pageSize, Layout<KEY,VALUE> layout )
    {
        this.pageSize = pageSize;
        this.layout = layout;
        this.keySize = layout.keySize();
        this.valueSize = layout.valueSize();
        this.internalMaxKeyCount = Math.floorDiv( pageSize - (HEADER_LENGTH + SIZE_PAGE_REFERENCE),
                keySize + SIZE_PAGE_REFERENCE);
        this.leafMaxKeyCount = Math.floorDiv( pageSize - HEADER_LENGTH, keySize + valueSize );

        if ( internalMaxKeyCount < 2 )
        {
            throw new MetadataMismatchException(
                    "For layout %s a page size of %d would only fit %d internal keys, minimum is 2",
                    layout, pageSize, internalMaxKeyCount );
        }
        if ( leafMaxKeyCount < 2 )
        {
            throw new MetadataMismatchException( "A page size of %d would only fit leaf keys, minimum is 2",
                    pageSize, leafMaxKeyCount );
        }

        this.deltaLeafMaxKeyCount = min( DELTA_SECTION_SIZE, leafMaxKeyCount / 10 );

        this.mainSection = new MainSection();
        this.deltaSection = new DeltaSection();

        this.offsetKeyStart = HEADER_LENGTH;
        this.offsetKeyEnd = offsetKeyStart + leafMaxKeyCount * keySize;
        this.offsetValueStart = offsetKeyEnd;
        this.offsetValueEnd = offsetValueStart + leafMaxKeyCount * valueSize;
        this.offsetChildStart = HEADER_LENGTH + internalMaxKeyCount * keySize;
    }

    @Override
    byte formatIdentifier()
    {
        return FORMAT_IDENTIFIER;
    }

    @Override
    byte formatVersion()
    {
        return FORMAT_VERSION;
    }

    @Override
    public void initialize( PageCursor cursor, byte type, long stableGeneration, long unstableGeneration )
    {
        cursor.putByte( BYTE_POS_NODE_TYPE, NODE_TYPE_TREE_NODE );
        cursor.putByte( BYTE_POS_TYPE, type );
        setGeneration( cursor, unstableGeneration );
        main().setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setLeftSibling( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
        setSuccessor( cursor, NO_NODE_FLAG, stableGeneration, unstableGeneration );
    }
    @Override
    public void initializeLeaf( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        initialize( cursor, LEAF_FLAG, stableGeneration, unstableGeneration );
    }

    @Override
    public void initializeInternal( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        initialize( cursor, INTERNAL_FLAG, stableGeneration, unstableGeneration );
    }

    // HEADER METHODS

    @Override
    public boolean isLeaf( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == LEAF_FLAG;
    }

    @Override
    public boolean isInternal( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == INTERNAL_FLAG;
    }

    @Override
    public long generation( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_GENERATION ) & GenerationSafePointer.GENERATION_MASK;
    }

    @Override
    public long rightSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_RIGHTSIBLING );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    @Override
    public long leftSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_LEFTSIBLING );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    @Override
    public long successor( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_SUCCESSOR );
        return read( cursor, stableGeneration, unstableGeneration, NO_LOGICAL_POS );
    }

    @Override
    public void setGeneration( PageCursor cursor, long generation )
    {
        GenerationSafePointer.assertGenerationOnWrite( generation );
        cursor.putInt( BYTE_POS_GENERATION, (int) generation );
    }

    @Override
    public void setRightSibling( PageCursor cursor, long rightSiblingId, long stableGeneration,
            long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_RIGHTSIBLING );
        long result = GenerationSafePointerPair.write( cursor, rightSiblingId, stableGeneration, unstableGeneration );
        GenerationSafePointerPair.assertSuccess( result );
    }

    @Override
    public void setLeftSibling( PageCursor cursor, long leftSiblingId, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( BYTE_POS_LEFTSIBLING );
        long result = GenerationSafePointerPair.write( cursor, leftSiblingId, stableGeneration, unstableGeneration );
        GenerationSafePointerPair.assertSuccess( result );
    }
    @Override
    public void setSuccessor( PageCursor cursor, long successorId, long stableGeneration, long unstableGeneration )
    {        cursor.setOffset( BYTE_POS_SUCCESSOR );
        long result = GenerationSafePointerPair.write( cursor, successorId, stableGeneration, unstableGeneration );
        GenerationSafePointerPair.assertSuccess( result );
    }

    @Override
    public long pointerGeneration( PageCursor cursor, long readResult )
    {
        if ( !GenerationSafePointerPair.isRead( readResult ) )
        {
            throw new IllegalArgumentException( "Expected read result, but got " + readResult );
        }
        int offset = GenerationSafePointerPair.generationOffset( readResult );
        int gsppOffset = GenerationSafePointerPair.isLogicalPos( readResult ) ? childOffset( offset ) : offset;
        int gspOffset = GenerationSafePointerPair.resultIsFromSlotA( readResult ) ?
                        gsppOffset : gsppOffset + GenerationSafePointer.SIZE;
        cursor.setOffset( gspOffset );
        return GenerationSafePointer.readGeneration( cursor );
    }

    // BODY METHODS

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

    @Override
    void insertKeySlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount, offsetKeyStart, keySize );
    }

    @Override
    void insertValueSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount, offsetValueStart, valueSize );
    }

    @Override
    void insertChildSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount + 1, offsetChildStart, childSize() );
    }

    // HELPERS

    @Override
    public int keyOffset( int pos )
    {
        return offsetKeyStart + pos * keySize;
    }

    @Override
    public int valueOffset( int pos )
    {
        return offsetValueStart + pos * valueSize;
    }

    @Override
    public int childOffset( int pos )
    {
        return offsetChildStart + pos * SIZE_PAGE_REFERENCE;
    }

    @Override
    public int keySize()
    {
        return keySize;
    }

    @Override
    public int valueSize()
    {
        return valueSize;
    }

    @Override
    public int childSize()
    {
        return SIZE_PAGE_REFERENCE;
    }

    @Override
    public Comparator<KEY> keyComparator()
    {
        return layout;
    }

    @Override
    public void goTo( PageCursor cursor, String messageOnError, long nodeId )
            throws IOException
    {
        PageCursorUtil.goTo( cursor, messageOnError, GenerationSafePointerPair.pointer( nodeId ) );
    }

    @Override
    public String toString()
    {
        return "TreeNode[pageSize:" + pageSize + ", internalMax:" + internalMaxKeyCount +
                ", leafMax:" + leafMaxKeyCount + ", keySize:" + keySize + ", valueSize:" + valueSize + "]";
    }

    @Override
    Section<KEY,VALUE> main()
    {
        return mainSection;
    }

    @Override
    Section<KEY,VALUE> delta()
    {
        return deltaSection;
    }

    @Override
    int leftSiblingOffset()
    {
        return BYTE_POS_LEFTSIBLING;
    }

    @Override
    int rightSiblingOffset()
    {
        return BYTE_POS_RIGHTSIBLING;
    }

    @Override
    int successorOffset()
    {
        return BYTE_POS_SUCCESSOR;
    }

    @Override
    int keyCountOffset()
    {
        return BYTE_POS_KEYCOUNT;
    }

    private class MainSection extends Section<KEY,VALUE>
    {
        MainSection()
        {
            super( Type.MAIN );
        }

        @Override
        public Comparator<KEY> keyComparator()
        {
            return layout;
        }

        @Override
        public int keyCount( PageCursor cursor )
        {
            return cursor.getShort( BYTE_POS_KEYCOUNT ) & 0xFFFF;
        }

        @Override
        public void setKeyCount( PageCursor cursor, int count )
        {
            if ( (count & ~0xFFFF) != 0 )
            {
                throw new IllegalArgumentException( "Count must be non-negative short, but was " + count );
            }
            cursor.putShort( BYTE_POS_KEYCOUNT, (short) count );
        }

        @Override
        public KEY keyAt( PageCursor cursor, KEY into, int pos )
        {
            cursor.setOffset( keyOffset( pos ) );
            layout.readKey( cursor, into );
            return into;
        }

        @Override
        public void insertKeyAt( PageCursor cursor, KEY key, int pos, int keyCount )
        {
            insertKeySlotsAt( cursor, pos, 1, keyCount );
            cursor.setOffset( keyOffset( pos ) );
            layout.writeKey( cursor, key );
        }

        @Override
        public void removeKeyAt( PageCursor cursor, int pos, int keyCount )
        {
            removeSlotAt( cursor, pos, keyCount, keyOffset( 0 ), keySize );
        }

        private void removeSlotAt( PageCursor cursor, int pos, int itemCount, int baseOffset, int itemSize )
        {
            for ( int posToMoveLeft = pos + 1, offset = baseOffset + posToMoveLeft * itemSize;
                    posToMoveLeft < itemCount; posToMoveLeft++, offset += itemSize )
            {
                cursor.copyTo( offset, cursor, offset - itemSize, itemSize );
            }
        }
        @Override
        public void setKeyAt( PageCursor cursor, KEY key, int pos )
        {
            cursor.setOffset( keyOffset( pos ) );
            layout.writeKey( cursor, key );
        }

        @Override
        public VALUE valueAt( PageCursor cursor, VALUE value, int pos )
        {
            cursor.setOffset( valueOffset( pos ) );
            layout.readValue( cursor, value );
            return value;
        }

        @Override
        public void insertValueAt( PageCursor cursor, VALUE value, int pos, int keyCount )
        {
            insertValueSlotsAt( cursor, pos, 1, keyCount );
            setValueAt( cursor, value, pos );
        }

        @Override
        public void removeValueAt( PageCursor cursor, int pos, int keyCount )
        {
            removeSlotAt( cursor, pos, keyCount, valueOffset( 0 ), valueSize );
        }

        @Override
        public void setValueAt( PageCursor cursor, VALUE value, int pos )
        {
            cursor.setOffset( valueOffset( pos ) );
            layout.writeValue( cursor, value );
        }

        @Override
        public long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
        {
            cursor.setOffset( childOffset( pos ) );
            return read( cursor, stableGeneration, unstableGeneration, pos );
        }

        @Override
        public void insertChildAt( PageCursor cursor, long child, int pos, int keyCount,
                long stableGeneration, long unstableGeneration )
        {
            insertChildSlotsAt( cursor, pos, 1, keyCount );
            setChildAt( cursor, child, pos, stableGeneration, unstableGeneration );
        }

        @Override
        public void removeChildAt( PageCursor cursor, int pos, int keyCount )
        {
            removeSlotAt( cursor, pos, keyCount + 1, childOffset( 0 ), childSize() );
        }

        @Override
        public void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration )
        {
            cursor.setOffset( childOffset( pos ) );
            writeChild( cursor, child, stableGeneration, unstableGeneration );
        }

        @Override
        void writeChild( PageCursor cursor, long child, long stableGeneration, long unstableGeneration )
        {
            GenerationSafePointerPair.write( cursor, child, stableGeneration, unstableGeneration );
        }

        @Override
        public int internalMaxKeyCount()
        {
            return internalMaxKeyCount;
        }

        @Override
        public int leafMaxKeyCount()
        {
            return leafMaxKeyCount;
        }

        @Override
        int removalKeyCount( PageCursor cursor )
        {
            return 0;
        }

        @Override
        void setRemovalKeyCount( PageCursor cursor, int keyCount )
        {
            throw new UnsupportedOperationException();
        }
    }

    private class DeltaSection extends Section<KEY,VALUE>
    {
        DeltaSection()
        {
            super( Type.DELTA );
        }

        @Override
        public Comparator<KEY> keyComparator()
        {
            return layout;
        }

        @Override
        public int keyCount( PageCursor cursor )
        {
            return cursor.getByte( BYTE_POS_DELTA_KEY_COUNT ) & 0xFF;
        }

        @Override
        public void setKeyCount( PageCursor cursor, int count )
        {
            if ( (count & ~0xFF) != 0 )
            {
                throw new IllegalArgumentException( "Key count must be non-negative byte, but was " + count );
            }
            cursor.putByte( BYTE_POS_DELTA_KEY_COUNT, (byte) count );
        }

        @Override
        public KEY keyAt( PageCursor cursor, KEY into, int pos )
        {
            cursor.setOffset( keyOffset( pos ) );
            layout.readKey( cursor, into );
            return into;
        }

        @Override
        public void insertKeyAt( PageCursor cursor, KEY key, int pos, int keyCount )
        {
            deltaInsertSlotsAt( cursor, pos, keyCount, offsetKeyEnd, keySize );
            cursor.setOffset( keyOffset( pos ) );
            layout.writeKey( cursor, key );
        }

        @Override
        public void removeKeyAt( PageCursor cursor, int pos, int keyCount )
        {
            deltaRemoveSlotAt( cursor, pos, keyCount, offsetKeyEnd, keySize );
        }

        @Override
        public void setKeyAt( PageCursor cursor, KEY key, int pos )
        {
            cursor.setOffset( keyOffset( pos ) );
            layout.writeKey( cursor, key );
        }

        @Override
        public VALUE valueAt( PageCursor cursor, VALUE value, int pos )
        {
            cursor.setOffset( valueOffset( pos ) );
            layout.readValue( cursor, value );
            return value;
        }

        @Override
        public void insertValueAt( PageCursor cursor, VALUE value, int pos, int keyCount )
        {
            deltaInsertSlotsAt( cursor, pos, keyCount, offsetValueEnd, valueSize );
            setValueAt( cursor, value, pos );
        }

        @Override
        public void removeValueAt( PageCursor cursor, int pos, int keyCount )
        {
            deltaRemoveSlotAt( cursor, pos, keyCount, offsetValueEnd, valueSize );
        }

        @Override
        public void setValueAt( PageCursor cursor, VALUE value, int pos )
        {
            cursor.setOffset( valueOffset( pos ) );
            layout.writeValue( cursor, value );
        }

        @Override
        public long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
        {
            throw new UnsupportedOperationException( "Not supported a.t.m." );
        }

        @Override
        public void insertChildAt( PageCursor cursor, long child, int pos, int keyCount,
                long stableGeneration, long unstableGeneration )
        {
            throw new UnsupportedOperationException( "Not supported a.t.m." );
        }

        @Override
        public void removeChildAt( PageCursor cursor, int pos, int keyCount )
        {
            throw new UnsupportedOperationException( "Not supported a.t.m." );
        }

        @Override
        public void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration )
        {
            throw new UnsupportedOperationException( "Not supported a.t.m." );
        }

        @Override
        void writeChild( PageCursor cursor, long child, long stableGeneration, long unstableGeneration )
        {
            throw new UnsupportedOperationException( "Not supported a.t.m." );
        }

        @Override
        public int internalMaxKeyCount()
        {
            return 0;
        }

        @Override
        public int leafMaxKeyCount()
        {
            return deltaLeafMaxKeyCount;
        }

        @Override
        int removalKeyCount( PageCursor cursor )
        {
            return cursor.getByte( BYTE_POS_DELTA_REMOVAL_KEY_COUNT ) & 0xFF;
        }

        @Override
        void setRemovalKeyCount( PageCursor cursor, int count )
        {
            if ( (count & ~0xFF) != 0 )
            {
                throw new IllegalArgumentException( "Key count must fit in byte" );
            }
            cursor.putByte( BYTE_POS_DELTA_REMOVAL_KEY_COUNT, (byte) count );
        }

        // Delta keys start from the end and go backwards
        private int keyOffset( int pos )
        {
            return offsetKeyEnd - keySize - pos * keySize;
        }

        // Delta values start from the end and go backwards
        private int valueOffset( int pos )
        {
            return offsetValueEnd - valueSize - pos * valueSize;
        }

        // ,,,,,,,,,,|,,,G,D,A
        // POS           2 1 0
        private void deltaInsertSlotsAt( PageCursor cursor, int pos, int itemCount, int baseOffset,
                int itemSize )
        {
            // TODO optimize
            int itemsToMove = itemCount - pos;
            for ( int i = 0; i < itemsToMove; i++ )
            {
                int sourceOffset = baseOffset - (itemCount - i) * itemSize;
                cursor.copyTo( sourceOffset, cursor, sourceOffset - itemSize, itemSize );
            }
        }

        // ,,,,,,,,,,|,,L,G,D,A
        // POS          3 2 1 0
        private void deltaRemoveSlotAt( PageCursor cursor, int pos, int itemCount, int baseOffset, int itemSize )
        {
            // TODO optimize
            int itemsToMove = itemCount - pos - 1;
            for ( int i = 0; i < itemsToMove; i++ )
            {
                int sourceOffset = baseOffset - (pos + i + 2) * itemSize;
                cursor.copyTo( sourceOffset, cursor, sourceOffset + itemSize, itemSize );
            }
        }
    }
}
