package org.neo4j.index.internal.gbptree;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.PageCursorUtil.get6BLong;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.getUnsignedInt;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.put6BLong;

class OffloadFreelistNode
{
    private static final int SIZE_PAGE_ID = GenerationSafePointer.POINTER_SIZE;
    private static final int SIZE_BIT_SET = Byte.BYTES;

    private static final int BYTE_POS_NEXT = TreeNode.BYTE_POS_PAGE_TYPE + TreeNode.SIZE_PAGE_TYPE;
    private static final int SIZE_HEADER = BYTE_POS_NEXT + SIZE_PAGE_ID;
    private static final int SIZE_ENTRY = GenerationSafePointer.GENERATION_SIZE + SIZE_PAGE_ID + SIZE_BIT_SET;
    static final long NO_RECORD = 0;
    private static final int SHIFT_BIT_SET = Long.SIZE - Byte.SIZE;
    private static final long MASK_PAGE_ID = (1L << SHIFT_BIT_SET) - 1;

    private final int maxEntries;

    OffloadFreelistNode( int pageSize )
    {
        this.maxEntries = (pageSize - SIZE_HEADER) / SIZE_ENTRY;
    }

    static void initialize( PageCursor cursor )
    {
        cursor.putByte( TreeNode.BYTE_POS_PAGE_TYPE, TreeNode.PAGE_TYPE_FREE_LIST );
    }

    void write( PageCursor cursor, long unstableGeneration, long pageId, byte bitSet, int pos )
    {
        assert pageId >= IdSpace.MIN_TREE_NODE_ID : "Tried to write pageId " + pageId + " which is a reserved page";

        assertPos( pos );
        GenerationSafePointer.assertGenerationOnWrite( unstableGeneration );
        cursor.setOffset( entryOffset( pos ) );
        cursor.putInt( (int) unstableGeneration );
        put6BLong( cursor, pageId );
        cursor.putByte( bitSet );
    }

    long read( PageCursor cursor, long stableGeneration, int pos )
    {
        assertPos( pos );
        cursor.setOffset( entryOffset( pos ) );
        long generation = getUnsignedInt( cursor );
        if ( generation <= stableGeneration )
        {
            long pageId = get6BLong( cursor );
            long bitSet = cursor.getByte();
            return bitSet << SHIFT_BIT_SET | pageId;
        }
        return NO_RECORD;
    }

    static long extractPageId( long readResult )
    {
        return readResult & MASK_PAGE_ID;
    }

    static byte extractBitSet( long readResult )
    {
        return (byte) (readResult >>> SHIFT_BIT_SET);
    }

    int maxEntries()
    {
        return maxEntries;
    }

    static void setNext( PageCursor cursor, long nextFreelistPage )
    {
        cursor.setOffset( BYTE_POS_NEXT );
        put6BLong( cursor, nextFreelistPage );
    }

    static long next( PageCursor cursor )
    {
        cursor.setOffset( BYTE_POS_NEXT );
        return get6BLong( cursor );
    }

    private void assertPos( int pos )
    {
        if ( pos >= maxEntries )
        {
            throw new IllegalArgumentException( "Pos " + pos + " too big, max entries " + maxEntries );
        }
        if ( pos < 0 )
        {
            throw new IllegalArgumentException( "Negative pos " + pos );
        }
    }

    private static int entryOffset( int pos )
    {
        return SIZE_HEADER + pos * SIZE_ENTRY;
    }
}
