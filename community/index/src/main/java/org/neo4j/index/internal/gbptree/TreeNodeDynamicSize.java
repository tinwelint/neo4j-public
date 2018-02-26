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

import java.io.IOException;
import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.collection.primitive.PrimitiveIntStack;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Integer.min;
import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.SIZE_KEY_SIZE;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.SIZE_OFFSET;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.SIZE_TOTAL_OVERHEAD;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.SIZE_VALUE_SIZE;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractTombstone;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getEntryOverhead;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyChildHeader;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyOffset;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyValueHeader;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putTombstone;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyOffset;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyValueSize;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.putUnsignedShort;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;

/**
 * # = empty space
 * K* = offset to key or key and value
 *
 * LEAF
 * [                                   HEADER   86B                                                   ]|[KEY_OFFSETS]##########[KEYS_VALUES]
 * [NODETYPE][TYPE][GENERATION][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][SUCCESSOR][ALLOCOFFSET][DEADSPACE]|[K0*,K1*,K2*]->      <-[KV0,KV2,KV1]
 *  0         1     2           6         10            34           58         82           84          86
 *
 *  INTERNAL
 * [                                   HEADER   86B                                                   ]|[  KEY_OFFSET_CHILDREN  ]######[  KEYS  ]
 * [NODETYPE][TYPE][GENERATION][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][SUCCESSOR][ALLOCOFFSET][DEADSPACE]|[C0,K0*,C1,K1*,C2,K2*,C3]->  <-[K2,K0,K1]
 *  0         1     2           6         10            34           58         82           84          86
 *
 * See {@link DynamicSizeUtil} for more detailed layout for individual offset array entries and key / key_value entries.
 */
public class TreeNodeDynamicSize<KEY, VALUE> extends TreeNode<KEY,VALUE>
{
    static final byte FORMAT_IDENTIFIER = 3;
    static final byte FORMAT_VERSION = 0;

    /**
     * Concepts
     * Total space - The space available for data (pageSize - headerSize)
     * Active space - Space currently occupied by active data (not including dead keys)
     * Dead space - Space currently occupied by dead data that could be reclaimed by defragment
     * Alloc offset - Exact offset to leftmost key and thus the end of alloc space
     * Alloc space - The available space between offset array and data space
     *
     * TotalSpace  |----------------------------------------|
     * ActiveSpace |-----------|   +    |---------|  + |----|
     * DeadSpace                                  |----|
     * AllocSpace              |--------|
     * AllocOffset                      v
     *     [Header][OffsetArray]........[_________,XXXX,____] (_ = alive key, X = dead key)
     */
    private static final int BYTE_POS_ALLOCOFFSET = BASE_HEADER_LENGTH;
    private static final int BYTE_POS_DEADSPACE = BYTE_POS_ALLOCOFFSET + bytesPageOffset();
    private static final int HEADER_LENGTH_DYNAMIC = BYTE_POS_DEADSPACE + bytesPageOffset();

    private static final int LEAST_NUMBER_OF_ENTRIES_PER_PAGE = 2;
    private static final int MINIMUM_ENTRY_SIZE_CAP = Long.SIZE;
    private final int keyValueSizeCap;
    private final PrimitiveIntStack deadKeysOffset = new PrimitiveIntStack();
    private final PrimitiveIntStack aliveKeysOffset = new PrimitiveIntStack();
    private final int maxKeyCount = pageSize / (bytesKeyOffset() + SIZE_KEY_SIZE + SIZE_VALUE_SIZE);
    private final int[] oldOffset = new int[maxKeyCount];
    private final int[] newOffset = new int[maxKeyCount];
    private final int totalSpace;
    private final OffloadIdProvider offloadIdProvider;
    private final int halfSpace;

    TreeNodeDynamicSize( int pageSize, Layout<KEY,VALUE> layout, OffloadIdProvider offloadIdProvider )
    {
        super( pageSize, layout );
        totalSpace = pageSize - HEADER_LENGTH_DYNAMIC;
        this.offloadIdProvider = offloadIdProvider;
        halfSpace = totalSpace / 2;
        // TODO update this calculation, it should include:
        // - key/value header (potentially including offload reference)
        // - key bytes
        // - value bytes
        keyValueSizeCap = totalSpace / LEAST_NUMBER_OF_ENTRIES_PER_PAGE - SIZE_TOTAL_OVERHEAD;

        if ( keyValueSizeCap < MINIMUM_ENTRY_SIZE_CAP )
        {
            throw new MetadataMismatchException(
                    "We need to fit at least %d key-value entries per page in leaves. To do that a key-value entry can be at most %dB " +
                            "with current page size of %dB. We require this cap to be at least %dB.",
                    LEAST_NUMBER_OF_ENTRIES_PER_PAGE, keyValueSizeCap, pageSize, Long.SIZE );
        }
    }

    // Accessible for testing
    int getKeyValueSizeCap()
    {
        return keyValueSizeCap;
    }

    @Override
    void writeAdditionalHeader( PageCursor cursor )
    {
        setAllocOffset( cursor, pageSize );
        setDeadSpace( cursor, 0 );
    }

    @Override
    KEY keyAt( PageCursor cursor, KEY into, int pos, Type type )
    {
        placeCursorAtActualKey( cursor, pos, type );
        int offset = cursor.getOffset();

        // Read key
        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        if ( keySize > keyValueSizeCap || keySize < 0 )
        {
            cursor.setCursorException( format( "Read unreliable key, keySize=%d, keyValueSizeCap=%d, keyHasTombstone=%b, offset=%d, pos=%d",
                    keySize, keyValueSizeCap, extractTombstone( keyValueSize ), offset, pos ) );
            return into;
        }
        layout.readKey( cursor, into, keySize, 0, keySize );
        return into;
    }

    @Override
    void keyValueAt( PageCursor cursor, KEY intoKey, VALUE intoValue, int pos )
    {
        placeCursorAtActualKey( cursor, pos, LEAF );
        int offset = cursor.getOffset();

        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );
        if ( keySize + valueSize > keyValueSizeCap || keySize < 0 || valueSize < 0 )
        {
            cursor.setCursorException( format( "Read unreliable key, keySize=%d, valueSize=%d, keyValueSizeCap=%d, keyHasTombstone=%b, offset=%d, pos=%d",
                    keySize, valueSize, keyValueSizeCap, extractTombstone( keyValueSize ), offset, pos ) );
            return;
        }
        layout.readKey( cursor, intoKey, keySize, 0, keySize );
        layout.readValue( cursor, intoValue, valueSize, 0, valueSize );
    }

    @Override
    void insertKeyAndRightChildAt( PageCursor cursor, KEY key, long child, int pos, int keyCount, long stableGeneration,
            long unstableGeneration )
    {
        // Where to write key?
        int currentKeyOffset = getAllocOffset( cursor );
        int keySize = layout.keySize( key );
        int newKeyOffset = currentKeyOffset - keySize - getEntryOverhead( keySize, 0, keyValueSizeCap );

        // Write key
        cursor.setOffset( newKeyOffset );
        putKeyChildHeader( cursor, keySize );
        layout.writeKey( cursor, key, 0, keySize );

        // Update alloc space
        setAllocOffset( cursor, newKeyOffset );

        // Write to offset array
        insertSlotsAt( cursor, pos, 1, keyCount, keyPosOffsetInternal( 0 ), keyChildSize() );
        cursor.setOffset( keyPosOffsetInternal( pos ) );
        putKeyOffset( cursor, newKeyOffset );
        writeChild( cursor, child, stableGeneration, unstableGeneration );
    }

    @Override
    void insertKeyValueAt( PageCursor cursor, KEY key, VALUE value, int pos, int keyCount, long stableGeneration, long unstableGeneration ) throws IOException
    {
        // Where to write key?
        int currentKeyValueOffset = getAllocOffset( cursor );
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        int overheadSize = getEntryOverhead( keySize, valueSize, keyValueSizeCap );
        long combinedSpaceForKeyValue = totalSpaceOfKeyValue( keySize, valueSize, overheadSize );

        // Write key and value header
        int treeNodeKeySize = extractTreeNodeKeySizeOfTotalSpace( combinedSpaceForKeyValue );
        int treeNodeValueSize = extractTreeNodeValueSizeOfTotalSpace( combinedSpaceForKeyValue );
        int treeNodeEntryTotalSize = treeNodeKeySize + treeNodeValueSize + overheadSize;
        int offloadSize = extractOffloadSizeOfTotalSpace( combinedSpaceForKeyValue );
        long offloadRecordReference = NO_NODE_FLAG;
        if ( offloadSize > 0 )
        {
            // Some of the bytes needs to go to offload storage, allocate a record ID for that
            offloadRecordReference = offloadIdProvider.allocate( stableGeneration, unstableGeneration, offloadSize );
        }
        int newKeyValueOffset = currentKeyValueOffset - treeNodeEntryTotalSize;
        cursor.setOffset( newKeyValueOffset );
        putKeyValueHeader( cursor, treeNodeKeySize, treeNodeValueSize, offloadSize, offloadRecordReference );

        // Write key and value data
        if ( offloadSize == 0 )
        {
            // Everything fits in tree-node
            layout.writeKey( cursor, key, 0, keySize );
            layout.writeValue( cursor, value, 0, valueSize );
        }
        else
        {
            // Some data in tree-node, the rest in offload storage
            long currentTreeNodeId = cursor.getCurrentPageId();

            // Write key. Regardless of the size of the key, the value will be written to offload storage
            layout.writeKey( cursor, key, 0, treeNodeKeySize );
            int offloadKeySize = keySize - treeNodeKeySize;
            int keyOffset = treeNodeKeySize;
            int bytesLeftOnCurrentOffloadRecord = 0;
            int maxDataInOffloadRecord = offloadIdProvider.maxDataInRecord();
            while ( offloadKeySize > 0 )
            {
                offloadRecordReference = offloadIdProvider.placeAt( cursor, offloadRecordReference );
                int length = min( offloadKeySize, maxDataInOffloadRecord );
                layout.writeKey( cursor, key, keyOffset, length );
                offloadKeySize -= length;
                bytesLeftOnCurrentOffloadRecord = maxDataInOffloadRecord - length;
            }

            // Write value
            int offloadValueSize = valueSize;
            int valueOffset = 0;
            while ( offloadValueSize > 0 )
            {
                if ( bytesLeftOnCurrentOffloadRecord == 0 )
                {
                    offloadRecordReference = offloadIdProvider.placeAt( cursor, offloadRecordReference );
                    bytesLeftOnCurrentOffloadRecord = 0;
                }

                int length = min( bytesLeftOnCurrentOffloadRecord, min( offloadValueSize, maxDataInOffloadRecord ) );
                layout.writeValue( cursor, value, length, valueOffset );
                offloadValueSize -= length;
            }

            goTo( cursor, "Back to tree node from offload", currentTreeNodeId );
        }

        // Update alloc space
        setAllocOffset( cursor, newKeyValueOffset );

        // Write to offset array
        insertSlotsAt( cursor, pos, 1, keyCount, keyPosOffsetLeaf( 0 ), bytesKeyOffset() );
        cursor.setOffset( keyPosOffsetLeaf( pos ) );
        putKeyOffset( cursor, newKeyValueOffset );
    }

    @Override
    void removeKeyValueAt( PageCursor cursor, int pos, int keyCount )
    {
        // Kill actual key
        placeCursorAtActualKey( cursor, pos, LEAF );
        int keyOffset = cursor.getOffset();
        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );
        cursor.setOffset( keyOffset );
        putTombstone( cursor );

        // Update dead space
        int deadSpace = getDeadSpace( cursor );
        setDeadSpace( cursor, deadSpace + keySize + valueSize + getEntryOverhead( keySize, valueSize, keyValueSizeCap ) );

        // Remove from offset array
        removeSlotAt( cursor, pos, keyCount, keyPosOffsetLeaf( 0 ), bytesKeyOffset() );
    }

    @Override
    void removeKeyAndRightChildAt( PageCursor cursor, int keyPos, int keyCount )
    {
        // Kill actual key
        placeCursorAtActualKey( cursor, keyPos, INTERNAL );
        int keyOffset = cursor.getOffset();
        int keySize = extractKeySize( readKeyValueSize( cursor ) );
        cursor.setOffset( keyOffset );
        putTombstone( cursor );

        // Update dead space
        int deadSpace = getDeadSpace( cursor );
        setDeadSpace( cursor, deadSpace + keySize + getEntryOverhead( keySize, 0, keyValueSizeCap ) );

        // Remove for offsetArray
        removeSlotAt( cursor, keyPos, keyCount, keyPosOffsetInternal( 0 ), keyChildSize() );

        // Zero pad empty area
        zeroPad( cursor, keyPosOffsetInternal( keyCount - 1 ), bytesKeyOffset() + childSize() );
    }

    @Override
    void removeKeyAndLeftChildAt( PageCursor cursor, int keyPos, int keyCount )
    {
        // Kill actual key
        placeCursorAtActualKey( cursor, keyPos, INTERNAL );
        int keyOffset = cursor.getOffset();
        int keySize = extractKeySize( readKeyValueSize( cursor ) );
        cursor.setOffset( keyOffset );
        putTombstone( cursor );

        // Update dead space
        int deadSpace = getDeadSpace( cursor );
        setDeadSpace( cursor, deadSpace + keySize + getEntryOverhead( keySize, 0, keyValueSizeCap ) );

        // Remove for offsetArray
        removeSlotAt( cursor, keyPos, keyCount, keyPosOffsetInternal( 0 ) - childSize(), keyChildSize() );

        // Move last child
        cursor.copyTo( childOffset( keyCount ), cursor, childOffset( keyCount - 1 ), childSize() );

        // Zero pad empty area
        zeroPad( cursor, keyPosOffsetInternal( keyCount - 1 ), bytesKeyOffset() + childSize() );
    }

    @Override
    boolean setKeyAtInternal( PageCursor cursor, KEY key, int pos )
    {
        placeCursorAtActualKey( cursor, pos, INTERNAL );

        int oldKeySize = extractKeySize( readKeyValueSize( cursor ) );
        if ( oldKeySize > keyValueSizeCap )
        {
            cursor.setCursorException( format( "Read unreliable key size greater than cap: keySize=%d, keyValueSizeCap=%d",
                    oldKeySize, keyValueSizeCap ) );
        }
        int newKeySize = layout.keySize( key );
        if ( newKeySize == oldKeySize )
        {
            // Fine, we can just overwrite
            layout.writeKey( cursor, key, 0, newKeySize );
            return true;
        }
        return false;
    }

    @Override
    VALUE valueAt( PageCursor cursor, VALUE into, int pos )
    {
        // TODO ideally we'd like to avoid a naked call to valueAt because of the overhead it brings for large entries, i.e. entries
        //      that have offload storage. At the time of writing this the remaining calls are (1) overwrite and (2) remove.
        //      Those call sites could be changed, together with TreeNode perhaps, to instead get key and value at the same time,
        //      or to allow for state carried over from keyAt to valueAt, or some such. Consider this as an optimization reminder.

        placeCursorAtActualKey( cursor, pos, LEAF );

        // Read value
        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );
        if ( keySize + valueSize > keyValueSizeCap || keySize < 0 || valueSize < 0 )
        {
            cursor.setCursorException(
                    format( "Read unreliable key, value size greater than cap: keySize=%d, valueSize=%d, keyValueSizeCap=%d",
                            keySize, valueSize, keyValueSizeCap ) );
            return into;
        }
        progressCursor( cursor, keySize );
        layout.readValue( cursor, into, valueSize, 0, valueSize );
        return into;
    }

    @Override
    boolean setValueAt( PageCursor cursor, VALUE value, int pos )
    {
        placeCursorAtActualKey( cursor, pos, LEAF );

        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int oldValueSize = extractValueSize( keyValueSize );
        int newValueSize = layout.valueSize( value );
        if ( oldValueSize == newValueSize )
        {
            // Fine we can just overwrite
            progressCursor( cursor, keySize );
            layout.writeValue( cursor, value, 0, newValueSize );
            return true;
        }
        return false;
    }

    private void progressCursor( PageCursor cursor, int delta )
    {
        cursor.setOffset( cursor.getOffset() + delta );
    }

    @Override
    long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( childOffset( pos ) );
        return read( cursor, stableGeneration, unstableGeneration, pos );
    }

    @Override
    void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( childOffset( pos ) );
        writeChild( cursor, child, stableGeneration, unstableGeneration );
    }

    @Override
    boolean reasonableKeyCount( int keyCount )
    {
        return keyCount >= 0 && keyCount <= totalSpace / SIZE_TOTAL_OVERHEAD;
    }

    @Override
    boolean reasonableChildCount( int childCount )
    {
        return reasonableKeyCount( childCount );
    }

    @Override
    int childOffset( int pos )
    {
        // Child pointer to the left of key at pos
        return keyPosOffsetInternal( pos ) - childSize();
    }

    @Override
    Overflow internalOverflow( PageCursor cursor, int currentKeyCount, KEY newKey )
    {
        // How much space do we have?
        int allocSpace = getAllocSpace( cursor, currentKeyCount, INTERNAL );
        int deadSpace = getDeadSpace( cursor );

        // How much space do we need?
        int neededSpace = totalSpaceOfKeyChild( newKey );

        // There is your answer!
        return neededSpace < allocSpace ? Overflow.NO :
               neededSpace < allocSpace + deadSpace ? Overflow.NO_NEED_DEFRAG : Overflow.YES;
    }

    @Override
    Overflow leafOverflow( PageCursor cursor, int currentKeyCount, KEY newKey, VALUE newValue )
    {
        // How much space do we have?
        int deadSpace = getDeadSpace( cursor );
        int allocSpace = getAllocSpace( cursor, currentKeyCount, LEAF );

        // How much space do we need?
        long combinedNeededSpace = totalSpaceOfKeyValue( newKey, newValue );
        int neededSpace = extractTreeNodeSizeOfTotalSpace( combinedNeededSpace );
        assert extractOffloadSizeOfTotalSpace( combinedNeededSpace ) == 0 : "Implement support for offload storage";

        // There is your answer!
        return neededSpace < allocSpace ? Overflow.NO :
               neededSpace < allocSpace + deadSpace ? Overflow.NO_NEED_DEFRAG : Overflow.YES;
    }

    @Override
    void defragmentLeaf( PageCursor cursor )
    {
        doDefragment( cursor, LEAF );
    }

    @Override
    void defragmentInternal( PageCursor cursor )
    {
        doDefragment( cursor, INTERNAL );
    }

    private void doDefragment( PageCursor cursor, Type type )
    {
        /*
        The goal is to compact all alive keys in the node
        by reusing the space occupied by dead keys.

        BEFORE
        [8][X][1][3][X][2][X][7][5]

        AFTER
        .........[8][1][3][2][7][5]
            ^ Reclaimed space

        It works like this:
        Work from right to left.
        For each dead space of size X (can be multiple consecutive dead keys)
        Move all neighbouring alive keys to the left of that dead space X bytes to the right.
        Can only move in blocks of size X at the time.

        Step by step:
        [8][X][1][3][X][2][X][7][5]
        [8][X][1][3][X][X][2][7][5]
        [8][X][X][X][1][3][2][7][5]
        [X][X][X][8][1][3][2][7][5]

        Here is how the offsets work
        BEFORE MOVE
                          v       aliveRangeOffset
        [X][_][_][X][_][X][_][_]
                   ^   ^          deadRangeOffset
                   |_____________ moveRangeOffset

        AFTER MOVE
                       v          aliveRangeOffset
        [X][_][_][X][X][_][_][_]
                 ^                 deadRangeOffset
        */

        // Mark all offsets
        deadKeysOffset.clear();
        aliveKeysOffset.clear();
        if ( type == INTERNAL )
        {
            recordDeadAndAliveInternal( cursor, deadKeysOffset, aliveKeysOffset );
        }
        else
        {
            recordDeadAndAliveLeaf( cursor, deadKeysOffset, aliveKeysOffset );
        }

        // Cursors into field byte arrays
        int oldOffsetCursor = 0;
        int newOffsetCursor = 0;

        int aliveRangeOffset = pageSize; // Everything after this point is alive
        int deadRangeOffset; // Everything between this point and aliveRangeOffset is dead space

        // Rightmost alive keys does not need to move
        while ( deadKeysOffset.peek() < aliveKeysOffset.peek() )
        {
            aliveRangeOffset = aliveKeysOffset.poll();
        }

        do
        {
            // Locate next range of dead keys
            deadRangeOffset = aliveRangeOffset;
            while ( aliveKeysOffset.peek() < deadKeysOffset.peek() )
            {
                deadRangeOffset = deadKeysOffset.poll();
            }

            // Locate next range of alive keys
            int moveOffset = deadRangeOffset;
            while ( deadKeysOffset.peek() < aliveKeysOffset.peek() )
            {
                int moveKey = aliveKeysOffset.poll();
                oldOffset[oldOffsetCursor++] = moveKey;
                moveOffset = moveKey;
            }

            // Update offset mapping
            int deadRangeSize = aliveRangeOffset - deadRangeOffset;
            while ( oldOffsetCursor > newOffsetCursor )
            {
                newOffset[newOffsetCursor] = oldOffset[newOffsetCursor] + deadRangeSize;
                newOffsetCursor++;
            }

            // Do move
            while ( moveOffset < (deadRangeOffset - deadRangeSize) )
            {
                // Move one block
                deadRangeOffset -= deadRangeSize;
                aliveRangeOffset -= deadRangeSize;
                cursor.copyTo( deadRangeOffset, cursor, aliveRangeOffset, deadRangeSize );
            }
            // Move the last piece
            int lastBlockSize = deadRangeOffset - moveOffset;
            if ( lastBlockSize > 0 )
            {
                deadRangeOffset -= lastBlockSize;
                aliveRangeOffset -= lastBlockSize;
                cursor.copyTo( deadRangeOffset, cursor, aliveRangeOffset, lastBlockSize );
            }
        }
        while ( !aliveKeysOffset.isEmpty() );
        // Update allocOffset
        int prevAllocOffset = getAllocOffset( cursor );
        setAllocOffset( cursor, aliveRangeOffset );

        // Zero pad reclaimed area
        zeroPad( cursor, prevAllocOffset, aliveRangeOffset - prevAllocOffset );

        // Update offset array
        int keyCount = keyCount( cursor );
        keyPos:
        for ( int pos = 0; pos < keyCount; pos++ )
        {
            int keyPosOffset = keyPosOffset( pos, type );
            cursor.setOffset( keyPosOffset );
            int keyOffset = readKeyOffset( cursor );
            for ( int index = 0; index < oldOffsetCursor; index++ )
            {
                if ( keyOffset == oldOffset[index] )
                {
                    // Overwrite with new offset
                    cursor.setOffset( keyPosOffset );
                    putKeyOffset( cursor, newOffset[index] );
                    continue keyPos;
                }
            }
        }

        // Update dead space
        setDeadSpace( cursor, 0 );
    }

    @Override
    boolean leafUnderflow( PageCursor cursor, int keyCount )
    {
        int halfSpace = this.halfSpace;
        int allocSpace = getAllocSpace( cursor, keyCount, LEAF );
        int deadSpace = getDeadSpace( cursor );
        int availableSpace = allocSpace + deadSpace;

        return availableSpace > halfSpace;
    }

    @Override
    int canRebalanceLeaves( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount )
    {
        int leftActiveSpace = totalActiveSpace( leftCursor, leftKeyCount );
        int rightActiveSpace = totalActiveSpace( rightCursor, rightKeyCount );

        if ( leftActiveSpace + rightActiveSpace < totalSpace )
        {
            // We can merge
            return -1;
        }
        if ( leftActiveSpace < rightActiveSpace )
        {
            // Moving keys to the right will only create more imbalance
            return 0;
        }

        int prevDelta;
        int currentDelta = Math.abs( leftActiveSpace - rightActiveSpace );
        int keysToMove = 0;
        int lastChunkSize;
        do
        {
            keysToMove++;
            lastChunkSize = totalSpaceOfKeyValue( leftCursor, leftKeyCount - keysToMove );
            leftActiveSpace -= lastChunkSize;
            rightActiveSpace += lastChunkSize;

            prevDelta = currentDelta;
            currentDelta = Math.abs( leftActiveSpace - rightActiveSpace );
        }
        while ( currentDelta < prevDelta );
        keysToMove--; // Move back to optimal split
        leftActiveSpace += lastChunkSize;
        rightActiveSpace -= lastChunkSize;

        int halfSpace = this.halfSpace;
        boolean canRebalance = leftActiveSpace > halfSpace && rightActiveSpace > halfSpace;
        return canRebalance ? keysToMove : 0;
    }

    @Override
    boolean canMergeLeaves( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount )
    {
        int leftActiveSpace = totalActiveSpace( leftCursor, leftKeyCount );
        int rightActiveSpace = totalActiveSpace( rightCursor, rightKeyCount );
        int totalSpace = this.totalSpace;
        return totalSpace >= leftActiveSpace + rightActiveSpace;
    }

    @Override
    void doSplitLeaf( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int insertPos, KEY newKey, VALUE newValue, KEY newSplitter,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        // Find middle
        int keyCountAfterInsert = leftKeyCount + 1;
        int middlePos = middleLeaf( leftCursor, insertPos, newKey, newValue );

        if ( middlePos == insertPos )
        {
            layout.copyKey( newKey, newSplitter );
        }
        else
        {
            keyAt( leftCursor, newSplitter, insertPos < middlePos ? middlePos - 1 : middlePos, LEAF );
        }
        int rightKeyCount = keyCountAfterInsert - middlePos;

        if ( insertPos < middlePos )
        {
            //                  v-------v       copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,X,_,_,_,_,_,_,_
            // middle           ^
            moveKeysAndValues( leftCursor, middlePos - 1, rightCursor, 0, rightKeyCount );
            defragmentLeaf( leftCursor );
            insertKeyValueAt( leftCursor, newKey, newValue, insertPos, middlePos - 1, stableGeneration, unstableGeneration );
        }
        else
        {
            //                  v---v           first copy
            //                        v-v       second copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,_,_,_,_,_,X,_,_
            // middle           ^

            // Copy everything in one go
            int newInsertPos = insertPos - middlePos;
            int keysToMove = leftKeyCount - middlePos;
            moveKeysAndValues( leftCursor, middlePos, rightCursor, 0, keysToMove );
            defragmentLeaf( leftCursor );
            insertKeyValueAt( rightCursor, newKey, newValue, newInsertPos, keysToMove, stableGeneration, unstableGeneration );
        }
        TreeNode.setKeyCount( leftCursor, middlePos );
        TreeNode.setKeyCount( rightCursor, rightKeyCount );
    }

    @Override
    void doSplitInternal( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int insertPos, KEY newKey,
            long newRightChild, long stableGeneration, long unstableGeneration, KEY newSplitter )
    {
        int keyCountAfterInsert = leftKeyCount + 1;
        int middlePos = middleInternal( leftCursor, insertPos, newKey );

        if ( middlePos == insertPos )
        {
            layout.copyKey( newKey, newSplitter );
        }
        else
        {
            keyAt( leftCursor, newSplitter, insertPos < middlePos ? middlePos - 1 : middlePos, INTERNAL );
        }
        int rightKeyCount = keyCountAfterInsert - middlePos - 1; // -1 because don't keep prim key in internal

        if ( insertPos < middlePos )
        {
            //                         v-------v       copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,X,_,_,_,_,_,_,_,_
            // insert child -,-,-,x,-,-,-,-,-,-,-,-
            // middle key              ^

            moveKeysAndChildren( leftCursor, middlePos, rightCursor, 0, rightKeyCount, true );
            // Rightmost key in left is the one we send up to parent, remove it from here.
            removeKeyAndRightChildAt( leftCursor, middlePos - 1, middlePos );
            defragmentInternal( leftCursor );
            insertKeyAndRightChildAt( leftCursor, newKey, newRightChild, insertPos, middlePos - 1, stableGeneration, unstableGeneration );
        }
        else
        {
            // pos > middlePos
            //                         v-v          first copy
            //                             v-v-v    second copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,_,_,_,_,_,X,_,_,_
            // insert child -,-,-,-,-,-,-,-,x,-,-,-
            // middle key              ^

            // pos == middlePos
            //                                      first copy
            //                         v-v-v-v-v    second copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,_,_,_,X,_,_,_,_,_
            // insert child -,-,-,-,-,-,x,-,-,-,-,-
            // middle key              ^

            // Keys
            if ( insertPos == middlePos )
            {
                int copyFrom = middlePos;
                int copyCount = leftKeyCount - copyFrom;
                moveKeysAndChildren( leftCursor, copyFrom, rightCursor, 0, copyCount, false );
                defragmentInternal( leftCursor );
                setChildAt( rightCursor, newRightChild, 0, stableGeneration, unstableGeneration );
            }
            else
            {
                int copyFrom = middlePos + 1;
                int copyCount = leftKeyCount - copyFrom;
                moveKeysAndChildren( leftCursor, copyFrom, rightCursor, 0, copyCount, true );
                // Rightmost key in left is the one we send up to parent, remove it from here.
                removeKeyAndRightChildAt( leftCursor, middlePos, middlePos + 1 );
                defragmentInternal( leftCursor );
                insertKeyAndRightChildAt( rightCursor, newKey, newRightChild, insertPos - copyFrom, copyCount,
                        stableGeneration, unstableGeneration );
            }
        }
        TreeNode.setKeyCount( leftCursor, middlePos );
        TreeNode.setKeyCount( rightCursor, rightKeyCount );
    }

    @Override
    void moveKeyValuesFromLeftToRight( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount,
            int fromPosInLeftNode )
    {
        defragmentLeaf( rightCursor );
        int numberOfKeysToMove = leftKeyCount - fromPosInLeftNode;

        // Push keys and values in right sibling to the right
        insertSlotsAt( rightCursor, 0, numberOfKeysToMove, rightKeyCount, keyPosOffsetLeaf( 0 ), bytesKeyOffset() );

        // Move (also updates keyCount of left)
        moveKeysAndValues( leftCursor, fromPosInLeftNode, rightCursor, 0, numberOfKeysToMove );

        // Right keyCount
        setKeyCount( rightCursor, rightKeyCount + numberOfKeysToMove );
    }

    // NOTE: Does update keyCount
    private void moveKeysAndValues( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count )
    {
        int firstAllocOffset = getAllocOffset( toCursor );
        int toAllocOffset = firstAllocOffset;
        for ( int i = 0; i < count; i++, toPos++ )
        {
            toAllocOffset = moveRawKeyValue( fromCursor, fromPos + i, toCursor, toAllocOffset );
            toCursor.setOffset( keyPosOffsetLeaf( toPos ) );
            putKeyOffset( toCursor, toAllocOffset );
        }
        setAllocOffset( toCursor, toAllocOffset );

        // Update deadspace
        int deadSpace = getDeadSpace( fromCursor );
        int totalMovedBytes = firstAllocOffset - toAllocOffset;
        setDeadSpace( fromCursor, deadSpace + totalMovedBytes );

        // Key count
        setKeyCount( fromCursor, fromPos );
    }

    /**
     * Transfer key and value from logical position in 'from' to physical position next to current alloc offset in 'to'.
     * Mark transferred key as dead.
     * @return new alloc offset in 'to'
     */
    private int moveRawKeyValue( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toAllocOffset )
    {
        // What to copy?
        placeCursorAtActualKey( fromCursor, fromPos, LEAF );
        int fromKeyOffset = fromCursor.getOffset();
        long keyValueSize = readKeyValueSize( fromCursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );

        // Copy
        int toCopy = getEntryOverhead( keySize, valueSize, keyValueSizeCap ) + keySize + valueSize;
        int newRightAllocSpace = toAllocOffset - toCopy;
        fromCursor.copyTo( fromKeyOffset, toCursor, newRightAllocSpace, toCopy );

        // Put tombstone
        fromCursor.setOffset( fromKeyOffset );
        putTombstone( fromCursor );
        return newRightAllocSpace;
    }

    @Override
    void copyKeyValuesFromLeftToRight( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount )
    {
        defragmentLeaf( rightCursor );

        // Push keys and values in right sibling to the right
        insertSlotsAt( rightCursor, 0, leftKeyCount, rightKeyCount, keyPosOffsetLeaf( 0 ), bytesKeyOffset() );

        // Copy
        copyKeysAndValues( leftCursor, 0, rightCursor, 0, leftKeyCount );

        // KeyCount
        setKeyCount( rightCursor, rightKeyCount + leftKeyCount );
    }

    private void copyKeysAndValues( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count )
    {
        int toAllocOffset = getAllocOffset( toCursor );
        for ( int i = 0; i < count; i++, toPos++ )
        {
            toAllocOffset = copyRawKeyValue( fromCursor, fromPos + i, toCursor, toAllocOffset );
            toCursor.setOffset( keyPosOffsetLeaf( toPos ) );
            putKeyOffset( toCursor, toAllocOffset );
        }
        setAllocOffset( toCursor, toAllocOffset );
    }

    /**
     * Copy key and value from logical position in 'from' tp physical position next to current alloc offset in 'to'.
     * Does NOT mark transferred key as dead.
     * @return new alloc offset in 'to'
     */
    private int copyRawKeyValue( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toAllocOffset )
    {
        // What to copy?
        placeCursorAtActualKey( fromCursor, fromPos, LEAF );
        int fromKeyOffset = fromCursor.getOffset();
        long keyValueSize = readKeyValueSize( fromCursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );

        // Copy
        int toCopy = getEntryOverhead( keySize, valueSize, keyValueSizeCap ) + keySize + valueSize;
        int newRightAllocSpace = toAllocOffset - toCopy;
        fromCursor.copyTo( fromKeyOffset, toCursor, newRightAllocSpace, toCopy );
        return newRightAllocSpace;
    }

    private int getAllocSpace( PageCursor cursor, int keyCount, Type type )
    {
        int allocOffset = getAllocOffset( cursor );
        int endOfOffsetArray = type == LEAF ? keyPosOffsetLeaf( keyCount ) : keyPosOffsetInternal( keyCount );
        return allocOffset - endOfOffsetArray;
    }

    private void recordDeadAndAliveLeaf( PageCursor cursor, PrimitiveIntStack deadKeysOffset, PrimitiveIntStack aliveKeysOffset )
    {
        int currentOffset = getAllocOffset( cursor );
        while ( currentOffset < pageSize )
        {
            cursor.setOffset( currentOffset );
            long keyValueSize = readKeyValueSize( cursor );
            int keySize = extractKeySize( keyValueSize );
            int valueSize = extractValueSize( keyValueSize );
            boolean dead = extractTombstone( keyValueSize );

            if ( dead )
            {
                deadKeysOffset.push( currentOffset );
            }
            else
            {
                aliveKeysOffset.push( currentOffset );
            }
            currentOffset += keySize + valueSize + getEntryOverhead( keySize, valueSize, keyValueSizeCap );
        }
    }

    private void recordDeadAndAliveInternal( PageCursor cursor, PrimitiveIntStack deadKeysOffset, PrimitiveIntStack aliveKeysOffset )
    {
        int currentOffset = getAllocOffset( cursor );
        while ( currentOffset < pageSize )
        {
            cursor.setOffset( currentOffset );
            long keyValueSize = readKeyValueSize( cursor );
            int keySize = extractKeySize( keyValueSize );
            boolean dead = extractTombstone( keyValueSize );

            if ( dead )
            {
                deadKeysOffset.push( currentOffset );
            }
            else
            {
                aliveKeysOffset.push( currentOffset );
            }
            currentOffset += keySize + getEntryOverhead( keySize, 0, keyValueSizeCap );
        }
    }

    // NOTE: Does NOT update keyCount
    private void moveKeysAndChildren( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count,
            boolean includeLeftMostChild )
    {
        // All children
        // This will also copy key offsets but those will be overwritten below.
        int childFromOffset = includeLeftMostChild ? childOffset( fromPos ) : childOffset( fromPos + 1 );
        int childToOffset = childOffset( fromPos + count ) + childSize();
        int lengthInBytes = childToOffset - childFromOffset;
        int targetOffset = includeLeftMostChild ? childOffset( 0 ) : childOffset( 1 );
        fromCursor.copyTo( childFromOffset, toCursor, targetOffset, lengthInBytes );

        // Move actual keys and update pointers
        int toAllocOffset = getAllocOffset( toCursor );
        int firstAllocOffset = toAllocOffset;
        for ( int i = 0; i < count; i++, toPos++ )
        {
            // Key
            toAllocOffset = transferRawKey( fromCursor, fromPos + i, toCursor, toAllocOffset );
            toCursor.setOffset( keyPosOffsetInternal( toPos ) );
            putKeyOffset( toCursor, toAllocOffset );
        }
        setAllocOffset( toCursor, toAllocOffset );

        // Update deadspace
        int deadSpace = getDeadSpace( fromCursor );
        int totalMovedBytes = firstAllocOffset - toAllocOffset;
        setDeadSpace( fromCursor, deadSpace + totalMovedBytes );

        // Zero pad empty area
        zeroPad( fromCursor, childFromOffset, lengthInBytes );
    }

    private void zeroPad( PageCursor fromCursor, int fromOffset, int lengthInBytes )
    {
        fromCursor.setOffset( fromOffset );
        fromCursor.putBytes( lengthInBytes, (byte) 0 );
    }

    private int transferRawKey( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toAllocOffset )
    {
        // What to copy?
        placeCursorAtActualKey( fromCursor, fromPos, INTERNAL );
        int fromKeyOffset = fromCursor.getOffset();
        int keySize = extractKeySize( readKeyValueSize( fromCursor ) );

        // Copy
        int toCopy = getEntryOverhead( keySize, 0, keyValueSizeCap ) + keySize;
        toAllocOffset -= toCopy;
        fromCursor.copyTo( fromKeyOffset, toCursor, toAllocOffset, toCopy );

        // Put tombstone
        fromCursor.setOffset( fromKeyOffset );
        putTombstone( fromCursor );
        return toAllocOffset;
    }

    private int middleInternal( PageCursor cursor, int insertPos, KEY newKey )
    {
        int halfSpace = this.halfSpace;
        int middle = 0;
        int currentPos = 0;
        int middleSpace = childSize(); // Leftmost child will always be included in left side
        int currentDelta = Math.abs( middleSpace - halfSpace );
        int prevDelta;
        boolean includedNew = false;

        do
        {
            // We may come closer to split by keeping one more in left
            middle++;
            currentPos++;
            int space;
            if ( currentPos == insertPos & !includedNew )
            {
                space = totalSpaceOfKeyChild( newKey );
                includedNew = true;
                currentPos--;
            }
            else
            {
                space = totalSpaceOfKeyChild( cursor, currentPos );
            }
            middleSpace += space;
            prevDelta = currentDelta;
            currentDelta = Math.abs( middleSpace - halfSpace );
        }
        while ( currentDelta < prevDelta );
        middle--; // Step back to the pos that most equally divide the available space in two
        return middle;
    }

    private int middleLeaf( PageCursor cursor, int insertPos, KEY newKey, VALUE newValue )
    {
        int halfSpace = this.halfSpace;
        int middle = 0;
        int currentPos = 0;
        int middleSpace = 0;
        int currentDelta = halfSpace;
        int prevDelta;
        boolean includedNew = false;

        do
        {
            // We may come closer to split by keeping one more in left
            middle++;
            currentPos++;
            int space;
            if ( currentPos == insertPos & !includedNew )
            {
                space = extractTreeNodeSizeOfTotalSpace( totalSpaceOfKeyValue( newKey, newValue ) );
                includedNew = true;
                currentPos--;
            }
            else
            {
                space = totalSpaceOfKeyValue( cursor, currentPos );
            }
            middleSpace += space;
            prevDelta = currentDelta;
            currentDelta = Math.abs( middleSpace - halfSpace );
        }
        while ( currentDelta < prevDelta );
        middle--; // Step back to the pos that most equally divide the available space in two
        return middle;
    }

    private int totalActiveSpace( PageCursor cursor, int keyCount )
    {
        int deadSpace = getDeadSpace( cursor );
        int allocSpace = getAllocSpace( cursor, keyCount, LEAF );
        return totalSpace - deadSpace - allocSpace;
    }

    private long totalSpaceOfKeyValue( KEY key, VALUE value )
    {
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        int overheadSize = getEntryOverhead( keySize, valueSize, keyValueSizeCap );
        return totalSpaceOfKeyValue( keySize, valueSize, overheadSize );
    }

    /**
     * Calculates total space required by a key/value pair. The resulting {@code long} has four parts to it:
     * <pre>
     *          8          7          6          5           4          3          2          1
     *     [OOOO,OOOO][OOOO,OOOO][OOOO,OOOO][OOOO,OOOO] [HHHH,HHHH}[VVVV,VVVV}[VVVV,KKKK][KKKK,KKKK]
     *     K: tree node key size, i.e. number of key bytes that needs to be stored in the tree node
     *     V: tree node value size, i.e. number of value bytes that needs to be stored in the tree node
     *     H: overhead (including entry offset, tree node key/value size and offload pointer)
     *     O: offload size, i.e. number of bytes that needs to be stored in offload storage
     * </pre>
     *
     * Use {@link #extractTreeNodeSizeOfTotalSpace(long)} for extracting N and {@link #extractOffloadSizeOfTotalSpace(long)} for extracting O.
     *
     * OBS! For now, for simplicity, ths assumption is that there will be zero or one offload pointer for an entry, not one for key and another for value.
     *
     * @param keySize the key size to calculate space for.
     * @param valueSize the value size to calculate space for.
     * @param entryOverheadSize the overhead of entry header (not including entry offset pointer)
     * @return combination of tree node space and offload space needed for storing this entry (key/value).
     */
    private long totalSpaceOfKeyValue( int keySize, int valueSize, int entryOverheadSize )
    {
        int entrySize = entryOverheadSize + keySize + valueSize;

        // Divide the size into what will be in the tree node and what will have to be stored in offload
        int treeNodeKeySize;
        int treeNodeValueSize;
        int offloadSize;
        if ( entrySize <= keyValueSizeCap )
        {
            // All bytes fit inside tree-node
            treeNodeKeySize = keySize;
            treeNodeValueSize = valueSize;
            offloadSize = 0;
        }
        else
        {
            // Value will not be split up where some of its bytes lives in tree node and some in offload storage,
            // i.e. valueSize doesn't enter into treeNodeSize calculation
            treeNodeKeySize = min( keyValueSizeCap - entryOverheadSize, keySize );
            treeNodeValueSize = 0;
            offloadSize = keySize - treeNodeKeySize + valueSize;
        }

        return (((long) offloadSize) << Integer.SIZE) |                     // offload
               (entryOverheadSize + bytesKeyOffset() ) << (Byte.SIZE * 3) | // overhead
               (treeNodeValueSize << 12) |                                  // tree node value size
               treeNodeKeySize;                                             // tree node key size

    }

    /**
     * Extracts N part from result of {@link #totalSpaceOfKeyValue(Object, Object)} or {@link #totalSpaceOfKeyChild(Object)}.
     *
     * @param space result from {@link #totalSpaceOfKeyValue(Object, Object)}.
     * @return N part of the space.
     */
    private static int extractTreeNodeSizeOfTotalSpace( long space )
    {
        return extractTreeNodeKeySizeOfTotalSpace( space ) +
                extractTreeNodeValueSizeOfTotalSpace( space ) +
                extractTreeNodeOverheadSizeOfTotalSpace( space );
    }

    private static int extractTreeNodeOverheadSizeOfTotalSpace( long space )
    {
        int lsb = (int) space;
        return lsb >>> (Byte.SIZE * 3);
    }

    private static int extractTreeNodeKeySizeOfTotalSpace( long space )
    {
        return (int) (space & 0xFFF);
    }

    private static int extractTreeNodeValueSizeOfTotalSpace( long space )
    {
        return (int) ((space >>> 12) & 0xFFF);
    }

    /**
     * Extracts O part from result of {@link #totalSpaceOfKeyValue(Object, Object)} or {@link #totalSpaceOfKeyChild(Object)}.
     *
     * @param space result from {@link #totalSpaceOfKeyValue(Object, Object)}.
     * @return O part of the space.
     */
    private static int extractOffloadSizeOfTotalSpace( long space )
    {
        return (int) (space >>> Integer.SIZE);
    }

    private int totalSpaceOfKeyChild( KEY key )
    {
        int keySize = layout.keySize( key );
        return bytesKeyOffset() + getEntryOverhead( keySize, 0, keyValueSizeCap ) + childSize() + keySize;
    }

    private int totalSpaceOfKeyValue( PageCursor cursor, int pos )
    {
        placeCursorAtActualKey( cursor, pos, LEAF );
        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );
        return bytesKeyOffset() + getEntryOverhead( keySize, valueSize, keyValueSizeCap ) + keySize + valueSize;
    }

    private int totalSpaceOfKeyChild( PageCursor cursor, int pos )
    {
        placeCursorAtActualKey( cursor, pos, INTERNAL );
        int keySize = extractKeySize( readKeyValueSize( cursor ) );
        return bytesKeyOffset() + getEntryOverhead( keySize, 0, keyValueSizeCap ) + childSize() + keySize;
    }

    private void setAllocOffset( PageCursor cursor, int allocOffset )
    {
        PageCursorUtil.putUnsignedShort( cursor, BYTE_POS_ALLOCOFFSET, allocOffset );
    }

    int getAllocOffset( PageCursor cursor )
    {
        return PageCursorUtil.getUnsignedShort( cursor, BYTE_POS_ALLOCOFFSET );
    }

    private void setDeadSpace( PageCursor cursor, int deadSpace )
    {
        putUnsignedShort( cursor, BYTE_POS_DEADSPACE, deadSpace );
    }

    private int getDeadSpace( PageCursor cursor )
    {
        return PageCursorUtil.getUnsignedShort( cursor, BYTE_POS_DEADSPACE );
    }

    private void placeCursorAtActualKey( PageCursor cursor, int pos, Type type )
    {
        // Set cursor to correct place in offset array
        int keyPosOffset = keyPosOffset( pos, type );
        cursor.setOffset( keyPosOffset );

        // Read actual offset to key
        int keyOffset = readKeyOffset( cursor );

        // Verify offset is reasonable
        if ( keyOffset >= pageSize || keyOffset < HEADER_LENGTH_DYNAMIC )
        {
            cursor.setCursorException( format( "Tried to read key on offset=%d, headerLength=%d, pageSize=%d, pos=%d",
                    keyOffset, HEADER_LENGTH_DYNAMIC, pageSize, pos ) );
            return;
        }

        // Set cursor to actual offset
        cursor.setOffset( keyOffset );
    }

    private int keyPosOffset( int pos, Type type )
    {
        if ( type == LEAF )
        {
            return keyPosOffsetLeaf( pos );
        }
        else
        {
            return keyPosOffsetInternal( pos );
        }
    }

    private int keyPosOffsetLeaf( int pos )
    {
        return HEADER_LENGTH_DYNAMIC + pos * bytesKeyOffset();
    }

    private int keyPosOffsetInternal( int pos )
    {
        // header + childPointer + pos * (keyPosOffsetSize + childPointer)
        return HEADER_LENGTH_DYNAMIC + childSize() + pos * keyChildSize();
    }

    private int keyChildSize()
    {
        return bytesKeyOffset() + SIZE_PAGE_REFERENCE;
    }

    private int childSize()
    {
        return SIZE_PAGE_REFERENCE;
    }

    private static int bytesKeyOffset()
    {
        return SIZE_OFFSET;
    }

    private static int bytesPageOffset()
    {
        return SIZE_OFFSET;
    }

    @Override
    public String toString()
    {
        return "TreeNodeDynamicSize[pageSize:" + pageSize + ", keyValueSizeCap:" + keyValueSizeCap + "]";
    }

    private String asString( PageCursor cursor, boolean includeValue, boolean includeAllocSpace,
            long stableGeneration, long unstableGeneration )
    {
        int currentOffset = cursor.getOffset();
        // [header] <- dont care
        // LEAF:     [allocOffset=][child0,key0*,child1,...][keySize|key][keySize|key]
        // INTERNAL: [allocOffset=][key0*,key1*,...][offset|keySize|valueSize|key][keySize|valueSize|key]

        Type type = isInternal( cursor ) ? INTERNAL : LEAF;

        // HEADER
        int allocOffset = getAllocOffset( cursor );
        int deadSpace = getDeadSpace( cursor );
        String additionalHeader = "{" + cursor.getCurrentPageId() + "} [allocOffset=" + allocOffset + " deadSpace=" + deadSpace + "] ";

        // OFFSET ARRAY
        String offsetArray = readOffsetArray( cursor, stableGeneration, unstableGeneration, type );

        // ALLOC SPACE
        String allocSpace = "";
        if ( includeAllocSpace )
        {
            allocSpace = readAllocSpace( cursor, allocOffset, type );
        }

        // KEYS
        KEY readKey = layout.newKey();
        VALUE readValue = layout.newValue();
        StringJoiner keys = new StringJoiner( " " );
        cursor.setOffset( allocOffset );
        while ( cursor.getOffset() < cursor.getCurrentPageSize() )
        {
            StringJoiner singleKey = new StringJoiner( "|" );
            singleKey.add( Integer.toString( cursor.getOffset() ) );
            long keyValueSize = readKeyValueSize( cursor );
            int keySize = extractKeySize( keyValueSize );
            int valueSize = 0;
            if ( type == LEAF )
            {
                valueSize = extractValueSize( keyValueSize );
            }
            if ( DynamicSizeUtil.extractTombstone( keyValueSize ) )
            {
                singleKey.add( "X" );
            }
            else
            {
                singleKey.add( "_" );
            }
            layout.readKey( cursor, readKey, keySize, 0, keySize );
            if ( type == LEAF )
            {
                layout.readValue( cursor, readValue, valueSize, 0, valueSize );
            }
            singleKey.add( Integer.toString( keySize ) );
            if ( type == LEAF && includeValue )
            {
                singleKey.add( Integer.toString( valueSize ) );
            }
            singleKey.add( readKey.toString() );
            if ( type == LEAF && includeValue )
            {
                singleKey.add( readValue.toString() );
            }
            keys.add( singleKey.toString() );
        }

        cursor.setOffset( currentOffset );
        return additionalHeader + offsetArray + " " + allocSpace + " " + keys;
    }

    @SuppressWarnings( "unused" )
    @Override
    void printNode( PageCursor cursor, boolean includeValue, boolean includeAllocSpace, long stableGeneration, long unstableGeneration )
    {
        System.out.println( asString( cursor, includeValue, includeAllocSpace, stableGeneration, unstableGeneration ) );
    }

    private String readAllocSpace( PageCursor cursor, int allocOffset, Type type )
    {
        int keyCount = keyCount( cursor );
        int endOfOffsetArray = type == INTERNAL ? keyPosOffsetInternal( keyCount ) : keyPosOffsetLeaf( keyCount );
        cursor.setOffset( endOfOffsetArray );
        int bytesToRead = allocOffset - endOfOffsetArray;
        byte[] allocSpace = new byte[bytesToRead];
        cursor.getBytes( allocSpace );
        for ( byte b : allocSpace )
        {
            if ( b != 0 )
            {
                return "v" + endOfOffsetArray + ">" + bytesToRead + "|" + Arrays.toString( allocSpace );
            }
        }
        return "v" + endOfOffsetArray + ">" + bytesToRead + "|[0...]";
    }

    private String readOffsetArray( PageCursor cursor, long stableGeneration, long unstableGeneration, Type type )
    {
        int keyCount = keyCount( cursor );
        StringJoiner offsetArray = new StringJoiner( " " );
        for ( int i = 0; i < keyCount; i++ )
        {
            if ( type == INTERNAL )
            {
                long childPointer = GenerationSafePointerPair.pointer( childAt( cursor, i, stableGeneration, unstableGeneration ) );
                offsetArray.add( "/" + Long.toString( childPointer ) + "\\" );
            }
            cursor.setOffset( keyPosOffset( i, type ) );
            offsetArray.add( Integer.toString( DynamicSizeUtil.readKeyOffset( cursor ) ) );
        }
        if ( type == INTERNAL )
        {
            long childPointer = GenerationSafePointerPair.pointer( childAt( cursor, keyCount, stableGeneration, unstableGeneration ) );
            offsetArray.add( "/" + Long.toString( childPointer ) + "\\" );
        }
        return offsetArray.toString();
    }
}
