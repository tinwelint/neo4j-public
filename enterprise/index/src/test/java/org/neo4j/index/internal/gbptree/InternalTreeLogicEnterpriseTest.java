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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Test;

import org.neo4j.index.internal.gbptree.TreeNode.Section;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;

public class InternalTreeLogicEnterpriseTest extends InternalTreeLogicTest
{
    @Override
    protected TreeNode<MutableLong,MutableLong> instantiateTreeNode( int pageSize, Layout<MutableLong,MutableLong> layout )
    {
        TreeNode<MutableLong,MutableLong> format = new TreeNodeDelta<>( pageSize, layout );
        assertTrue( format.delta().leafMaxKeyCount() > 2 );
        return format;
    }

    @Test
    public void shouldOverwriteValueInMainEvenIfThereAreKeysInDelta() throws Exception
    {
        // given
        initialize();
        rawInsert( 2, 2, mainSection, 0 );
        rawInsert( 4, 4, mainSection, 1 );
        rawInsert( 3, 3, deltaSection, 0 );
        int overwrittenValue = 44;

        // when
        insertKey.setValue( 4 );
        insertValue.setValue( overwrittenValue );
        treeLogic.insert( cursor, structurePropagation, insertKey, insertValue, overwrite(), stableGeneration, unstableGeneration );

        // then
        assertEquals( 4, mainSection.keyAt( cursor, insertKey, 1 ).longValue() );
        assertEquals( overwrittenValue, mainSection.valueAt( cursor, insertValue, 1 ).longValue() );
    }

    @Test
    public void shouldInsertIntoDeltaSectionIfFartherFromEndThanDeltaSize() throws Exception
    {
        // given two keys in main
        initialize();
        insert( 2 );
        insert( 4 );
        assertEquals( 2, mainSection.keyCount( cursor ) );
        assertEquals( 0, deltaSection.keyCount( cursor ) );

        // when inserting a bit to the left
        insert( 1 );

        // then the key should end up in delta
        assertEquals( 2, mainSection.keyCount( cursor ) );
        assertEquals( 1, deltaSection.keyCount( cursor ) );

        // and when inserting more to the right, keep in mind that delta size is now 1
        insert( 3 );

        // then the key should end up in main
        assertEquals( 3, mainSection.keyCount( cursor ) );
        assertEquals( 1, deltaSection.keyCount( cursor ) );
    }

    @Test
    public void shouldOverwriteValueInDeltaIfExistsThere() throws Exception
    {
        // given
        initialize();
        insert( 2 );
        insert( 4 );
        insert( 3 );
        assertEquals( 2, mainSection.keyCount( cursor ) );
        assertEquals( 1, deltaSection.keyCount( cursor ) );

        // when
        insert( 3, 5 );

        // then
        assertEquals( 5, deltaSection.valueAt( cursor, insertKey, 0 ).longValue() );
    }

    @Test
    public void shouldRemoveFromDeltaIfKeyExistsThere() throws Exception
    {
        // given
        initialize();
        insert( 2 );
        insert( 4 );
        insert( 6 );
        insert( 8 );
        insert( 10 );
        insert( 5 );
        insert( 3 );
        insert( 1 );
        assertEquals( 5, mainSection.keyCount( cursor ) );
        assertEquals( 3, deltaSection.keyCount( cursor ) );

        // when
        remove( 3, insertValue );

        // then
        assertEquals( 2, deltaSection.keyCount( cursor ) );
        assertEquals( 1, deltaSection.keyAt( cursor, insertKey, 0 ).longValue() );
        assertEquals( 5, deltaSection.keyAt( cursor, insertKey, 1 ).longValue() );
    }

    @Test
    public void shouldConsolidateDeltaIntoMainIfFull() throws Exception
    {
        // given
        initialize();

        // when
        int prevDeltaCount = 0;
        int count = 0;
        for ( ; true; count++ )
        {
            // inserting from the back will have a lot end up in delta section
            long key = 1000 - count;
            insert( key );
            int deltaCount = deltaSection.keyCount( cursor );
            if ( deltaCount < prevDeltaCount )
            {
                break;
            }
            prevDeltaCount = deltaCount;
        }

        // then
        assertEquals( count, mainSection.keyCount( cursor ) );
    }

    @Test
    public void shouldConsolidateDeltaIntoMainBeforeSplit() throws Exception
    {
        // given
        initialize();
        for ( int i = 0; i < maxKeyCount - 1; i++ )
        {
            insert( 100 + i );
        }
        insert( 0 ); // delta

        // when splitting
        insert( 1 );

        // then all the keys must be in the main sections
        long leftChild = mainSection.childAt( cursor, 0, stableGeneration, unstableGeneration );
        long rightChild = mainSection.childAt( cursor, 1, stableGeneration, unstableGeneration );
        node.goTo( cursor, "left", leftChild );
        int leftKeyCount = mainSection.keyCount( cursor );
        assertEquals( 0, deltaSection.keyCount( cursor ) );
        node.goTo( cursor, "left", rightChild );
        int rightKeyCount = mainSection.keyCount( cursor );
        assertEquals( 0, deltaSection.keyCount( cursor ) );
        assertEquals( maxKeyCount + 1, leftKeyCount + rightKeyCount );
    }

    @Test
    public void shouldConsolidateDeltaIntoMainBeforeRebalance() throws Exception
    {
        // given a root w/ two children
        initialize();
        int key = 0;
        while ( numberOfRootSplits == 0 )
        {
            insert( (key++) * 2 );
        }
        // get some data into delta section of the left child
        long leftChild = mainSection.childAt( cursor, 0, stableGeneration, unstableGeneration );
        long midChild = mainSection.childAt( cursor, 1, stableGeneration, unstableGeneration );
        insert( 1 );
        insert( 3 );
        // and splitting the mid child into two
        insert( key++ );
        MutableLong midKey = new MutableLong();
        mainSection.keyAt( cursor, midKey, mainSection.keyCount( cursor ) / 2 );
        int toInsert = maxKeyCount - mainSection.keyCount( cursor ) + 5;
        for ( int i = 0; i < toInsert; i++ )
        {
            insert( (key++) * 2 );
        }
        insert( ((maxKeyCount * 3 / 4) * 2) + 1 ); // to get something into delta in mid child
        // when removing, causing rebalance
        int toRemove;
        {
            long prev = cursor.getCurrentPageId();
            node.goTo( cursor, "mid", midChild );
            toRemove = (mainSection.keyCount( cursor ) + deltaSection.keyCount( cursor )) - (maxKeyCount + 1) / 2;
            node.goTo( cursor, "prev", prev );
        }
        for ( int i = 0; i <= toRemove; i++ )
        {
            remove( midKey.longValue() + i * 2 );
        }

        // then all the keys must be in the main sections
        node.goTo( cursor, "left", leftChild );
        int leftKeyCount = mainSection.keyCount( cursor );
        assertEquals( 0, deltaSection.keyCount( cursor ) );
        node.goTo( cursor, "mid", midChild );
        int midKeyCount = mainSection.keyCount( cursor );
        assertEquals( 0, deltaSection.keyCount( cursor ) );
        assertEquals( maxKeyCount + 1, leftKeyCount + midKeyCount );
    }

    @Test
    public void shouldConsolidateDeltaIntoMainBeforeMerge() throws Exception
    {
        // given a root w/ two children
        initialize();
        int key = 0;
        while ( numberOfRootSplits == 0 )
        {
            insert( (key++) * 2 );
        }
        // get some data into delta section of the left child
        long midChild = mainSection.childAt( cursor, 1, stableGeneration, unstableGeneration );
        insert( 1 );
        insert( 3 );
        remove( 0 );
        remove( 2 );
        // and splitting the mid child into two
        insert( key++ );
        MutableLong midKey = new MutableLong();
        mainSection.keyAt( cursor, midKey, mainSection.keyCount( cursor ) / 2 );
        int toInsert = maxKeyCount - mainSection.keyCount( cursor ) + 5;
        for ( int i = 0; i < toInsert; i++ )
        {
            insert( (key++) * 2 );
        }
        insert( ((maxKeyCount * 3 / 4) * 2) + 1 ); // to get something into delta in mid child
        // when removing, causing rebalance
        int toRemove;
        {
            long prev = cursor.getCurrentPageId();
            node.goTo( cursor, "mid", midChild );
            toRemove = (mainSection.keyCount( cursor ) + deltaSection.keyCount( cursor )) - (maxKeyCount + 1) / 2;
            node.goTo( cursor, "prev", prev );
        }
        for ( int i = 0; i <= toRemove; i++ )
        {
            remove( midKey.longValue() + i * 2 );
        }

        // then all the keys must be in the main section
        node.goTo( cursor, "left", midChild );
        int midKeyCount = mainSection.keyCount( cursor );
        assertEquals( 0, deltaSection.keyCount( cursor ) );
        assertEquals( maxKeyCount - 1, midKeyCount );
    }

    @Test
    public void shouldMoveHighestFromDeltaToMainWhenRemoveHighestMain() throws Exception
    {
        // given
        initialize();
        // main
        int i = 0;
        for ( ; i < maxKeyCount - 1; i++ )
        {
            insert( i * 2 );
        }
        //delta
        int deltaKey = (i - 2) * 2 - 1;
        insert( deltaKey );
        assertEquals( i, mainSection.keyCount( cursor ) );
        assertEquals( 1, deltaSection.keyCount( cursor ) );

        // when
        remove( (i - 1) * 2 );
        assertEquals( i - 1, mainSection.keyCount( cursor ) );
        assertEquals( 1, deltaSection.keyCount( cursor ) );
        remove( (i - 2) * 2 );

        // then
        int mainKeyCount = mainSection.keyCount( cursor );
        assertEquals( i - 1, mainKeyCount );
        assertEquals( 0, deltaSection.keyCount( cursor ) );
        assertEquals( deltaKey, mainSection.keyAt( cursor, readKey, mainKeyCount - 1 ).longValue() );
    }

    private void rawInsert( long key, long value, Section<MutableLong,MutableLong> section, int pos )
    {
        int keyCount = section.keyCount( cursor );
        insertKey.setValue( key );
        section.insertKeyAt( cursor, insertKey, pos, keyCount );
        insertValue.setValue( value );
        section.insertValueAt( cursor, insertValue, pos, keyCount );
        section.setKeyCount( cursor, keyCount + 1 );
    }
}
