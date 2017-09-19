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

import static org.junit.Assert.assertFalse;

public class SeekCursorEnterpriseTest extends SeekCursorTest
{
    private int deltaMaxKeyCount;

    @Override
    protected TreeNode<MutableLong,MutableLong> createTreeNode( int pageSize, Layout<MutableLong,MutableLong> layout )
    {
        TreeNode<MutableLong,MutableLong> result = new TreeNodeDelta<>( pageSize, layout );
        deltaMaxKeyCount = result.delta().leafMaxKeyCount();
        assert deltaMaxKeyCount > 1 : deltaMaxKeyCount;
        return result;
    }

    @Test
    public void shouldViewMainAndDeltaSectionsAsMergedOnScan() throws Exception
    {
        // GIVEN
        insert( 1 );
        insert( 3 );
        insert( 5 );
        insert( 7 );
        insert( 2 );
        insert( 6 );

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( 1, 8 ) )
        {
            // THEN
            assertResults( cursor, 1, 2, 3, 5, 6, 7 );
        }
    }

    @Test
    public void shouldRediscoverMainAndDeltaSectionsOnReread() throws Exception
    {
        // GIVEN
        insert( 1 );
        insert( 3 );
        insert( 5 );
        insert( 7 );
        insert( 2 );
        insert( 6 );

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( 1, 8 ) )
        {
            // THEN
            assertNext( cursor, 1 );
            assertNext( cursor, 2 );
            remove( 2 );
            remove( 3 );
            forceRetry();
            assertNext( cursor, 5 );
            assertNext( cursor, 6 );
            assertNext( cursor, 7 );
            assertFalse( cursor.next() );
        }
    }
}
