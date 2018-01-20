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
package org.neo4j.kernel.impl.store.newprop;

import org.junit.Test;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.io.ByteUnit.kibiBytes;

public class HeaderTest
{
    private final PageCursor cursor = ByteArrayPageCursor.wrap( (int) kibiBytes( 8 ) );

    @Test
    public void shouldMarkSingleUnitAsUsed() throws Exception
    {
        markAndReadNumberOfUnits( 0, 1, true );
    }

    @Test
    public void shouldMarkTwoUnitRecordAsUsed() throws Exception
    {
        markAndReadNumberOfUnits( 0, 2, true );
    }

    @Test
    public void shouldMarkSingleUnitAsUnused() throws Exception
    {
        markAndReadNumberOfUnits( 0, 1, false );
    }

    @Test
    public void shouldMarkTwoUnitRecordAsUnused() throws Exception
    {
        markAndReadNumberOfUnits( 0, 2, false );
    }

    private void markAndReadNumberOfUnits( long id, int units, boolean used )
    {
        // when
        Header.mark( cursor, id, units, used );

        // then
        if ( used )
        {
            assertEquals( units, Header.numberOfUnits( cursor, id ) );
            assertTrue( Header.isStartUnit( cursor, id ) );
        }
        else
        {
            assertEquals( 0, Header.numberOfUnits( cursor, id ) );
            assertFalse( Header.isStartUnit( cursor, id ) );
        }
    }
}
