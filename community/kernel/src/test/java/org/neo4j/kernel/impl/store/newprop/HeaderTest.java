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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static java.lang.Integer.min;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.EFFECTIVE_UNITS_PER_PAGE;

public class HeaderTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    private PageCursor cursor;

    @Before
    public void before()
    {
        cursor = ByteArrayPageCursor.wrap( (int) kibiBytes( 8 ) );
    }

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
    public void shouldMarkSingleHighUnitAsUsed() throws Exception
    {
        markAndReadNumberOfUnits( 70, 1, true );
    }

    @Test
    public void shouldMarkTwoUnitHighRecordAsUsed() throws Exception
    {
        markAndReadNumberOfUnits( 70, 2, true );
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

    @Test
    public void shouldMarkRandomUnits() throws Exception
    {
        // given
        List<int[]> allocations = new ArrayList<>();
        BitSet occupied = new BitSet();
        int unitsOccupied = 0;

        // when
        for ( int i = 0; i < 100; i++ )
        {
            if ( unitsOccupied < EFFECTIVE_UNITS_PER_PAGE && random.nextFloat() < 0.7 )
            {   // mark as used
                int startId;
                do
                {
                    startId = random.nextInt( EFFECTIVE_UNITS_PER_PAGE );
                }
                while ( occupied.get( startId ) );
                int cappedAvailableUnits = min( EFFECTIVE_UNITS_PER_PAGE / 2, availableUnitsFrom( startId, occupied ) );
                int units = cappedAvailableUnits == 1 ? 1 : random.nextInt( cappedAvailableUnits - 1 ) + 1;
                Header.mark( cursor, startId, units, true );

                unitsOccupied += units;
                allocations.add( new int[] {startId, units} );
                for ( int j = 0; j < units; j++ )
                {
                    occupied.set( startId + j );
                }
            }
            else if ( unitsOccupied > 0 )
            {   // mark as unused
                int allocationIndex = random.nextInt( allocations.size() );
                int[] allocation = allocations.remove( allocationIndex );
                int startId = allocation[0];
                int units = allocation[1];
                Header.mark( cursor, startId, units, false );

                unitsOccupied -= units;
                for ( int j = 0; j < units; j++ )
                {
                    occupied.clear( startId + j );
                }
            }

            // check everything against expected
            for ( int[] allocation : allocations )
            {
                int startId = allocation[0];
                int units = allocation[1];
                if ( !Header.isStartUnit( cursor, startId ) )
                {
                    fail( "Start id " + startId + " wasn't marked as start unit\n" + Header.rawBits( cursor ) );
                }
                int markedUnits = Header.numberOfUnits( cursor, startId );
                if ( markedUnits != units )
                {
                    fail( "Start id " + startId + " was marked as having " + markedUnits + " units, but should've been " + units +
                            "\n" + Header.rawBits( cursor ) );
                }
            }
        }
    }

    private int availableUnitsFrom( int startId, BitSet occupied )
    {
        assert !occupied.get( startId );
        int units = 1;
        while ( !occupied.get( startId + units ) && startId + units < EFFECTIVE_UNITS_PER_PAGE )
        {
            units++;
        }
        return units;
    }
}
