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

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNITS_PER_PAGE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.unitInPage;

class Header
{
    static final int HEADER_BITSET_SIZE = Long.BYTES * 2;
    static final int HEADER_OFFSET_IN_USE = 0;
    static final int HEADER_OFFSET_START = HEADER_BITSET_SIZE;

    static void mark( PageCursor cursor, long startId, int units, boolean inUse )
    {
        // Header is the first UNIT_SIZE bytes in each page
        // Mark in use
        int startUnitInPage = unitInPage( startId );
        if ( startUnitInPage < 64 )
        {
            long bits = cursor.getLong( HEADER_OFFSET_IN_USE );
            for ( int i = 0; i < units && startUnitInPage + i < 64; i++ )
            {
                bits = setBit( bits, startUnitInPage + i, inUse );
            }
            cursor.putLong( HEADER_OFFSET_IN_USE, bits );
        }
        if ( startUnitInPage + units > 64 )
        {
            int unitsInSecondLong = 64 - (startUnitInPage + units);
            long bits = cursor.getLong( HEADER_OFFSET_IN_USE + Long.BYTES );
            for ( int i = 0; i < unitsInSecondLong; i++ )
            {
                bits = setBit( bits, startUnitInPage - 64 + i, inUse );
            }
            cursor.putLong( HEADER_OFFSET_IN_USE + Long.BYTES, bits );
        }

        // Mark start
        if ( startUnitInPage < 64 )
        {   // First long
            long bits = cursor.getLong( HEADER_OFFSET_START );
            bits = setBit( bits, startUnitInPage, inUse );
            cursor.putLong( HEADER_OFFSET_START, bits );
        }
        else
        {   // Second long
            long bits = cursor.getLong( HEADER_OFFSET_START + Long.BYTES );
            bits = setBit( bits, startUnitInPage - 64, inUse );
            cursor.putLong( HEADER_OFFSET_START + Long.BYTES, bits );
        }
    }

    private static long setBit( long bits, int bit, boolean inUse )
    {
        return inUse ? bits | 1L << bit : bits & ~(1L << bit);
    }

    static int numberOfUnits( PageCursor cursor, long id )
    {
        int startUnit = unitInPage( id );
        int searchUnit = startUnit + 1;
        if ( searchUnit == UNITS_PER_PAGE )
        {   // This is the last unit in this page, for now this means that this record is length 1
            return 1;
        }

        // First check for the next start mark
        if ( searchUnit < 64 )
        {   // First long
            long bits = cursor.getLong( HEADER_OFFSET_START );
            long higherBits = bits & ~((1L << searchUnit) - 1);
            int nextStartUnit = Long.numberOfTrailingZeros( higherBits );
            if ( nextStartUnit < 64 )
            {   // A record start found after this point
                return nextStartUnit - startUnit;
            }

            // No more records after this point in the first long, check the second long
            long secondBits = cursor.getLong( HEADER_OFFSET_START + Long.BYTES );
            int secondNextStartUnit = Long.numberOfTrailingZeros( secondBits );
            if ( secondNextStartUnit < 64 )
            {   // Next start found in the second long
                return 2 + secondNextStartUnit;
            }
        }
        else
        {   // Second long
            long bits = cursor.getLong( HEADER_OFFSET_START + Long.BYTES );
            long higherBits = bits & ~((1L << (searchUnit - 64)) - 1);
            int nextSetBitIndex = Long.numberOfTrailingZeros( higherBits );
            if ( nextSetBitIndex < 64 )
            {   // A record start found after this point
                return (64 + nextSetBitIndex) - startUnit;
            }
        }

        // If none found then count the number of inUse records from that point
        int unit = searchUnit;
        if ( unit < 64 )
        {   // Count in the first long
            long bits = cursor.getLong( HEADER_OFFSET_IN_USE );
            while ( unit < 64 && bitIsSet( bits, unit ) )
            {
                unit++;
            }
        }
        if ( unit >= 64 )
        {   // Count in the second long
            long bits = cursor.getLong( HEADER_OFFSET_IN_USE + Long.BYTES );
            while ( unit < 128 && bitIsSet( bits, unit - 64 ) )
            {
                unit++;
            }
        }
        return unit - startUnit;
    }

    private static boolean bitIsSet( long bits, int bit )
    {
        return (bits & (1L << bit)) != 0;
    }
}
