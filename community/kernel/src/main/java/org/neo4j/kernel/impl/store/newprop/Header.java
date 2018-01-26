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

import static java.lang.Integer.max;
import static java.lang.Integer.min;

import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.EFFECTIVE_UNITS_PER_PAGE;
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
            int startUnitInSecondLong = max( 64, startUnitInPage ) - 64;
            int unitsInSecondLong = min( units, startUnitInPage + units - 64 );
            long bits = cursor.getLong( HEADER_OFFSET_IN_USE + Long.BYTES );
            for ( int i = 0; i < unitsInSecondLong; i++ )
            {
                bits = setBit( bits, startUnitInSecondLong + i, inUse );
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
        if ( !isStartUnit( cursor, startUnit ) )
        {
            return 0;
        }

        int searchUnit = startUnit + 1;
//        if ( searchUnit % EFFECTIVE_UNITS_PER_PAGE == EFFECTIVE_UNITS_PER_PAGE - 1 )
//        {   // This is the last unit in this page, for now this means that this record is length 1
//            return 1;
//        }

        // First check for the next start mark
        int endUnit = EFFECTIVE_UNITS_PER_PAGE;
        if ( searchUnit < 64 )
        {   // First long
            long bits = cursor.getLong( HEADER_OFFSET_START );
            long higherBits = bits & ~((1L << searchUnit) - 1);
            int nextStartUnit = Long.numberOfTrailingZeros( higherBits );
            if ( nextStartUnit < 64 )
            {   // Next start found in the first long
                endUnit = nextStartUnit;
            }
            else
            {
                // No more records after this point in the first long, check the second long
                long secondBits = cursor.getLong( HEADER_OFFSET_START + Long.BYTES );
                int secondNextStartUnit = Long.numberOfTrailingZeros( secondBits );
                if ( secondNextStartUnit < 64 )
                {   // Next start found in the second long
                    endUnit = 64 + secondNextStartUnit;
                }
            }
        }
        else
        {   // Second long
            long bits = cursor.getLong( HEADER_OFFSET_START + Long.BYTES );
            long higherBits = bits & ~((1L << (searchUnit - 64)) - 1);
            int nextSetBitIndex = Long.numberOfTrailingZeros( higherBits );
            if ( nextSetBitIndex < 64 )
            {   // Next start found in the second long
                endUnit = 64 + nextSetBitIndex;
            }
        }

        // If none found then count the number of inUse records from that point
        int unit = searchUnit;
        if ( unit < 64 )
        {   // Count in the first long
            // TODO use Long.numberOfTrailing/LeadingZeros method instead of looping?
            long bits = cursor.getLong( HEADER_OFFSET_IN_USE );
            while ( unit < 64 && unit < endUnit && bitIsSet( bits, unit )  )
            {
                unit++;
            }
        }
        if ( unit >= 64 )
        {   // Count in the second long
            // TODO use Long.numberOfTrailing/LeadingZeros method instead of looping?
            long bits = cursor.getLong( HEADER_OFFSET_IN_USE + Long.BYTES );
            while ( unit < 128 && unit < endUnit && bitIsSet( bits, unit - 64 ) )
            {
                unit++;
            }
        }
        return unit - startUnit;
    }

    static boolean isStartUnit( PageCursor cursor, long id )
    {
        int unit = unitInPage( id );
        return isStartUnit( cursor, unit );
    }

    private static boolean isStartUnit( PageCursor cursor, int unit )
    {
        int offset = unit < 64 ? HEADER_OFFSET_START : HEADER_OFFSET_START + Long.BYTES;
        long bits = cursor.getLong( offset );
        int checkUnit = unit % 64;
        return bitIsSet( bits, checkUnit );
    }

    private static boolean bitIsSet( long bits, int bit )
    {
        return (bits & (1L << bit)) != 0;
    }

    static String rawBits( PageCursor cursor )
    {
        StringBuilder builder = new StringBuilder();

        appendBitsFromLong( builder, "START", cursor.getLong( HEADER_OFFSET_START ), cursor.getLong( HEADER_OFFSET_START + Long.BYTES ) );
        appendBitsFromLong( builder, "USED ", cursor.getLong( HEADER_OFFSET_IN_USE ), cursor.getLong( HEADER_OFFSET_IN_USE + Long.BYTES ) );

        return builder.toString();
    }

    private static void appendBitsFromLong( StringBuilder builder, String name, long bitsA, long bitsB )
    {
        builder.append( name + " " );
        for ( int i = 0; i < 64; i++ )
        {
            if ( i > 0 && i % 8 == 0 )
            {
                builder.append( " " );
            }
            builder.append( bitIsSet( bitsA, i ) ? "1" : "0" );
        }
        builder.append( "  " );
        for ( int i = 0; i < 64; i++ )
        {
            if ( i > 0 && i % 8 == 0 )
            {
                builder.append( " " );
            }
            builder.append( bitIsSet( bitsB, i ) ? "1" : "0" );
        }
        builder.append( "\n" );
    }
}
