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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.ArrayUtil.contains;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;

public class TypeTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldSelectMaxInt8TypeIfFitsRegardlessOfSignAndJavaType() throws Exception
    {
        shouldSelectCorrectIntegralTypeIfFitsRegardlessOfSignAndJavaType( Byte.MIN_VALUE, Byte.MAX_VALUE, Type.INT8 );
    }

    @Test
    public void shouldSelectMaxInt16TypeIfFitsRegardlessOfSignAndJavaType() throws Exception
    {
        shouldSelectCorrectIntegralTypeIfFitsRegardlessOfSignAndJavaType( Short.MIN_VALUE, Short.MAX_VALUE, Type.INT8, Type.INT16 );
    }

    @Test
    public void shouldSelectMaxInt32TypeIfFitsRegardlessOfSignAndJavaType() throws Exception
    {
        shouldSelectCorrectIntegralTypeIfFitsRegardlessOfSignAndJavaType( Integer.MIN_VALUE, Integer.MAX_VALUE, Type.INT8, Type.INT16, Type.INT32 );
    }

    @Test
    public void shouldSelectMaxInt64TypeIfFitsRegardlessOfSignAndJavaType() throws Exception
    {
        shouldSelectCorrectIntegralTypeIfFitsRegardlessOfSignAndJavaType( Long.MIN_VALUE, Long.MAX_VALUE, Type.INT8, Type.INT16, Type.INT32, Type.INT64 );
    }

    private void shouldSelectCorrectIntegralTypeIfFitsRegardlessOfSignAndJavaType( long min, long max, Type... anyOfTypes ) throws Exception
    {
        for ( int i = 0; i < 1_000; i++ )
        {
            // when
            Value integralValue = randomIntegralValueOfSize( min, max );
            Type type = Type.fromValue( integralValue );

            // then
            assertTrue( type.toString(), contains( anyOfTypes, type ) );
        }
    }

    private void shouldSelectCorrectType( Value value, Type expectedType ) throws Exception
    {
        // when
        Type type = Type.fromValue( value );

        // then
        assertEquals( expectedType, type );
    }

    private Value randomIntegralValueOfSize( long min, long max )
    {
        long raw = min == Long.MIN_VALUE && max == Long.MAX_VALUE ? random.nextLong() : random.nextLong( min, max );
        switch ( random.nextInt( 4 ) )
        {
        case 0: return byteValue( (byte) raw );
        case 1: return shortValue( (short) raw );
        case 2: return intValue( (int) raw );
        case 3: return longValue( raw );
        default: throw new IllegalArgumentException( "Should not happend" );
        }
    }
}
