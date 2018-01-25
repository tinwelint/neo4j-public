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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.kernel.impl.store.newprop.SimplePropertyStoreAbstraction.Read;
import org.neo4j.kernel.impl.store.newprop.SimplePropertyStoreAbstraction.Write;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.test.Randoms.CSA_LETTERS_AND_DIGITS;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.Values.stringValue;

@RunWith( Parameterized.class )
public class SimplePropertyStoreAbstractionCorrectnessTest extends SimplePropertyStoreAbstractionTestBase
{
    public SimplePropertyStoreAbstractionCorrectnessTest( Creator creator )
    {
        super( creator );
    }

    @Test
    public void shouldSetOneProperty() throws Exception
    {
        // GIVEN
        int key = 0;
        IntValue value = intValue( 10 );

        // WHEN
        long id;
        try ( Write access = store.newWrite() )
        {
            id = access.set( -1, key, value );
        }

        try ( Read access = store.newRead() )
        {
            // THEN
            assertTrue( access.has( id, key ) );
            assertEquals( value, access.get( id, key ) );
        }
    }

    @Test
    public void shouldSetTwoProperties() throws Exception
    {
        // GIVEN
        int key1 = 0, key2 = 1;
        IntValue value1 = intValue( 10 );
        IntValue value2 = intValue( 1_000_000_000 );

        // WHEN
        long id;
        try ( Write access = store.newWrite() )
        {
            id = access.set( -1, key1, value1 );
            id = access.set( id, key2, value2 );
        }

         // THEN
        try ( Read access = store.newRead() )
        {
            assertTrue( access.has( id, key1 ) );
            assertEquals( value1, access.get( id, key1 ) );

            assertTrue( access.has( id, key2 ) );
            assertEquals( value2, access.get( id, key2 ) );
        }
    }

    @Test
    public void shouldSetManyProperties() throws Exception
    {
        // WHEN
        long id = -1;
        for ( int i = 0; i < 100; i++ )
        {
            try ( Write access = store.newWrite() )
            {
                id = access.set( id, i, intValue( i ) );
            }

            // THEN
            try ( Read access = store.newRead() )
            {
                for ( int j = 0; j <= i; j++ )
                {
                    assertTrue( "Key " + j, access.has( id, j ) );
                    assertEquals( "Key " + j, intValue( j ), access.get( id, j ) );
                }
            }
        }
    }

    @Test
    public void shouldRemoveProperty() throws Exception
    {
        // GIVEN
        long id = -1;
        try ( Write access = store.newWrite() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                id = access.set( id, i, intValue( i ) );
            }

            // WHEN
            access.remove( id, 1 );
        }

        // THEN
        try ( Read access = store.newRead() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                assertEquals( "" + i, i != 1, access.has( id, i ) );
            }
        }
    }

    @Test
    public void shouldSetPropertiesForMultipleNodes() throws Exception
    {
        // given
        int nodeCount = 20;
        int keyCount = 10;
        long[] ids = new long[nodeCount];
        Arrays.fill( ids, -1 );

        // when
        try ( Write access = store.newWrite() )
        {
            for ( int n = 0; n < nodeCount; n++ )
            {
                for ( int k = 0; k < keyCount; k++ )
                {
                    ids[n] = access.set( ids[n], k, intValue( k ) );
                }
            }
        }

        // then
        try ( Read access = store.newRead() )
        {
            for ( int n = 0; n < nodeCount; n++ )
            {
                for ( int k = 0; k < keyCount; k++ )
                {
                    assertEquals( intValue( k ), access.get( ids[n], k ) );
                }
            }
        }
    }

    @Test
    public void shouldSetStringProperties() throws Exception
    {
        // given
        TextValue value = stringValue( "New property store pwns" );

        // when
        long id;
        try ( Write access = store.newWrite() )
        {
            id = access.set( -1, 0, value );
        }

        // then
        try ( Read access = store.newRead() )
        {
            assertEquals( value, access.get( id, 0 ) );
        }
    }

    @Test
    public void shouldSetManyStrings() throws Exception
    {
        // given
        long id = -1;
        Map<Integer,Value> expected = new HashMap<>();

        // when
        try ( Write access = store.newWrite() )
        {
            for ( int key = 0; key < 150; key++ )
            {
                Value value = Values.of( random.string( 5, 15, CSA_LETTERS_AND_DIGITS ) );
                id = access.set( id, key, value );
                expected.put( key, value );
            }
        }

        // then
        try ( Read access = store.newRead() )
        {
            for ( int key : expected.keySet() )
            {
                assertEquals( expected.get( key ), access.get( id, key ) );
            }
        }
    }

    @Test
    public void shouldChangeIntPropertyValueOfEqualSize() throws Exception
    {
        shouldChangeProperty( intValue( 100 ), intValue( 105 ) );
    }

    private void shouldChangeProperty( Value value, Value newValue ) throws IOException
    {
        // given
        Value otherValue = stringValue( "sdkfhdk" );
        long id;
        try ( Write access = store.newWrite() )
        {
            id = access.set( -1, 0, value );
            id = access.set( id, 1, otherValue );

            // when
            id = access.set( id, 0, newValue );
        }

        // then
        try ( Read access = store.newRead() )
        {
            assertEquals( newValue, access.get( id, 0 ) );
            assertEquals( otherValue, access.get( id, 1 ) );
        }
    }

    @Test
    public void shouldChangeIntPropertyValueOfLargerSize() throws Exception
    {
        shouldChangeProperty( intValue( 100 ), intValue( 1234567 ) );
    }

    @Test
    public void shouldChangeIntPropertyValueOfSmallerSize() throws Exception
    {
        shouldChangeProperty( intValue( 1234567 ), intValue( 100 ) );
    }

    @Test
    public void shouldChangeStringPropertyValueOfEqualSize() throws Exception
    {
        shouldChangeProperty( stringValue( "abcdefg" ), stringValue( "hijklmn" ) );
    }

    @Test
    public void shouldChangeStringPropertyValueToLargerSize() throws Exception
    {
        shouldChangeProperty( stringValue( "abcdefg" ), stringValue( "hijklmnopqrstuvwxyz" ) );
    }

    @Test
    public void shouldChangeStringPropertyValueToSmallerSize() throws Exception
    {
        shouldChangeProperty( stringValue( "abcdefg" ), stringValue( "hij" ) );
    }

    @Test
    public void shouldChangeIntToStringProperty() throws Exception
    {
        shouldChangeProperty( intValue( 10 ), stringValue( "abcdefg" ) );
    }

    @Test
    public void shouldChangeStringToIntProperty() throws Exception
    {
        shouldChangeProperty( stringValue( "abcdefg" ), intValue( 10 ) );
    }

    @Test
    public void shouldGrowWhenChangingProperty() throws Exception
    {
        shouldChangeProperty( stringValue( "abc" ), stringValue( random.string( 3_000, 3_000, CSA_LETTERS_AND_DIGITS ) ) );
    }

    @Test
    public void shouldStoreStringArray() throws Exception
    {
        shouldSetAndGetValue( Values.of( new String[] {"abc", "defg", "hi"} ) );
    }

    @Test
    public void shouldStoreDoubleArray() throws Exception
    {
        shouldSetAndGetValue( Values.of( new double[] {0.10171606200504502D, 0.4423817012294845D} ) );
    }

    @Test
    public void shouldStoreBooleanArray() throws Exception
    {
        shouldSetAndGetValue( Values.of( new boolean[] {false, false, false, true, true, false} ) );
        shouldSetAndGetValue( Values.of( new boolean[] {false, false, false, true, true, false, true, true} ) );
    }

    @Test
    public void shouldSetNegativeLongValue() throws Exception
    {
        shouldSetAndGetValue( longValue( -7262393586424567340L ) );
    }

    @Test
    public void shouldSetShortValue() throws Exception
    {
        shouldSetAndGetValue( shortValue( (short) 246 ) );
    }

    @Test
    public void shouldSetNegativeShortValue() throws Exception
    {
        shouldSetAndGetValue( shortValue( (short) -31491 ) );
    }

    private void shouldSetAndGetValue( Value value ) throws IOException
    {
        // when
        long id;
        try ( Write access = store.newWrite() )
        {
            id = access.set( -1, 0, value );
        }

        // then
        try ( Read access = store.newRead() )
        {
            Value readValue = access.get( id, 0 );
            assertEquals( value, readValue );
        }
    }

    @Test
    public void shouldSetAndRemoveRandomProperties() throws Exception
    {
        // given
        long id = -1;
        PrimitiveIntObjectMap<Value> expected = Primitive.intObjectMap();
        int updates = 500;
        int maxKeys = 100;

        // when/then
        for ( int i = 0; i < updates; i++ )
        {
            try ( Write access = store.newWrite() )
            {
                int key = random.nextInt( maxKeys );
                if ( random.nextFloat() < 0.7 )
                {   // Set
                    Value value = Values.of( random.propertyValue() );
                    id = access.set( id, key, value );
                    expected.put( key, value );
//                    System.out.println( "Set " + key + " " + value );
                }
                else if ( !expected.isEmpty() )
                {   // Remove
                    while ( !expected.containsKey( key ) )
                    {
                        key = random.nextInt( maxKeys );
                    }
                    assertEquals( expected.remove( key ), access.get( id, key ) );
                    id = access.remove( id, key );
//                    System.out.println( "Remove " + key );
                    assertFalse( access.has( id, key ) );
                }
            }

            try ( Read access = store.newRead() )
            {
                for ( int candidateKey = 0; candidateKey < maxKeys; candidateKey++ )
                {
                    Value value = expected.get( candidateKey );
                    if ( value != null )
                    {
                        assertTrue( access.has( id, candidateKey ) );
                        Value readValue = access.get( id, candidateKey );
                        assertEquals( "For key " + candidateKey, value, readValue );
                    }
                    else
                    {
                        assertFalse( "For key " + candidateKey, access.has( id, candidateKey ) );
                    }
                }
            }
        }
    }
}
