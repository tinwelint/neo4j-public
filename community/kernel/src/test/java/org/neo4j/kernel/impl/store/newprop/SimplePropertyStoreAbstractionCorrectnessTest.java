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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RandomRule.Seed;
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
    @Rule
    public final RandomRule random = new RandomRule();

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
        long id = store.set( -1, key, value );

        // THEN
        assertTrue( store.has( id, key ) );
        assertEquals( value, store.get( id, key ) );
    }

    @Test
    public void shouldSetTwoProperties() throws Exception
    {
        // GIVEN
        int key1 = 0, key2 = 1;
        IntValue value1 = intValue( 10 );
        IntValue value2 = intValue( 1_000_000_000 );

        // WHEN
        long id = store.set( -1, key1, value1 );
        id = store.set( id, key2, value2 );

         // THEN
        assertTrue( store.has( id, key1 ) );
        assertEquals( value1, store.get( id, key1 ) );

        assertTrue( store.has( id, key2 ) );
        assertEquals( value2, store.get( id, key2 ) );
    }

    @Test
    public void shouldSetManyProperties() throws Exception
    {
        // WHEN
        long id = -1;
        for ( int i = 0; i < 100; i++ )
        {
            id = store.set( id, i, intValue( i ) );
        }

        // THEN
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( store.has( id, i ) );
            assertEquals( intValue( i ), store.get( id, i ) );
        }
    }

    @Test
    public void shouldRemoveProperty() throws Exception
    {
        // GIVEN
        long id = -1;
        for ( int i = 0; i < 10; i++ )
        {
            id = store.set( id, i, intValue( i ) );
        }

        // WHEN
        store.remove( id, 1 );

        // THEN
        for ( int i = 0; i < 10; i++ )
        {
            assertEquals( "" + i, i != 1, store.has( id, i ) );
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
        for ( int n = 0; n < nodeCount; n++ )
        {
            for ( int k = 0; k < keyCount; k++ )
            {
                ids[n] = store.set( ids[n], k, intValue( k ) );
            }
        }

        // then
        for ( int n = 0; n < nodeCount; n++ )
        {
            for ( int k = 0; k < keyCount; k++ )
            {
                assertEquals( intValue( k ), store.get( ids[n], k ) );
            }
        }
    }

    @Test
    public void shouldSetStringProperties() throws Exception
    {
        // given
        TextValue value = stringValue( "New property store pwns" );

        // when
        long id = store.set( -1, 0, value );

        // then
        assertEquals( value, store.get( id, 0 ) );
    }

    @Test
    public void shouldSetManyStrings() throws Exception
    {
        // given
        long id = -1;
        Map<Integer,Value> expected = new HashMap<>();

        // when
        for ( int key = 0; key < 150; key++ )
        {
            Value value = Values.of( random.string( 5, 15, CSA_LETTERS_AND_DIGITS ) );
            id = store.set( id, key, value );
            expected.put( key, value );
        }

        // then
        for ( int key : expected.keySet() )
        {
            assertEquals( expected.get( key ), store.get( id, key ) );
        }
    }

    @Test
    public void shouldChangeIntPropertyValueOfEqualSize() throws Exception
    {
        // given
        Value value = intValue( 100 );
        long id = store.set( -1, 0, value );

        // when
        Value newValue = intValue( 105 );
        store.set( id, 0, newValue );

        // then
        assertEquals( newValue, store.get( id, 0 ) );
    }

    @Test
    public void shouldChangeIntPropertyValueOfLargerSize() throws Exception
    {
        // given
        Value value = intValue( 100 );
        long id = store.set( -1, 0, value );
        Value otherValue = stringValue( "abcdefgh" );
        id = store.set( id, 1, otherValue );

        // when
        Value newValue = intValue( 1234567 );
        store.set( id, 0, newValue );

        // then
        assertEquals( newValue, store.get( id, 0 ) );
        assertEquals( otherValue, store.get( id, 1 ) );
    }

    @Test
    public void shouldChangeIntPropertyValueOfSmallerSize() throws Exception
    {
        // given
        Value value = intValue( 1234567 );
        long id = store.set( -1, 0, value );
        Value otherValue = stringValue( "abcdefgh" );
        id = store.set( id, 1, otherValue );

        // when
        Value newValue = intValue( 100 );
        store.set( id, 0, newValue );

        // then
        assertEquals( newValue, store.get( id, 0 ) );
        assertEquals( otherValue, store.get( id, 1 ) );
    }

    @Test
    public void shouldChangeStringPropertyValueOfEqualSize() throws Exception
    {
     // given
        Value value = stringValue( "abcdefg" );
        long id = store.set( -1, 0, value );

        // when
        Value newValue = stringValue( "hijklmn" );
        store.set( id, 0, newValue );

        // then
        assertEquals( newValue, store.get( id, 0 ) );
    }

    @Test
    public void shouldChangeStringPropertyValueToLargerSize() throws Exception
    {
        // given
        Value value = stringValue( "abcdefg" );
        Value otherValue = stringValue( "LAST" );
        long id = store.set( -1, 0, value );
        id = store.set( id, 1, otherValue );

        // when
        Value newValue = stringValue( "hijklmnopqrstuvwxyz" );
        store.set( id, 0, newValue );

        // then
        assertEquals( newValue, store.get( id, 0 ) );
        assertEquals( otherValue, store.get( id, 1 ) );
    }

    @Test
    public void shouldChangeStringPropertyValueToSmallerSize() throws Exception
    {
        // given
        Value value = stringValue( "abcdefg" );
        Value otherValue = stringValue( "LAST" );
        long id = store.set( -1, 0, value );
        id = store.set( id, 1, otherValue );

        // when
        Value newValue = stringValue( "hij" );
        store.set( id, 0, newValue );

        // then
        assertEquals( newValue, store.get( id, 0 ) );
        assertEquals( otherValue, store.get( id, 1 ) );
    }

    @Test
    public void shouldChangeIntToStringProperty() throws Exception
    {
        // given
        Value value = intValue( 10 );
        Value otherValue = stringValue( "LAST" );
        long id = store.set( -1, 0, value );
        id = store.set( id, 1, otherValue );

        // when
        Value newValue = stringValue( "abcdefg" );
        store.set( id, 0, newValue );

        // then
        assertEquals( newValue, store.get( id, 0 ) );
        assertEquals( otherValue, store.get( id, 1 ) );
    }

    @Test
    public void shouldChangeStringToIntProperty() throws Exception
    {
        // given
        Value value = stringValue( "abcdefg" );
        Value otherValue = stringValue( "LAST" );
        long id = store.set( -1, 0, value );
        id = store.set( id, 1, otherValue );

        // when
        Value newValue = intValue( 10 );
        store.set( id, 0, newValue );

        // then
        assertEquals( newValue, store.get( id, 0 ) );
        assertEquals( otherValue, store.get( id, 1 ) );
    }

    @Test
    public void shouldGrowWhenChangingProperty() throws Exception
    {
        // given
        Value value = stringValue( "abc" );
        Value otherValue = stringValue( "LAST" );
        long id = store.set( -1, 0, value );
        id = store.set( id, 1, otherValue );

        // when
        Value newValue = stringValue( random.string( 3_000, 3_000, CSA_LETTERS_AND_DIGITS ) );
        id = store.set( id, 0, newValue );

        // then
        assertEquals( newValue, store.get( id, 0 ) );
        assertEquals( otherValue, store.get( id, 1 ) );
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
        // given
        long id = store.set( -1, 0, value );

        // when
        Value readValue = store.get( id, 0 );

        // then
        assertEquals( value, readValue );
    }

//    @Repeat( times = 100 )
    @Seed( 1516270823605L )
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
            int key = random.nextInt( maxKeys );
            if ( random.nextFloat() < 0.7 )
            {   // Set
                Value value = Values.of( random.propertyValue() );
                System.out.println( "Set " + key + " = " + value );
                id = store.set( id, key, value );
                expected.put( key, value );
            }
            else if ( !expected.isEmpty() )
            {   // Remove
                while ( !expected.containsKey( key ) )
                {
                    key = random.nextInt( maxKeys );
                }
                System.out.println( "Remove " + key );
                assertEquals( expected.remove( key ), store.get( id, key ) );
                id = store.remove( id, key );
                assertFalse( store.has( id, key ) );
            }

            for ( int candidateKey = 0; candidateKey < maxKeys; candidateKey++ )
            {
                Value value = expected.get( candidateKey );
                if ( value != null )
                {
                    assertTrue( store.has( id, candidateKey ) );
                    Value readValue = store.get( id, candidateKey );
                    assertEquals( "For key " + candidateKey, value, readValue );
                }
                else
                {
                    assertFalse( "For key " + candidateKey, store.has( id, candidateKey ) );
                }
            }
        }
    }
}
