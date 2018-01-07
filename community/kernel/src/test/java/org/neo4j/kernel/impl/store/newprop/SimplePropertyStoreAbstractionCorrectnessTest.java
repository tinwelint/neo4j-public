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

import java.util.Arrays;

import org.neo4j.values.storable.IntValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.values.storable.Values.intValue;

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
}
