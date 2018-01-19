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

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.test.Race;
import org.neo4j.test.rule.PageCacheRule.PageCacheConfig;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertTrue;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.test.Race.throwing;
import static org.neo4j.test.rule.PageCacheRule.config;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

@RunWith( Parameterized.class )
public class SimplePropertyStoreAbstractionConcurrencyCorrectnessIT extends SimplePropertyStoreAbstractionTestBase
{
    private static final List<Pair<Value,Value>> VALUE_ALTERNATIVES = new ArrayList<>();
    private static final int KEYS = 10;
    static
    {
        VALUE_ALTERNATIVES.add( Pair.of( intValue( 10 ), intValue( 10_000 ) ) );
        VALUE_ALTERNATIVES.add( Pair.of( stringValue( "abcdefg" ), stringValue( "hijlkmnopqrstuv" ) ) );
        VALUE_ALTERNATIVES.add( Pair.of( floatValue( 0.123f ), doubleValue( 456.789d ) ) );
    }

    public SimplePropertyStoreAbstractionConcurrencyCorrectnessIT( Creator creator )
    {
        super( creator );
    }

    @Override
    protected PageCacheConfig pageCacheConfig()
    {
        return config().withInconsistentReads( true );
    }

    @Test
    public void shouldReadConsistentValues() throws Throwable
    {
        // given
        Race race = new Race().withMaxDuration( 10, SECONDS );
        AtomicLong idKeeper = new AtomicLong( initialPopulation() );
        AtomicLong reads = new AtomicLong();
        AtomicLong writes = new AtomicLong();

        // The readers
        race.addContestants( Runtime.getRuntime().availableProcessors() - 1, throwing( () ->
        {
            long id = idKeeper.get();
            if ( id != -1 )
            {
                int key = random.nextInt( KEYS );
                Value value = store.get( id, key );
                boolean matches = false;
                for ( Pair<Value,Value> alternative : VALUE_ALTERNATIVES )
                {
                    if ( value.equals( alternative.getLeft() ) || value.equals( alternative.getRight() ) )
                    {
                        matches = true;
                        break;
                    }
                }
                assertTrue( matches );
                reads.incrementAndGet();
            }
        } ) );
        // The writer
        race.addContestant( throwing( () ->
        {
            long id = idKeeper.get();
            int key = random.nextInt( KEYS );
            long newId = store.set( id, key, randomValue() );
            if ( newId != id )
            {
                idKeeper.set( newId );
            }
            writes.incrementAndGet();
            Thread.sleep( 10 );
        } ) );
        race.go();

        System.out.println( "Reads:" + reads + " Writes:" + writes );
    }

    private long initialPopulation() throws IOException
    {
        long id = -1;
        for ( int key = 0; key < KEYS; key++ )
        {
            id = store.set( id, key, randomValue() );
        }
        return id;
    }

    private Value randomValue()
    {
        Pair<Value,Value> pair = VALUE_ALTERNATIVES.get( random.nextInt( VALUE_ALTERNATIVES.size() ) );
        return random.nextBoolean() ? pair.getLeft() : pair.getRight();
    }
}
