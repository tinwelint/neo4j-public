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
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.test.Race;
import org.neo4j.test.rule.PageCacheRule.PageCacheConfig;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.fail;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.test.Race.throwing;
import static org.neo4j.test.rule.PageCacheRule.config;

@RunWith( Parameterized.class )
public class SimplePropertyStoreAbstractionConcurrencyCorrectnessIT extends SimplePropertyStoreAbstractionTestBase
{
    private final int KEYS = 10;
    private final Value[] VALUE_ALTERNATIVES = new Value[KEYS * 2];

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
        setUpValues();
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
                for ( Value candidate : VALUE_ALTERNATIVES )
                {
                    if ( value.equals( candidate ) )
                    {
                        matches = true;
                        break;
                    }
                }

                if ( !matches )
                {
                    fail( "Read value " + value + " doesn't match any of " + Arrays.toString( VALUE_ALTERNATIVES ) );
                }
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

            // Wait some time now and then to allow readers to get some breathing room
            if ( currentTimeMillis() % 100 == 0 )
            {
                Thread.sleep( 1 );
            }
        } ) );
        race.go();

        System.out.println( "Reads:" + reads + " Writes:" + writes );
    }

    private void setUpValues()
    {
        for ( int i = 0; i < KEYS * 2; i++ )
        {
            VALUE_ALTERNATIVES[i] = Values.of( random.propertyValue() );
        }
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
        return random.among( VALUE_ALTERNATIVES );
    }
}
