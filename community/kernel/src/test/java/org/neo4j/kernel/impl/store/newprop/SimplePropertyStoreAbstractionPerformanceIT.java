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

import org.neo4j.helpers.Format;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Format.duration;

@RunWith( Parameterized.class )
public class SimplePropertyStoreAbstractionPerformanceIT extends SimplePropertyStoreAbstractionTestBase
{
    private static final int NODE_COUNT = 1_000_000;
    private static final int PROPERTY_COUNT = 10;

    public SimplePropertyStoreAbstractionPerformanceIT( Creator creator )
    {
        super( creator );
    }

    @Test
    public void bestCase() throws Exception
    {
        // given
        long time = currentTimeMillis();
        long[] ids = new long[NODE_COUNT];
        for ( int n = 0; n < NODE_COUNT; n++ )
        {
            long id = -1;
            for ( int k = 0; k < PROPERTY_COUNT; k++ )
            {
                id = store.set( id, k, value( k ) );
            }
            ids[n] = id;
        }
        long createDuration = currentTimeMillis() - time;

        // when
        long duration = readAll( ids );

        // then
        System.out.println( store + " best-case " + Format.duration( duration ) + " size " + bytes( store.storeSize() ) +
                " " + duration( createDuration ) + " write" );
    }

    private TextValue value( int k )
    {
        return Values.stringValue( "" + k );
    }

    @Test
    public void worstCase() throws Exception
    {
        // given
        long time = currentTimeMillis();
        long[] ids = new long[NODE_COUNT];
        Arrays.fill( ids, -1 );
        for ( int k = 0; k < PROPERTY_COUNT; k++ )
        {
            for ( int n = 0; n < NODE_COUNT; n++ )
            {
                ids[n] = store.set( ids[n], k, value( k ) );
            }
        }
        long writeDuration = currentTimeMillis() - time;

        // when
        long duration = readAll( ids );

        // then
        System.out.println( store + " worst-case " + Format.duration( duration ) + " size " + bytes( store.storeSize() ) +
                " " + duration( writeDuration ) + " write");
    }

    private long readAll( long[] ids ) throws IOException
    {
        long time = currentTimeMillis();
        for ( int i = 0; i < NODE_COUNT; i++ )
        {
            for ( int j = 0; j < PROPERTY_COUNT; j++ )
            {
                store.getWithoutDeserializing( ids[i], j );
            }
        }
        long duration = currentTimeMillis() - time;
        return duration;
    }
}
