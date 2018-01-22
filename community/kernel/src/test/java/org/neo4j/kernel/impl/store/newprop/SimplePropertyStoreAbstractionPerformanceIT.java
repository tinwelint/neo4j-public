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

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.kernel.impl.store.newprop.SimplePropertyStoreAbstraction.Read;
import org.neo4j.kernel.impl.store.newprop.SimplePropertyStoreAbstraction.Write;
import org.neo4j.test.rule.RepeatRule.Repeat;
import org.neo4j.values.storable.Value;
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

    @Repeat( times = 3 )
    @Test
    public void bestCase() throws Exception
    {
        // given
        long time = currentTimeMillis();
        long[] ids = new long[NODE_COUNT];
        ProgressListener progress = progress( "Write", NODE_COUNT );
        try ( Write write = store.newWrite() )
        {
            for ( int n = 0; n < NODE_COUNT; n++ )
            {
                long id = -1;
                for ( int k = 0; k < PROPERTY_COUNT; k++ )
                {
                    id = write.set( id, k, value( k ) );
                }
                ids[n] = id;
                progress.add( 1 );
            }
        }
        long writeDuration = currentTimeMillis() - time;

        // when
        long readDuration = readAll( ids );
        long readDurationLow = readAll( ids, 0 );
        long readDurationHigh = readAll( ids, PROPERTY_COUNT - 1 );

        // then
        print( store, "best-case", writeDuration, readDuration, readDurationLow, readDurationHigh );
    }

    @Repeat( times = 3 )
    @Test
    public void worstCase() throws Exception
    {
        // given
        long time = currentTimeMillis();
        long[] ids = new long[NODE_COUNT];
        Arrays.fill( ids, -1 );
        ProgressListener progress = progress( "Write", PROPERTY_COUNT );
        try ( Write write = store.newWrite() )
        {
            for ( int k = 0; k < PROPERTY_COUNT; k++ )
            {
                for ( int n = 0; n < NODE_COUNT; n++ )
                {
                    ids[n] = write.set( ids[n], k, value( k ) );
                }
                progress.add( 1 );
            }
        }
        long writeDuration = currentTimeMillis() - time;

        // when
        long readDuration = readAll( ids );
        long readDurationLow = readAll( ids, 0 );
        long readDurationHigh = readAll( ids, PROPERTY_COUNT - 1 );

        // then
        print( store, "worst-case", writeDuration, readDuration, readDurationLow, readDurationHigh );
    }

    private long readAll( long[] ids ) throws IOException
    {
        ProgressListener progress = progress( "Read", NODE_COUNT );
        long time = currentTimeMillis();
        try ( Read read = store.newRead() )
        {
            for ( int i = 0; i < NODE_COUNT; i++ )
            {
                for ( int j = 0; j < PROPERTY_COUNT; j++ )
                {
                    read.get( ids[i], j );
                }
                progress.add( 1 );
            }
        }
        long duration = currentTimeMillis() - time;
        return duration;
    }

    private long readAll( long[] ids, int specificKey ) throws IOException
    {
        ProgressListener progress = progress( "Read", NODE_COUNT );
        long time = currentTimeMillis();
        try ( Read read = store.newRead() )
        {
            for ( int i = 0; i < NODE_COUNT; i++ )
            {
                read.get( ids[i], specificKey );
                progress.add( 1 );
            }
        }
        long duration = currentTimeMillis() - time;
        return duration;
    }

    private void print( SimplePropertyStoreAbstraction store, String name, long writeDuration, long readDuration,
            long readDurationLow, long readDurationHigh ) throws IOException
    {
        System.out.println( store.getClass().getSimpleName() + " " + name + "\n" +
                " write " + duration( writeDuration ) + "\n" +
                " read " + duration( readDuration ) + "\n" +
                " readL " + duration( readDurationLow ) + "\n" +
                " readH " + duration( readDurationHigh ) + "\n" +
                " size " + bytes( store.storeSize() ) );
    }

    private static final Value VALUE = Values.of(
//            "sskldskdaslkdlas&*$^%&*^D&f6d7f67e 6r72346r78^&*^&*^#*kd"
//            "abcde"
            12345
            );

    private Value value( int k )
    {
//        return Values.intValue( k );
        return VALUE;
    }

    private ProgressListener progress( String name, long count )
    {
//        return ProgressMonitorFactory.textual( System.out ).singlePart( name, count );
        return ProgressListener.NONE;
    }
}
