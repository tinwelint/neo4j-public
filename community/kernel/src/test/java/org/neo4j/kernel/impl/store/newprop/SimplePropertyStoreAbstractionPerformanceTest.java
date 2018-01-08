package org.neo4j.kernel.impl.store.newprop;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.helpers.Format;
import org.neo4j.values.storable.Values;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Format.duration;

@RunWith( Parameterized.class )
public class SimplePropertyStoreAbstractionPerformanceTest extends SimplePropertyStoreAbstractionTestBase
{
    private static final int NODE_COUNT = 1_000_000;
    private static final int PROPERTY_COUNT = 10;

    public SimplePropertyStoreAbstractionPerformanceTest( Creator creator )
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
                id = store.set( id, k, Values.intValue( k ) );
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
                ids[n] = store.set( ids[n], k, Values.intValue( k ) );
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
