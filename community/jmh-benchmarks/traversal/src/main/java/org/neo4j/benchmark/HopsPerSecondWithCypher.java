/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.util.SnapImport;

@State( Scope.Benchmark )
@Measurement( time = 2, timeUnit = TimeUnit.MINUTES, iterations = 10 )
@Warmup( iterations = 10 )
@BenchmarkMode( value = Mode.Throughput )
@Fork( 1 )
public class HopsPerSecondWithCypher
{
    private GraphDatabaseAPI db;
    private int nodeCount;
    private long ignored;
    private final LongAdder adder = new LongAdder();

    @Setup
    public void setUp() throws IOException, URISyntaxException
    {
        // Setup store
        final File storeDir = new File( "graph.db" );
        if ( !storeDir.exists() )
        {
            buildStore( storeDir );
        }
        db = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .newGraphDatabase();

        // Count number of nodes
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node ignored : db.getAllNodes() )
            {
                nodeCount++;
            }
            tx.success();
        }

        ignored = 1;
        startTimeKeeper();
    }

    @TearDown
    public void tearDown()
    {
        System.out.println( "Finishing...." );
        db.shutdown();
    }

    private void startTimeKeeper()
    {
        // Will print number of hops per second in 10 second intervals
        Thread timeKeeper = new Thread()
        {
            @Override
            public void run()
            {
                long start = System.nanoTime();

                while ( true )
                {
                    final long now = System.nanoTime();
                    final long duration = now - start;
                    if ( duration >= TimeUnit.SECONDS.toNanos( 10 ) )
                    {
                        System.out.printf( "%d hops/s\n",
                                adder.sum() / TimeUnit.NANOSECONDS.toSeconds( duration ) );
                        adder.reset();
                        start = now;
                    }
                    LockSupport.parkNanos( TimeUnit.SECONDS.toNanos( 10 ) );
                }
            }
        };
        timeKeeper.start();
    }

    private void buildStore( File storeDir ) throws IOException
    {
        final Path amazon0302 = Files.createTempFile( "amazon0302", ".txt" );
        try ( InputStream amazonResourceStream = getClass().getClassLoader()
                .getResourceAsStream( "amazon0302.txt" ) )
        {
            Files.copy( amazonResourceStream, amazon0302, StandardCopyOption.REPLACE_EXISTING );
        }


        SnapImport.main( new String[]{
                "--into", storeDir.getAbsolutePath(),
                "--source", amazon0302.toAbsolutePath().toString(),
                "--reltype", "BOUGHT" } );
    }

    @Benchmark
    public long shouldBenchmarkCypher( Blackhole blackhole ) throws Exception
    {
        long hops = 0;
        while ( hops >= 0 )
        {
            hops = traverseWithCypher( db, getRandomNode() );
            adder.add( hops );
        }
        blackhole.consume( hops );
        return ignored;
    }

    private long traverseWithCypher( GraphDatabaseService db, long id )
    {
        try ( Transaction tx = db.beginTx() )
        {
            String query = "MATCH (you)-[:BOUGHT]->(something)<-[:BOUGHT]-(other)-[:BOUGHT]->(somethingElse)\n" +
                           "WHERE id(you)={id}\n" +
                           "return count(somethingElse) as result";
            final Result result = db.execute( query, MapUtil.genericMap( "id", id ) );
            tx.success();
            final ResourceIterator<Object> iterator = result.columnAs( "result" );
            return (iterator.hasNext() ? (Number) iterator.next() : 0).longValue();
        }
    }

    private int getRandomNode()
    {
        return ThreadLocalRandom.current().nextInt( nodeCount );
    }
}
