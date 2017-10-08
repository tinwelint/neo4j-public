/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.tooling;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Format;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.BatchFeedStep;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors;
import org.neo4j.unsafe.impl.batchimport.staging.ForkedProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.ReadRecordsStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static java.lang.Math.toIntExact;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.unsafe.impl.batchimport.RecordIdIterator.forwards;

public class IODiagnosticsTool
{
    public static void main( String[] argv ) throws Exception
    {
        Args args = Args.parse( argv );
        String type = args.get( "type" );
        File file = new File( args.get( "file" ) );
        int threads = args.getNumber( "threads", 1 ).intValue();
        AtomicLong position = new AtomicLong();
        AtomicBoolean end = new AtomicBoolean();
        startMonitor( position, end );
        long startTime = currentTimeMillis();
        switch ( type )
        {
        case "create-file":
            createFile( file, position, args );
            break;
        case "channel-read":
            channelRead( file, position, threads, args );
            break;
        case "channel-read-write":
            channelReadWrite( file, position, threads, args );
            break;
        case "channel-read-write-behind":
            channelReadWriteBehind( file, position, threads, args );
            break;
        case "pagecache-read":
            pageCacheRead( file, position, threads, args );
            break;
        case "stage-read":
            stageRead( file, position, threads, args );
            break;
        case "stage-read-write":
            stageReadWrite( file, position, threads, args );
            break;
        case "stage-read-write-flush":
            stageReadWriteFlush( file, position, threads, args );
            break;
        default:
            throw new UnsupportedOperationException( "Unknown " + type );
        }
        end.set( true );
        long duration = currentTimeMillis() - startTime;
        System.out.println( "Done in " + Format.duration( duration ) );
    }

    private static void stageRead( File file, AtomicLong position, int threads, Args args ) throws IOException
    {
        long pageCacheSize = pageCacheSize( args );
        try ( PageCache pageCache = pageCache( pageCacheSize );
                PagedFile pagedFile = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.READ );
                PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            NeoStores stores = new StoreFactory( file.getParentFile(), pageCache, new DefaultFileSystemAbstraction(),
                    NullLogProvider.getInstance() ).openNeoStores( StoreType.RELATIONSHIP );
            RelationshipStore store = stores.getRelationshipStore();
            Configuration config = new Configuration()
            {
                @Override
                public int maxNumberOfProcessors()
                {
                    return threads;
                }

                @Override
                public int batchSize()
                {
                    return store.getRecordsPerPage();
                }
            };
            Stage stage = new ReadStage( config, 0, store, position );
            ExecutionSupervisors.superviseDynamicExecution( ExecutionMonitors.defaultVisible(), config, stage );
        }
    }

    private static void stageReadWrite( File file, AtomicLong position, int threads, Args args ) throws IOException
    {
        long pageCacheSize = pageCacheSize( args );
        try ( PageCache pageCache = pageCache( pageCacheSize ) )
        {
            NeoStores stores = new StoreFactory( file.getParentFile(), pageCache, new DefaultFileSystemAbstraction(),
                    NullLogProvider.getInstance() ).openNeoStores( StoreType.RELATIONSHIP );
            RelationshipStore store = stores.getRelationshipStore();
            Configuration config = new Configuration()
            {
                @Override
                public int maxNumberOfProcessors()
                {
                    return threads;
                }

                @Override
                public int batchSize()
                {
                    return store.getRecordsPerPage();
                }
            };
            Stage stage = new ReadWriteStage( config, 0, store, position );
            ExecutionSupervisors.superviseDynamicExecution( ExecutionMonitors.defaultVisible(), config, stage );
        }
    }

    private static void stageReadWriteFlush( File file, AtomicLong position, int threads, Args args ) throws IOException
    {
        long pageCacheSize = pageCacheSize( args );
        try ( PageCache pageCache = pageCache( pageCacheSize ) )
        {
            NeoStores stores = new StoreFactory( file.getParentFile(), pageCache, new DefaultFileSystemAbstraction(),
                    NullLogProvider.getInstance() ).openNeoStores( StoreType.RELATIONSHIP );
            RelationshipStore store = stores.getRelationshipStore();
            Configuration config = new Configuration()
            {
                @Override
                public int maxNumberOfProcessors()
                {
                    return threads;
                }

                @Override
                public int batchSize()
                {
                    return store.getRecordsPerPage();
                }
            };
            Stage stage = new ReadWriteFlushStage( config, 0, store, position, pageCache );
            ExecutionSupervisors.superviseDynamicExecution( ExecutionMonitors.defaultVisible(), config, stage );
        }
    }

    private static void createFile( File file, AtomicLong position, Args args ) throws IOException
    {
        int bs = bs( args );
        long size = args.getNumber( "size", gibiBytes( 20 ) ).longValue();
        int blocks = toIntExact( size / bs );
        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
              StoreChannel channel = fs.open( file, "rw" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocateDirect( bs );
            byte[] data = new byte[bs];
            ThreadLocalRandom.current().nextBytes( data );
            buffer.put( data );
            for ( int i = 0; i < blocks; i++ )
            {
                buffer.clear();
                int read = channel.write( buffer );
                position.addAndGet( read );
            }
        }
    }

    private static int bs( Args args )
    {
        return args.getNumber( "bs", kibiBytes( 8 ) ).intValue();
    }

    private static void channelRead( File file, AtomicLong position, int threads, Args args ) throws Exception
    {
        int bs = bs( args );
        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
              StoreChannel channel = fs.open( file, "r" ) )
        {
            ExecutorService executor = Executors.newFixedThreadPool( threads );
            for ( int i = 0; i < threads; i++ )
            {
                executor.submit( new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        ByteBuffer buffer = ByteBuffer.allocateDirect( bs );
                        while ( true )
                        {
                            buffer.clear();
                            int read = channel.read( buffer, position.getAndAdd( bs ) );
                            if ( read == -1 )
                            {
                                break;
                            }
                        }
                        return null;
                    }
                } );
            }
            executor.shutdown();
            executor.awaitTermination( 1, TimeUnit.HOURS );
        }
    }

    private static void channelReadWrite( File file, AtomicLong position, int threads, Args args ) throws Exception
    {
        int bs = bs( args );
        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
              StoreChannel channel = fs.open( file, "rw" ) )
        {
            ExecutorService executor = Executors.newFixedThreadPool( threads );
            for ( int i = 0; i < threads; i++ )
            {
                executor.submit( new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        ByteBuffer buffer = ByteBuffer.allocateDirect( bs );
                        while ( true )
                        {
                            buffer.clear();
                            long pos = position.getAndAdd( bs );
                            int read = channel.read( buffer, pos );
                            if ( read == -1 )
                            {
                                break;
                            }
                            buffer.flip();
                            channel.write( buffer, pos );
                        }
                        return null;
                    }
                } );
            }
            executor.shutdown();
            executor.awaitTermination( 1, TimeUnit.HOURS );
        }
    }

    private static void randomize( int bs, ByteBuffer buffer )
    {
        byte[] bytes = new byte[bs];
        ThreadLocalRandom.current().nextBytes( bytes );
        buffer.put( bytes );
    }

    private static void channelReadWriteBehind( File file, AtomicLong position, int threads, Args args ) throws Exception
    {
        int bs = bs( args );
        int readers = threads - 1;
        AtomicInteger readersEnded = new AtomicInteger();
        AtomicBoolean writerActive = new AtomicBoolean();
        boolean writeInIsolation = args.getBoolean( "write-in-isolation" );
        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                StoreChannel readChannel = fs.open( file, "r" );
                StoreChannel writeChannel = fs.open( file, "rw" ) )
        {
            ExecutorService executor = Executors.newFixedThreadPool( threads );

            // multiple readers
            for ( int i = 0; i < readers; i++ )
            {
                executor.submit( new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        ByteBuffer buffer = ByteBuffer.allocateDirect( bs );
                        randomize( bs, buffer );
                        while ( true )
                        {
                            while ( writeInIsolation && writerActive.get() )
                            {
                                Thread.sleep( 100 );
                            }

                            buffer.clear();
                            long pos = position.getAndAdd( bs );
                            int read = readChannel.read( buffer, pos );
                            if ( read == -1 )
                            {
                                break;
                            }
                        }
                        readersEnded.incrementAndGet();
                        return null;
                    }
                } );
            }

            // single "behind" writer
            executor.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    try
                    {
                        int lastChunk = 0;
                        long chunkSize = gibiBytes( 5 );
                        ByteBuffer buffer = ByteBuffer.allocateDirect( bs );
                        randomize( bs, buffer );
                        while ( readersEnded.get() < readers )
                        {
                            lastChunk = catchUpWithWriter( position, writerActive, writeChannel, lastChunk, chunkSize, buffer );
                        }
                        lastChunk = catchUpWithWriter( position, writerActive, writeChannel, lastChunk, chunkSize, buffer );
                        return null;
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace();
                        throw e;
                    }
                }

                private int catchUpWithWriter( AtomicLong position, AtomicBoolean writerActive, StoreChannel writeChannel, int lastChunk,
                        long chunkSize, ByteBuffer buffer ) throws IOException, InterruptedException
                {
                    int chunk = (int) (position.get() / chunkSize);
                    if ( chunk > lastChunk )
                    {
                        while ( chunk > lastChunk )
                        {
                            writerActive.set( true );
                            writeChunk( writeChannel, buffer, lastChunk++, chunkSize );
                            writerActive.set( false );
                        }
                    }
                    else
                    {
                        Thread.sleep( 100 );
                    }
                    return lastChunk;
                }

                private void writeChunk( StoreChannel channel, ByteBuffer buffer, int chunk, long chunkSize ) throws IOException
                {
                    long pos = chunkSize * chunk;
                    long end = pos + chunkSize;
                    System.out.println( "writing chunk " + chunk + " " + pos + "-" + end );
                    while ( pos < end )
                    {
                        buffer.clear();
                        channel.write( buffer, pos );
                        pos += bs;
                    }
                    System.out.println( "Wrote chunk " + chunk );
                }
            } );
            executor.shutdown();
            executor.awaitTermination( 1, TimeUnit.HOURS );
        }
    }

    private static void pageCacheRead( File file, AtomicLong position, int threads, Args args ) throws Exception
    {
        long pageCacheSize = pageCacheSize( args );
        try ( PageCache pageCache = pageCache( pageCacheSize );
                PagedFile pagedFile = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.READ );
                PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            long lastPageId = pagedFile.getLastPageId();
            AtomicLong pageId = new AtomicLong();
            ExecutorService executor = Executors.newFixedThreadPool( threads );
            for ( int i = 0; i < threads; i++ )
            {
                executor.submit( new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        while ( true )
                        {
                            long pid = pageId.getAndIncrement();
                            if ( pid > lastPageId )
                            {
                                break;
                            }

                            cursor.next( pid );
                            cursor.getByte();
                            position.addAndGet( pageCache.pageSize() );
                        }
                        return null;
                    }
                } );
            }
            executor.shutdown();
            executor.awaitTermination( 1, HOURS );
        }
    }

    private static long pageCacheSize( Args args )
    {
        return args.getNumber( "pagecache-size", gibiBytes( 5 ) ).longValue();
    }

    private static PageCache pageCache( long pageCacheSize )
    {
        return new ConfiguringPageCacheFactory( new DefaultFileSystemAbstraction(),
                Config.defaults( GraphDatabaseSettings.pagecache_memory, String.valueOf( pageCacheSize ) ),
                PageCacheTracer.NULL, PageCursorTracerSupplier.NULL, NullLog.getInstance() )
                .getOrCreatePageCache();
    }

    private static void startMonitor( AtomicLong position, AtomicBoolean end )
    {
        new Thread()
        {
            @Override
            public void run()
            {
                long startTime = currentTimeMillis();
                long meg = mebiBytes( 1 );
                while ( !end.get() )
                {
                    try
                    {
                        Thread.sleep( SECONDS.toMillis( 1 ) );
                        double secs = (currentTimeMillis() - startTime) / 1000D;
                        double megs = position.longValue() / meg;
                        double megsPerSec = megs / secs;
                        System.out.println( megsPerSec + " MB/s pos:" + position );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
        }.start();
    }

    private static class ReadStage extends Stage
    {
        ReadStage( Configuration config, int orderingGuarantees, RecordStore<RelationshipRecord> store, AtomicLong position )
        {
            super( "READ", config, orderingGuarantees );
            add( new BatchFeedStep( control(), config, forwards( 0, store.getHighId(), config ), store.getRecordSize() ) );
            add( new ReadRecordsStep<>( control(), config, false, store, Predicates.alwaysTrue() ) );
            add( new SomeProcessingStep( control(), config ) );
        }
    }

    private static class ReadWriteStage extends Stage
    {
        ReadWriteStage( Configuration config, int orderingGuarantees, RecordStore<RelationshipRecord> store, AtomicLong position )
        {
            super( "READ", config, orderingGuarantees );
            add( new BatchFeedStep( control(), config, forwards( 0, store.getHighId(), config ), store.getRecordSize() ) );
            add( new ReadRecordsStep<>( control(), config, false, store, Predicates.alwaysTrue() ) );
            add( new MarkPagesAsDirtyStep( control(), config, store ) );
        }
    }

    private static class ReadWriteFlushStage extends Stage
    {
        ReadWriteFlushStage( Configuration config, int orderingGuarantees, RecordStore<RelationshipRecord> store, AtomicLong position,
                PageCache pageCache )
        {
            super( "READ", config, orderingGuarantees );
            add( new BatchFeedStep( control(), config, forwards( 0, store.getHighId(), config ), store.getRecordSize() ) );
            add( new FlusherStep( control(), config, pageCache ) );
            add( new ReadRecordsStep<>( control(), config, false, store, Predicates.alwaysTrue() ) );
            add( new MarkPagesAsDirtyStep( control(), config, store ) );
        }
    }

    private static class SomeProcessingStep extends ForkedProcessorStep<RelationshipRecord[]>
    {
        private long count;

        SomeProcessingStep( StageControl control, Configuration config )
        {
            super( control, "PROC", config );
        }

        @Override
        protected void forkedProcess( int id, int processors, RelationshipRecord[] batch ) throws Throwable
        {
            for ( RelationshipRecord relationshipRecord : batch )
            {
                if ( relationshipRecord.getFirstNode() % processors == id )
                {
                    count++;
                }
            }
        }

        @Override
        public void close() throws Exception
        {
            System.out.println( count );
            super.close();
        }
    }

    private static class EndStep extends ProcessorStep<RelationshipRecord[]>
    {
        private final AtomicLong position;
        private final int recordSize;

        protected EndStep( StageControl control, Configuration config, AtomicLong position, int recordSize )
        {
            super( control, "END", config, 1 );
            this.position = position;
            this.recordSize = recordSize;
        }

        @Override
        protected void process( RelationshipRecord[] batch, BatchSender sender ) throws Throwable
        {
            position.addAndGet( batch.length * recordSize );
        }
    }

    private static class MarkPagesAsDirtyStep extends ProcessorStep<RelationshipRecord[]>
    {
        private final RecordStore<RelationshipRecord> store;

        MarkPagesAsDirtyStep( StageControl control, Configuration config, RecordStore<RelationshipRecord> store )
        {
            super( control, "MARK", config, 0 );
            this.store = store;
        }

        @Override
        protected void process( RelationshipRecord[] batch, BatchSender sender ) throws Throwable
        {
            store.updateRecord( batch[0] );
        }
    }

    private static class FlusherStep extends ProcessorStep<PrimitiveLongIterator>
    {
        private final long pages;
        private long count;
        private final PageCache pageCache;

        protected FlusherStep( StageControl control, Configuration config, PageCache pageCache )
        {
            super( control, "FLUSHER", config, 1 );
            this.pageCache = pageCache;
            this.pages = pageCache.maxCachedPages();
        }

        @Override
        protected void process( PrimitiveLongIterator batch, BatchSender sender ) throws Throwable
        {
            count++;
            if ( count == pages )
            {
                System.out.println( "Flush" );
                pageCache.flushAndForce();
                System.out.println( "Flush END" );
                count = 0;
            }
            sender.send( batch );
        }
    }
}
