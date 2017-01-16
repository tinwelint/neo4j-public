package yo;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.labelscan.LabelScanIndex;
import org.neo4j.kernel.api.impl.labelscan.LuceneLabelScanStore;
import org.neo4j.kernel.api.impl.labelscan.WritableDatabaseLabelScanIndex;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.count;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.api.scan.FullStoreChangeStream.EMPTY;

public class InsertIntoNativeLSSAtScale
{
    public static void main( String[] args ) throws IOException
    {
        int arg = 0;
        String name = args[arg++];
        File storeDir = new File( args[arg++] );
        storeDir.mkdirs();
        long nodeCount = Long.parseLong( separatableNumber( args[arg++] ) );
        int labelCountPerNode = Integer.parseInt( args[arg++] );
        long[] labelsToAdd = new long[labelCountPerNode];
        for ( int i = 0; i < labelCountPerNode; i++ )
        {
            labelsToAdd[i] = i;
        }

        long startTime;
        long endTime;
        try ( Store theStore = createStore( storeDir, name ) )
        {
            // WRITE
            startTime = currentTimeMillis();
            LabelScanStore store = theStore.store();
            try ( LabelScanWriter writer = store.newWriter() )
            {
                int groupSize = 100_000_000;
                long groups = nodeCount / groupSize;
                long nodeId = 0;
                for ( int g = 0; g < groups; g++ )
                {
                    long groupStartTime = currentTimeMillis();
                    for ( int i = 0; i < groupSize; i++ )
                    {
                        writer.write( labelChanges( nodeId++, EMPTY_LONG_ARRAY, labelsToAdd ) );
                    }
                    long groupEndTime = currentTimeMillis();
                    System.out.println( pretty( nodeId ) +
                            " group:" + average( groupSize, groupEndTime - groupStartTime ) +
                            " total:" + average( nodeId, groupEndTime - startTime ) );

                }
            }
            endTime = currentTimeMillis();
            System.out.println( duration( endTime - startTime ) + " to insert " + nodeCount + " nodes, each with " +
                    labelCountPerNode + " labels" );

            // READ
            try ( LabelScanReader reader = store.newReader() )
            {
                for ( int labelId = 0; labelId < labelCountPerNode; labelId++ )
                {
                    startTime = currentTimeMillis();
                    long count = count( reader.nodesWithLabel( labelId ) );
                    endTime = currentTimeMillis();
                    System.out.println( duration( endTime - startTime ) + " to read " +
                            pretty( count ) + " nodes having label " + labelId );
                }
            }

            store.force( IOLimiter.unlimited() );
        }

        long terminateTime = currentTimeMillis();
        System.out.println( "To shut down " + duration( terminateTime - endTime ) );
    }

    static PageCache pageCache()
    {
        System.out.print( "Creating page cache..." );
        try
        {
            ConfiguringPageCacheFactory pageCacheFactory =
                    new ConfiguringPageCacheFactory( new DefaultFileSystemAbstraction(), Config.defaults(),
                            PageCacheTracer.NULL, NullLog.getInstance() );
            return pageCacheFactory.getOrCreatePageCache();
        }
        finally
        {
            System.out.println( "  CREATED" );
        }
    }

    static String average( long count, long durationMillis )
    {
        long value = (long) (count / (durationMillis / 1000d));
        return pretty( value ) + " nodes/s";
    }

    static String pretty( long value )
    {
        return format( "%,d", value ).replace( " ", "," );
    }

    static String separatableNumber( String string )
    {
        return string.replaceAll( "_", "" );
    }

    static interface Store extends Closeable
    {
        LabelScanStore store();
    }

    private static Store createNativeStore( File storeDir )
    {
        PageCache pageCache = pageCache();
        LifeSupport life = new LifeSupport();
        LabelScanStore store = life.add( new NativeLabelScanStore( pageCache, storeDir, EMPTY ) );
        life.start();

        return new Store()
        {
            @Override
            public void close() throws IOException
            {
                life.shutdown();
                pageCache.close();
            }

            @Override
            public LabelScanStore store()
            {
                return store;
            }
        };
    }

    private static Store createLuceneStore( File storeDir )
    {
        LifeSupport life = new LifeSupport();
        DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        PartitionedIndexStorage storage = new PartitionedIndexStorage( directoryFactory, fs, storeDir, "lucene", false );
        LabelScanIndex index = new WritableDatabaseLabelScanIndex( BitmapDocumentFormat._64, storage );
        LabelScanStore store = life.add( new LuceneLabelScanStore( index, EMPTY, NullLogProvider.getInstance(),
                LabelScanStore.Monitor.EMPTY ) );
        life.start();

        return new Store()
        {
            @Override
            public void close() throws IOException
            {
                life.shutdown();
            }

            @Override
            public LabelScanStore store()
            {
                return store;
            }
        };
    }

    static Store createStore( File storeDir, String name )
    {
        if ( name.equals( "lucene" ) )
        {
            return createLuceneStore( storeDir );
        }
        else if ( name.equals( "native" ) )
        {
            return createNativeStore( storeDir );
        }
        throw new IllegalArgumentException( name );
    }
}
