package yo;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
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
        File storeDir = new File( args[0] );
        storeDir.mkdirs();
        long nodeCount = Long.parseLong( separatableNumber( args[1] ) );
        int labelCountPerNode = Integer.parseInt( args[2] );
        long[] labelsToAdd = new long[labelCountPerNode];
        for ( int i = 0; i < labelCountPerNode; i++ )
        {
            labelsToAdd[i] = i;
        }

        long startTime;
        long endTime;
        try ( PageCache pageCache = pageCache() )
        {
            LifeSupport life = new LifeSupport();
            NativeLabelScanStore store = life.add( new NativeLabelScanStore( pageCache, storeDir, EMPTY ) );
            life.start();

            // WRITE
            startTime = currentTimeMillis();
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
            life.shutdown();
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
}
