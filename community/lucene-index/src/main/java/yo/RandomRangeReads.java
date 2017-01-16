package yo;

import yo.InsertIntoNativeLSSAtScale.Store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.kernel.impl.index.labelscan.LabelScanKey;
import org.neo4j.kernel.impl.index.labelscan.LabelScanValue;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import static yo.InsertIntoNativeLSSAtScale.createStore;
import static yo.InsertIntoNativeLSSAtScale.separatableNumber;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RandomRangeReads
{
    public static void main( String[] args ) throws Exception
    {
        int arg = 0;
        String name = args[arg++];
        File storeDir = new File( args[arg++] );
        int rangeSize = Integer.parseInt( separatableNumber( args[arg++] ) );
        int seconds = Integer.parseInt( separatableNumber( args[arg++] ) );

        try ( Store theStore = createStore( storeDir, name ) )
        {
            System.out.print( "Starting LSS..." );
            NativeLabelScanStore lss = (NativeLabelScanStore) theStore.store();
            System.out.println( "  STARTED" );
            GBPTree<LabelScanKey,LabelScanValue> tree = lss.getTree();
            long highId;
            System.out.print( "Getting high id..." );
            try ( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> seeker =
                    tree.seek( new LabelScanKey( 0, Long.MAX_VALUE ), new LabelScanKey( 0, 0 ) ) )
            {
                seeker.next();
                highId = seeker.get().key().idRange - rangeSize * 2;
            }
            System.out.println( "  GOT " + highId );

            int threads = Runtime.getRuntime().availableProcessors();
            ExecutorService executorService = newFixedThreadPool( threads );
            long endTime = currentTimeMillis() + SECONDS.toMillis( seconds );
            AtomicLong totalCount = new AtomicLong();
            AtomicLong totalQueries = new AtomicLong();
            List<Future<?>> futures = new ArrayList<>();
            for ( int i = 0; i < threads; i++ )
            {
                futures.add( executorService.submit( () ->
                {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    LabelScanKey from = new LabelScanKey();
                    LabelScanKey to = new LabelScanKey();
                    long localCount = 0;
                    long localQueries = 0;
                    while ( currentTimeMillis() < endTime )
                    {
                        long fromLong = random.nextLong( highId );
                        from.set( 0, fromLong );
                        to.set( 0, fromLong + rangeSize );
                        try ( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> seeker = tree.seek( from, to ) )
                        {
                            while ( seeker.next() )
                            {
                                localCount++;
                            }
                            localQueries++;
                        }
                    }
                    totalCount.addAndGet( localCount );
                    totalQueries.addAndGet( localQueries );
                    return null;
                } ) );
            }

            System.out.print( "Running load..." );
            for ( Future<?> future : futures )
            {
                future.get( seconds * 2, SECONDS );
            }
            System.out.println( "  DONE" );

            System.out.println( "Shutting down" );
            executorService.shutdown();

            // Printing time
            System.out.println( "queries:" + totalQueries + ", hits:" + totalCount +
                    ", queries/s:" + totalQueries.get() / (double) seconds +
                    ", hits/s:" + totalCount.get() / (double) seconds );
        }
    }
}
