package org.neo4j.tooling;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.Args;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.csv.SnapDatasetInput;

import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.defaultVisible;

public class SnapImport
{
    public static void main( String[] args ) throws IOException
    {
        Args argss = Args.parse( args );
        File source = new File( argss.get( "source" ) );
        String reltype = argss.get( "reltype" );
        File dir = new File( argss.get( "into" ) );
        FileUtils.deleteRecursively( dir );
        Input input = new SnapDatasetInput( source, Runtime.getRuntime().availableProcessors(), reltype );

        Configuration config = new Configuration.Default()
        {
//            @Override
//            public int denseNodeThreshold()
//            {
//                return 1;
//            }
        };
        BatchImporter consumer = new ParallelBatchImporter( dir, config,
                new SimpleLogService( NullLogProvider.getInstance(), NullLogProvider.getInstance() ),
                defaultVisible(),
                Config.defaults() );
        consumer.doImport( input );
    }
}
