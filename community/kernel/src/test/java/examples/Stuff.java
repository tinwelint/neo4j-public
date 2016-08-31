package examples;

import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.collection.Iterables.count;

public class Stuff
{
    private final RelationshipType type = RelationshipType.withName( "BOUGHT" );

    @Test
    public void shouldBenchmark() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( new File( "K:\\graph.db" ) )
//                .setConfig( GraphDatabaseFacadeFactory.Configuration.tracer, "default" )
                .newGraphDatabase();
        try
        {
            for ( int i = 0; i < 10; i++ )
            {
                long time = currentTimeMillis();

                long relationships =
//                        scan( db );
                        traverse( db );

                time = currentTimeMillis() - time;
                System.out.println( ((double) relationships / time) + " rels/ms" );
            }
        }
        finally
        {
//            PageCacheTracer tracer = db.getDependencyResolver().resolveDependency( Tracers.class ).pageCacheTracer;
//            System.out.println(
//                    "pins:" + tracer.pins() + "\n" +
//                    "unpins:" + tracer.unpins() + "\n" +
//                    "evictions:" + tracer.evictions() + "\n" +
//                    "pageFaults:" + tracer.faults() );

            db.shutdown();
        }
    }

    private long scan( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long count = count( db.getAllRelationships() );
            tx.success();
            return count;
        }
    }

    private long traverse( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Traverser traversal = db.traversalDescription()
                    .breadthFirst()
//                        .uniqueness( Uniqueness.NODE_PATH )
//                        .evaluator( Evaluators.toDepth( 5 ) )
//                        .relationships( type, Direction.OUTGOING )
                    .traverse( db.getNodeById( 10 ) );
//            traversal.forEach( path -> {} );
            count( traversal );
            long relationships = traversal.metadata().getNumberOfRelationshipsTraversed();
            System.out.println( relationships );

            tx.success();
            return relationships;
        }
    }
}
