package org.neo4j.kernel.impl.api.index;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class IndexCRUDIT
{

    private GraphDatabaseAPI db;
    private TestGraphDatabaseFactory factory;
    private final EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private final SchemaIndexProvider mockedIndexProvider = mock(SchemaIndexProvider.class);
    private final GatheringIndexWriter writer = new GatheringIndexWriter();
    private ThreadToStatementContextBridge ctxProvider;

    @Test
    public void addingANodeWithPropertyShouldGetIndexed() throws Exception
    {
        // Given
        Label myLabel = label( "MYLABEL" );
        String indexProperty = "indexProperty";
        createIndex( myLabel, indexProperty );

        // When
        Node node = createNode( map( indexProperty, 12, "otherProperty", 17 ), myLabel );

        // Then, for now, this should trigger two NodePropertyUpdates
        assertThat(writer.updates, equalTo( asSet(
                new NodePropertyUpdate( node.getId(), 1l, null, 12 ),
                new NodePropertyUpdate( node.getId(), 1l, null, 12 )) ));

        // We get two updates because we both add a label and a property to be indexed
        // in the same transaction, in the future, we should optimize this down to
        // one NodePropertyUpdate.
    }

    @Test
    public void addingALabelToPreExistingNodeShouldGetIndexed() throws Exception
    {
        // GIVEN
        Label myLabel = label( "MYLABEL" );
        String indexProperty = "indexProperty";
        createIndex( myLabel, indexProperty );

        // WHEN
        Node node = createNode( map( indexProperty, 12, "otherProperty", 17 ) );

        // THEN
        assertThat(writer.updates.size(), equalTo( 0 ));

        // AND WHEN
        Transaction tx = db.beginTx();
        node.addLabel( myLabel );
        tx.success();
        tx.finish();

        // THEN
        assertThat( writer.updates, equalTo( asSet( new NodePropertyUpdate( node.getId(), 1l, null, 12) ) ) );
    }

    private long labelId( Label label ) throws LabelNotFoundKernelException
    {
        return ctxProvider.getCtxForReading().getLabelId( label.name() );
    }

    private Node createNode( Map<String, Object> properties, Label ... labels )
    {
        Transaction tx = db.beginTx();
        Node node = db.createNode( labels );
        for ( Map.Entry<String, Object> prop : properties.entrySet() )
        {
            node.setProperty( prop.getKey(), prop.getValue() );
        }
        tx.success();
        tx.finish();
        return node;
    }

    private void createIndex( Label myLabel, String indexProperty )
    {
        Transaction tx = db.beginTx();
        db.schema().indexCreator( myLabel ).on( indexProperty ).create();
        tx.success();
        tx.finish();
    }

    @Before
    public void before() throws Exception
    {
        factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs );
        factory.setSchemaIndexProviders( Arrays.asList( mockedIndexProvider ) );
        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        ctxProvider = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );

        when(mockedIndexProvider.getPopulator( anyLong() )).thenReturn( writer );
        when(mockedIndexProvider.getWriter( anyLong() )).thenReturn( writer );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }


    private static class GatheringIndexWriter extends IndexWriter.Adapter implements IndexPopulator
    {
        private final Set<NodePropertyUpdate> updates = new HashSet<NodePropertyUpdate>();

        @Override
        public void createIndex()
        {
        }

        @Override
        public void add( long nodeId, Object propertyValue )
        {
            updates.add( new NodePropertyUpdate( nodeId, 0l, null, propertyValue ) );
        }

        @Override
        public void update( Iterator<NodePropertyUpdate> updates )
        {
            this.updates.addAll( asCollection( asIterable( updates ) ) );
        }

        @Override
        public void populationCompleted()
        {
        }
    }

}
