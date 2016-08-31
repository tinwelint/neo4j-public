package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.junit.Test;

import java.io.File;

import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import static org.neo4j.helpers.collection.Iterators.count;

public class SnapDatasetInputTest
{
    @Test
    public void shouldGetSnapData() throws Exception
    {
        // GIVEN
        File homeDirectory = new File( System.getProperty( "user.home" ) );
        File desktopDirectory = new File( homeDirectory, "Desktop" );
        Input input = new SnapDatasetInput( new File( desktopDirectory, "Amazon0302.txt" ),
                Runtime.getRuntime().availableProcessors(), "BOUGHT" );

        // WHEN
        long nodeCount;
        long relationshipCount;
        Number previousId = null;
        int nodesSkipped = 0;
        try ( InputIterator<InputNode> nodes = input.nodes().iterator() )
        {
            nodeCount = 0;
            while ( nodes.hasNext() )
            {
                Number id = (Number) nodes.next().id();
                if ( previousId != null && id.longValue() - previousId.longValue() > 1 )
                {
                    System.out.println( "skipped betweel " + previousId + " and " + id );
                    nodesSkipped += (id.longValue() - previousId.longValue()) - 1;
                }
                previousId = id;
                nodeCount++;
            }
        }
        try ( InputIterator<InputRelationship> relationships = input.relationships().iterator() )
        {
            relationshipCount = count( relationships );
        }

        System.out.println( "nodes:" + nodeCount + " rels:" + relationshipCount );
    }
}
