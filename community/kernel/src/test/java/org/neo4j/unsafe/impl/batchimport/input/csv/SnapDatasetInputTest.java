/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
