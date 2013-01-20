/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.traversal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.traversal.Evaluators.atDepth;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.Traversal.bidirectionalTraversal;
import static org.neo4j.kernel.Traversal.traversal;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.Uniqueness;

public class TestPath extends AbstractTestBase
{
    private static Node a,b,c,d,e;
    private static Node[] nodes;
    private static Relationship aToB, bToC, cToD, dToE;
    private static Relationship[] relationships;
    
    @Before
    public void setup()
    {
        /*
         * (A)--->(B)--->(C)--->(D)--->(E)
         */
        
        createGraph( "A TO B", "B TO C", "C TO D", "D TO E" );
        a = getNodeWithName( "A" );
        b = getNodeWithName( "B" );
        c = getNodeWithName( "C" );
        d = getNodeWithName( "D" );
        e = getNodeWithName( "E" );
        nodes = new Node[] { a, b, c, d, e };
        aToB = a.getRelationships( Direction.OUTGOING ).iterator().next();
        bToC = b.getRelationships( Direction.OUTGOING ).iterator().next();
        cToD = c.getRelationships( Direction.OUTGOING ).iterator().next();
        dToE = d.getRelationships( Direction.OUTGOING ).iterator().next();
        relationships = new Relationship[] { aToB, bToC, cToD, dToE };
    }
    
    @Test
    public void testPathIterator()
    {
        Path path = traversal().evaluator( atDepth( 4 ) ).traverse( node( "A" ) ).iterator().next();
        
        assertPathIsCorrect( path );
    }

    private void assertPathIsCorrect( Path path )
    {
        Node a = node( "A" );
        Relationship to1 = a.getRelationships( Direction.OUTGOING ).iterator().next();
        Node b = to1.getEndNode();
        Relationship to2 = b.getRelationships( Direction.OUTGOING ).iterator().next();
        Node c = to2.getEndNode();
        Relationship to3 = c.getRelationships( Direction.OUTGOING ).iterator().next();
        Node d = to3.getEndNode();
        Relationship to4 = d.getRelationships( Direction.OUTGOING ).iterator().next();
        Node e = to4.getEndNode();
        
        assertEquals( (Integer) 4, (Integer) path.length() );
        assertEquals( a, path.startNode() );
        assertEquals( e, path.endNode() );
        assertEquals( to4, path.lastRelationship() );
        
        assertContainsInOrder( path, a, to1, b, to2, c, to3, d, to4, e );
        assertContainsInOrder( path.nodes(), a, b, c, d, e );
        assertContainsInOrder( path.relationships(), to1, to2, to3, to4 );
        assertContainsInOrder( path.reverseNodes(), e, d, c, b, a );
        assertContainsInOrder( path.reverseRelationships(), to4, to3, to2, to1 );
    }
    
    @Test
    public void reverseNodes() throws Exception
    {
        Path path = first( traversal().evaluator( atDepth( 0 ) ).traverse( a ) );
        assertContains( path.reverseNodes(), a );
        
        path = first( traversal().evaluator( atDepth( 4 ) ).traverse( a ) );
        assertContainsInOrder( path.reverseNodes(), e, d, c, b, a );
    }

    @Test
    public void reverseRelationships() throws Exception
    {
        Path path = first( traversal().evaluator( atDepth( 0 ) ).traverse( a ) );
        assertFalse( path.reverseRelationships().iterator().hasNext() );
        
        path = first( traversal().evaluator( atDepth( 4 ) ).traverse( a ) );
        Node[] expectedNodes = new Node[] { e, d, c, b, a };
        int index = 0;
        for ( Relationship rel : path.reverseRelationships() )
            assertEquals( "For index " + index, expectedNodes[index++], rel.getEndNode() );
        assertEquals( 4, index );
    }
    
    @Test
    public void testBidirectionalPath() throws Exception
    {
        TraversalDescription side = traversal().uniqueness( Uniqueness.NODE_PATH );
        BidirectionalTraversalDescription bidirectional = bidirectionalTraversal().mirroredSides( side );
        Path bidirectionalPath = first( bidirectional.traverse( a, e ) );
        assertPathIsCorrect( bidirectionalPath );
        
        assertEquals( a, first( bidirectional.traverse( a, e ) ).startNode() );
        
        // White box testing below: relationships(), nodes(), reverseRelationships(), reverseNodes()
        // does cache the start node if not already cached, so just make sure they to it properly.
        bidirectionalPath = first( bidirectional.traverse( a, e ) );
        bidirectionalPath.relationships();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = first( bidirectional.traverse( a, e ) );
        bidirectionalPath.nodes();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = first( bidirectional.traverse( a, e ) );
        bidirectionalPath.reverseRelationships();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = first( bidirectional.traverse( a, e ) );
        bidirectionalPath.reverseNodes();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = first( bidirectional.traverse( a, e ) );
        bidirectionalPath.iterator();
        assertEquals( a, bidirectionalPath.startNode() );
    }
    
    @Test
    public void getEntityByIndex() throws Exception
    {
        Path path = first( traversal().evaluator( atDepth( 4 ) ).traverse( a ) );
        assertNodesByIndex( path );
        assertRelationshipsByIndex( path );
        
        TraversalDescription side = traversal().uniqueness( Uniqueness.NODE_PATH );
        BidirectionalTraversalDescription bidirectional = bidirectionalTraversal().mirroredSides( side );
        Path bidirectionalPath = first( bidirectional.traverse( a, e ) );
        assertNodesByIndex( bidirectionalPath );
        assertRelationshipsByIndex( bidirectionalPath );
    }
    
    @Test
    public void subPaths() throws Exception
    {
        // GIVEN
        Path path = first( traversal().evaluator( atDepth( 4 ) ).traverse( node( "A" ) ) );

        // THEN
        assertSubPaths( path );
    }
    
    @Test
    public void bidirectionalSubPaths() throws Exception
    {
        // GIVEN
        TraversalDescription side = traversal().uniqueness( Uniqueness.NODE_PATH );
        BidirectionalTraversalDescription bidirectional = bidirectionalTraversal().mirroredSides( side );
        Path path = first( bidirectional.traverse( a, e ) );

        // THEN
        assertSubPaths( path );
    }
    
    private void assertSubPaths( Path path )
    {
        // illegal indexes
        assertException( IndexOutOfBoundsException.class, subPath( 5 ), path );
        assertException( IndexOutOfBoundsException.class, subPath( -6 ), path );
        
        // beginIndex
        expectPath( path.subPath( 0 ), "A,B,C,D,E" );
        expectPath( path.subPath( 1 ), "B,C,D,E" );
        expectPath( path.subPath( 2 ), "C,D,E" );
        expectPath( path.subPath( 3 ), "D,E" );
        expectPath( path.subPath( 4 ), "E" );
        expectPath( path.subPath( -1 ), "E" );
        expectPath( path.subPath( -2 ), "D,E" );
        expectPath( path.subPath( -3 ), "C,D,E" );
        expectPath( path.subPath( -4 ), "B,C,D,E" );
        expectPath( path.subPath( -5 ), "B,C,D,E" );
        
        // endIndex
        expectPath( path.subPath( 0, 1 ), "A" );
        expectPath( path.subPath( 0, 2 ), "A,B" );
        expectPath( path.subPath( 0, 3 ), "A,B,C" );
        expectPath( path.subPath( 0, 4 ), "A,B,C,D" );
        expectPath( path.subPath( 0, 5 ), "A,B,C,D,E" );
        assertException( IndexOutOfBoundsException.class, subPath( 0, 0 ), path );
        expectPath( path.subPath( 0, -1 ), "A,B,C,D" );
        expectPath( path.subPath( 0, -2 ), "A,B,C" );
        expectPath( path.subPath( 0, -3 ), "A,B" );
        expectPath( path.subPath( 0, -4 ), "A" );
        
        // both
        expectPath( path.subPath( 1, 4 ), "B,C,D" );
    }

    private <T,R> void assertException( Class<? extends Exception> exceptionClass, Function<T, R> function, T argument )
    {
        try
        {
            function.apply( argument );
            fail( "Expected exception of type " + exceptionClass );
        }
        catch ( Exception e )
        {
            assertTrue( exceptionClass.isAssignableFrom( e.getClass() ) );
        }
    }

    private Function<Path, Void> subPath( final int beginIndex )
    {
        return new Function<Path, Void>()
        {
            @Override
            public Void apply( Path from )
            {
                from.subPath( beginIndex );
                return null;
            }
        };
    }

    private Function<Path, Void> subPath( final int beginIndex, final int endIndex )
    {
        return new Function<Path, Void>()
        {
            @Override
            public Void apply( Path from )
            {
                from.subPath( beginIndex, endIndex );
                return null;
            }
        };
    }
    
    private void assertRelationshipsByIndex( Path path )
    {
        for ( int i = 0; i < relationships.length; i++ )
        {
            assertEquals( "Wrong relationship " + relationships[i] + " at index " + i + ", got " +
                    path.relationship( i ) + " in " + path + ".", relationships[i], path.relationship( i ) );
            
            int negativeIndex = -1 - i;
            Relationship expected = relationships[relationships.length+negativeIndex];
            Relationship found = path.relationship( negativeIndex );
            assertEquals( "Wrong relationship " + expected + " at index " + negativeIndex + ", got " +
                    found + " in " + path + ".", expected, found );
        }
    }

    private void assertNodesByIndex( Path path )
    {
        for ( int i = 0; i < nodes.length; i++ )
        {
            assertEquals( "Wrong node " + nodes[i].getProperty( "name" ) + " at index " + i + ", got " +
                    path.node( i ).getProperty( "name" ) + " in " + path + ".", nodes[i], path.node( i ) );
            
            int negativeIndex = -1 - i;
            Node expected = nodes[nodes.length+negativeIndex];
            Node found = path.node( negativeIndex );
            assertEquals( "Wrong node " + expected.getProperty( "name" ) + " at index " + negativeIndex + ", got " +
                    found.getProperty( "name" ) + " in " + path + ".", expected, found );
        }
    }
}
