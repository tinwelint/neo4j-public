/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.internal.kernel.api;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.storageengine.api.schema.IndexProgressor;

import static org.junit.Assert.fail;

public class ManagedTestCursors implements CursorFactory
{
    private List<Cursor> allCursors = new ArrayList<>();

    private CursorFactory cursors;

    ManagedTestCursors( CursorFactory c )
    {
        this.cursors = c;
    }

    void assertAllClosedAndReset()
    {
        for ( Cursor n : allCursors )
        {
            if ( !n.isClosed() )
            {
                fail( "The Cursor " + n.toString() + " was not closed properly." );
            }
        }

        allCursors.clear();
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        NodeCursor n = cursors.allocateNodeCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        RelationshipScanCursor n = cursors.allocateRelationshipScanCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        RelationshipTraversalCursor n = cursors.allocateRelationshipTraversalCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        PropertyCursor n = cursors.allocatePropertyCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        RelationshipGroupCursor n = cursors.allocateRelationshipGroupCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        NodeValueIndexCursor n = cursors.allocateNodeValueIndexCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        NodeLabelIndexCursor n = cursors.allocateNodeLabelIndexCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        NodeExplicitIndexCursor n = cursors.allocateNodeExplicitIndexCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        RelationshipExplicitIndexCursor n = cursors.allocateRelationshipExplicitIndexCursor();
        allCursors.add( n );
        return n;
    }

    @Override
    public void allNodesScan( NodeCursor cursor )
    {

    }

    @Override
    public void singleNode( long reference, NodeCursor cursor )
    {

    }

    @Override
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {

    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {

    }

    @Override
    public void relationshipLabelScan( int label, RelationshipScanCursor cursor )
    {

    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {

    }

    @Override
    public void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor group )
    {

    }

    @Override
    public void nodeProperties( long nodeReference, long reference, PropertyCursor cursor )
    {

    }

    @Override
    public void relationshipProperties( long relationshipReference, long reference, PropertyCursor cursor )
    {

    }

    @Override
    public void graphProperties( long reference, PropertyCursor cursor )
    {

    }

    @Override
    public IndexProgressor.NodeValueClient indexSeek( NodeValueIndexCursor cursor )
    {
        return null;
    }

    @Override
    public IndexProgressor.NodeLabelClient labelSeek( NodeLabelIndexCursor cursor )
    {
        return null;
    }

    @Override
    public IndexProgressor.ExplicitClient explicitIndexSeek( NodeExplicitIndexCursor cursor )
    {
        return null;
    }

    @Override
    public IndexProgressor.ExplicitClient explicitIndexSeek( RelationshipExplicitIndexCursor cursor )
    {
        return null;
    }

    @Override
    public long nodeHighMark()
    {
        return 0;
    }

    @Override
    public long relationshipHighMark()
    {
        return 0;
    }

    @Override
    public void assertClosed()
    {
    }

    @Override
    public void release()
    {
    }
}
