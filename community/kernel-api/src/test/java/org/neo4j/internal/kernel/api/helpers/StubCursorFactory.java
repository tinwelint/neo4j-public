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
package org.neo4j.internal.kernel.api.helpers;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.storageengine.api.schema.IndexProgressor;

public class StubCursorFactory implements CursorFactory
{
    private final boolean continueWithLastItem;
    private Queue<NodeCursor> nodeCursors = new LinkedList<>();
    private Queue<RelationshipScanCursor> relationshipScanCursors = new LinkedList<>();
    private Queue<RelationshipTraversalCursor> relationshiTraversalCursors = new LinkedList<>();
    private Queue<PropertyCursor> propertyCursors = new LinkedList<>();
    private Queue<RelationshipGroupCursor> groupCursors = new LinkedList<>();
    private Queue<NodeValueIndexCursor> nodeValueIndexCursors = new LinkedList<>();
    private Queue<NodeLabelIndexCursor> nodeLabelIndexCursors = new LinkedList<>();
    private Queue<NodeExplicitIndexCursor> nodeExplicitIndexCursors = new LinkedList<>();
    private Queue<RelationshipExplicitIndexCursor> relationshipExplicitIndexCursors = new LinkedList<>();

    public StubCursorFactory()
    {
        this( false );
    }

    public StubCursorFactory( boolean continueWithLastItem )
    {
        this.continueWithLastItem = continueWithLastItem;
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return poll( nodeCursors );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return poll( relationshipScanCursors );
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return poll( relationshiTraversalCursors );
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return poll( propertyCursors );
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return poll( groupCursors );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return poll( nodeValueIndexCursors );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return poll( nodeLabelIndexCursors );
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        return poll( nodeExplicitIndexCursors );
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        return poll( relationshipExplicitIndexCursors );
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
    public void initialize( SecurityContext securityContext )
    {

    }

    @Override
    public SecurityContext securityContext()
    {
        return null;
    }

    @Override
    public void assertClosed()
    {
    }

    @Override
    public void release()
    {
    }

    public StubCursorFactory withGroupCursors( RelationshipGroupCursor...cursors )
    {
        groupCursors.addAll( Arrays.asList( cursors ) );
        return this;
    }

    public StubCursorFactory withRelationshipTraversalCursors( RelationshipTraversalCursor...cursors )
    {
        relationshiTraversalCursors.addAll( Arrays.asList( cursors ) );
        return this;
    }

    private <T> T poll( Queue<T> queue )
    {
        T poll = queue.poll();
        if ( continueWithLastItem && queue.isEmpty() )
        {
            queue.offer( poll );
        }
        return poll;
    }
}
