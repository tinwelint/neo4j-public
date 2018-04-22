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

import org.neo4j.storageengine.api.schema.IndexProgressor;

/**
 * Allocates various types of {@link Cursor cursors} for reading data from a database.
 * Allocated cursors can and should be reused for reading multiple data items.
 * Cursors can be positioned and primed by any of the positioning methods, like {@link #singleNode(long, NodeCursor)},
 * {@link #relationships(long, long, RelationshipTraversalCursor)} etc.
 * A {@link CursorFactory} instance is considered to be tied to one transaction, not necessarily one thread, at a time.
 */
public interface CursorFactory
{
    // === CURSOR ALLOCATION ===

    // entities

    NodeCursor allocateNodeCursor();

    RelationshipScanCursor allocateRelationshipScanCursor();

    RelationshipTraversalCursor allocateRelationshipTraversalCursor();

    // properties

    PropertyCursor allocatePropertyCursor();

    // traversal

    RelationshipGroupCursor allocateRelationshipGroupCursor();

    // schema indexes

    NodeValueIndexCursor allocateNodeValueIndexCursor();

    NodeLabelIndexCursor allocateNodeLabelIndexCursor();

    // explicit indexes

    NodeExplicitIndexCursor allocateNodeExplicitIndexCursor();

    RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor();

    // === CURSOR POSITIONING

    void allNodesScan( NodeCursor cursor );

    void singleNode( long reference, NodeCursor cursor );

    void singleRelationship( long reference, RelationshipScanCursor cursor );

    void allRelationshipsScan( RelationshipScanCursor cursor );

    void relationshipLabelScan( int label, RelationshipScanCursor cursor );

    void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor );

    void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor group );

    void nodeProperties( long nodeReference, long reference, PropertyCursor cursor );

    void relationshipProperties( long relationshipReference, long reference, PropertyCursor cursor );

    void graphProperties( long reference, PropertyCursor cursor );

    IndexProgressor.NodeValueClient indexSeek( NodeValueIndexCursor cursor );

    IndexProgressor.NodeLabelClient labelSeek( NodeLabelIndexCursor cursor );

    IndexProgressor.ExplicitClient explicitIndexSeek( NodeExplicitIndexCursor cursor );

    IndexProgressor.ExplicitClient explicitIndexSeek( RelationshipExplicitIndexCursor cursor );

    // === MISC STUFF, SHOULD PROBABLY MOVE TO ANOTHER INTERFACE ===

    long nodeHighMark();

    long relationshipHighMark();

    // === LIFECYCLE ===

    void assertClosed();

    void release();
}
