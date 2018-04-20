/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.storageengine.api;

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
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.storageengine.api.schema.IndexProgressor;

public interface CursorBootstrap extends CursorFactory
{
    Client newClient( TxStateHolder txStateHolder, AssertOpen assertOpen, SecurityContext securityContext );

    /**
     * Bootstraps cursors at specified references.
     * TODO if tx state and assert-open functionality would live outside the actual cursors, decorated or something,
     * then this interface could be collapsed into a simpler interface, but now the tx state etc. leak into this interface
     * because the cursors need to implement that themselves.
     */
    interface Client extends TxStateHolder, AssertOpen
    {
        SecurityContext securityContext();

        long nodeHighMark();

        long relationshipHighMark();

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
    }
}
