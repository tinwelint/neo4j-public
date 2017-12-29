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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.CursorBootstrap;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

public class Cursors implements CursorBootstrap
{
    private final NeoStores neoStores;

    public Cursors( NeoStores neoStores )
    {
        this.neoStores = neoStores;
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return new DefaultNodeCursor();
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new DefaultRelationshipScanCursor();
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return new DefaultRelationshipTraversalCursor( allocateRelationshipGroupCursor() );
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return new DefaultPropertyCursor( );
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return new DefaultRelationshipGroupCursor( );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return new DefaultNodeValueIndexCursor( );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return new DefaultNodeLabelIndexCursor( );
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        return new DefaultNodeExplicitIndexCursor( );
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        return new DefaultRelationshipExplicitIndexCursor( );
    }

    @Override
    public Client newClient( TxStateHolder txStateHolder, AssertOpen assertOpen )
    {
        return new CursorsClient( txStateHolder, assertOpen, neoStores );
    }

    public static class CursorsClient implements Client
    {
        private final TxStateHolder txStateHolder;
        private final AssertOpen assertOpen;
        private final NodeStore nodes;
        private final RelationshipStore relationships;
        private final RelationshipGroupStore groups;
        private final PropertyStore properties;

        @Override
        public TransactionState txState()
        {
            return txStateHolder.txState();
        }

        @Override
        public ExplicitIndexTransactionState explicitIndexTxState()
        {
            return txStateHolder.explicitIndexTxState();
        }

        @Override
        public boolean hasTxStateWithChanges()
        {
            return txStateHolder.hasTxStateWithChanges();
        }

        @Override
        public void assertOpen()
        {
            assertOpen.assertOpen();
        }

        CursorsClient( TxStateHolder txStateHolder, AssertOpen assertOpen, NeoStores neoStores )
        {
            this.txStateHolder = txStateHolder;
            this.assertOpen = assertOpen;
            this.nodes = neoStores.getNodeStore();
            this.relationships = neoStores.getRelationshipStore();
            this.groups = neoStores.getRelationshipGroupStore();
            this.properties = neoStores.getPropertyStore();
        }

        @Override
        public void allNodesScan( org.neo4j.internal.kernel.api.NodeCursor cursor )
        {
            ((NodeCursor)cursor).scan( this );
        }

        @Override
        public void singleNode( long reference, org.neo4j.internal.kernel.api.NodeCursor cursor )
        {
            ((NodeCursor)cursor).single( reference, this );
        }

        @Override
        public long nodeHighMark()
        {
            return nodes.getHighId();
        }

        @Override
        public long relationshipHighMark()
        {
            return relationships.getHighId();
        }

        @Override
        public void singleRelationship( long reference, org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
        {
            ((DefaultRelationshipScanCursor) cursor).single( reference, this );
        }

        @Override
        public void nodeProperties( long nodeReference, long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
        {
            ((DefaultPropertyCursor) cursor).initNode( nodeReference, reference, this );
        }

        @Override
        public void relationshipProperties( long relationshipReference, long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
        {
            ((DefaultPropertyCursor) cursor).initRelationship( relationshipReference, reference, this );
        }

        @Override
        public void graphProperties( long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
        {
            ((DefaultPropertyCursor) cursor).initGraph( NO_ID, reference, this );
        }

        @Override
        public void allRelationshipsScan( org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
        {
            ((DefaultRelationshipScanCursor) cursor).scan( -1/*include all labels*/, this );
        }

        @Override
        public void relationshipLabelScan( int label, org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
        {
            ((DefaultRelationshipScanCursor) cursor).scan( label, this );
        }

        @Override
        public void relationships( long nodeReference, long reference,
                org.neo4j.internal.kernel.api.RelationshipTraversalCursor cursor )
        {
            /* TODO: There are actually five (5!) different ways a relationship traversal cursor can be initialized:
            *
            * 1. From a batched group in a detached way. This happens when the user manually retrieves the relationships
            *    references from the group cursor and passes it to this method and if the group cursor was based on having
            *    batched all the different types in the single (mixed) chain of relationships.
            *    In this case we should pass a reference marked with some flag to the first relationship in the chain that
            *    has the type of the current group in the group cursor. The traversal cursor then needs to read the type
            *    from that first record and use that type as a filter for when reading the rest of the chain.
            *    - NOTE: we probably have to do the same sort of filtering for direction - so we need a flag for that too.
            *
            * 2. From a batched group in a DIRECT way. This happens when the traversal cursor is initialized directly from
            *    the group cursor, in this case we can simply initialize the traversal cursor with the buffered state from
            *    the group cursor, so this method here does not have to be involved, and things become pretty simple.
            *
            * 3. Traversing all relationships - regardless of type - of a node that has grouped relationships. In this case
            *    the traversal cursor needs to traverse through the group records in order to get to the actual
            *    relationships. The initialization of the cursor (through this here method) should be with a FLAGGED
            *    reference to the (first) group record.
            *
            * 4. Traversing a single chain - this is what happens in the cases when
            *    a) Traversing all relationships of a node without grouped relationships.
            *    b) Traversing the relationships of a particular group of a node with grouped relationships.
            *
            * 5. There are no relationships - i.e. passing in NO_ID to this method.
            *
            * This means that we need reference encodings (flags) for cases: 1, 3, 4, 5
            */
           if ( hasGroupFlag( reference ) ) // this reference is actually to a group record
           {
               ((RelationshipTraversalCursor) cursor).groups( nodeReference, clearFlags( reference ), this );
           }
           else if ( hasFilterFlag( reference ) ) // this relationship chain need to be filtered
           {
               ((RelationshipTraversalCursor) cursor).filtered( nodeReference, clearFlags( reference ), this );
           }
           else // this is a normal relationship reference
           {
               ((RelationshipTraversalCursor) cursor).chain( nodeReference, reference, this );
           }
        }

        @Override
        public void relationshipGroups( long nodeReference, long reference,
                org.neo4j.internal.kernel.api.RelationshipGroupCursor cursor )
        {
            if ( hasDirectFlag( reference ) ) // the relationships for this node are not grouped
            {
                ((RelationshipGroupCursor) cursor).buffer( nodeReference, clearFlags( reference ), this );
            }
            else // this is a normal group reference.
            {
                ((RelationshipGroupCursor) cursor).direct( nodeReference, reference, this );
            }
        }

        PageCursor nodePage( long reference )
        {
            return nodes.openPageCursorForReading( reference );
        }

        PageCursor relationshipPage( long reference )
        {
            return relationships.openPageCursorForReading( reference );
        }

        PageCursor groupPage( long reference )
        {
            return groups.openPageCursorForReading( reference );
        }

        PropertyStore propertyStore()
        {
            return properties;
        }

        RecordCursor<DynamicRecord> labelCursor()
        {
            return nodes.newLabelCursor();
        }

        void node( NodeRecord record, long reference, PageCursor pageCursor )
        {
            nodes.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
        }

        void relationship( RelationshipRecord record, long reference, PageCursor pageCursor )
        {
            relationships.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
        }

        void group( RelationshipGroupRecord record, long reference, PageCursor page )
        {
            groups.getRecordByCursor( reference, record, RecordLoad.NORMAL, page );
        }
    }
}
