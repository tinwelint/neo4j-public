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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.AutoCloseablePlus;
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
import org.neo4j.internal.kernel.api.TransactionalCursorDependencies;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.txstate.TransactionState;
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
import org.neo4j.storageengine.api.schema.IndexProgressor;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.RelationshipDirection.INCOMING;
import static org.neo4j.internal.kernel.api.RelationshipDirection.LOOP;
import static org.neo4j.internal.kernel.api.RelationshipDirection.OUTGOING;
import static org.neo4j.kernel.impl.newapi.GroupReferenceEncoding.isRelationship;
import static org.neo4j.kernel.impl.newapi.References.clearEncoding;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.util.FeatureToggles.flag;

public class DefaultCursors implements CursorFactory
{
    private final NodeStore nodes;
    private final RelationshipStore relationships;
    private final RelationshipGroupStore groups;
    private final PropertyStore properties;
    private final TransactionalCursorDependencies transactionalDependencies;

    private DefaultNodeCursor nodeCursor;
    private DefaultRelationshipScanCursor relationshipScanCursor;
    private DefaultRelationshipTraversalCursor relationshipTraversalCursor;
    private DefaultPropertyCursor propertyCursor;
    private DefaultRelationshipGroupCursor relationshipGroupCursor;
    private DefaultNodeValueIndexCursor nodeValueIndexCursor;
    private DefaultNodeLabelIndexCursor nodeLabelIndexCursor;
    private DefaultNodeExplicitIndexCursor nodeExplicitIndexCursor;
    private DefaultRelationshipExplicitIndexCursor relationshipExplicitIndexCursor;

    private static final boolean DEBUG_CLOSING = flag( DefaultCursors.class, "trackCursors", false );
    private List<CloseableStacktrace> closeables = new ArrayList<>();

    public DefaultCursors( NeoStores neoStores, TransactionalCursorDependencies transactionalDependencies )
    {
        this.nodes = neoStores.getNodeStore();
        this.relationships = neoStores.getRelationshipStore();
        this.groups = neoStores.getRelationshipGroupStore();
        this.properties = neoStores.getPropertyStore();
        this.transactionalDependencies = transactionalDependencies;
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor()
    {
        if ( nodeCursor == null )
        {
            return trace( new DefaultNodeCursor( this ) );
        }

        try
        {
            return nodeCursor;
        }
        finally
        {
            nodeCursor = null;
        }
    }

    public void accept( DefaultNodeCursor cursor )
    {
        if ( nodeCursor != null )
        {
            nodeCursor.release();
        }
        nodeCursor = cursor;
    }

    @Override
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor()
    {
        if ( relationshipScanCursor == null )
        {
            return trace( new DefaultRelationshipScanCursor( this, true ) );
        }

        try
        {
            return relationshipScanCursor;
        }
        finally
        {
            relationshipScanCursor = null;
        }
    }

    public void accept( DefaultRelationshipScanCursor cursor )
    {
        if ( relationshipScanCursor != null )
        {
            relationshipScanCursor.release();
        }
        relationshipScanCursor = cursor;
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        if ( relationshipTraversalCursor == null )
        {
            return trace( new DefaultRelationshipTraversalCursor( new DefaultRelationshipGroupCursor( this, false ), this ) );
        }

        try
        {
            return relationshipTraversalCursor;
        }
        finally
        {
            relationshipTraversalCursor = null;
        }
    }

    public void accept( DefaultRelationshipTraversalCursor cursor )
    {
        if ( relationshipTraversalCursor != null )
        {
            relationshipTraversalCursor.release();
        }
        relationshipTraversalCursor = cursor;
    }

    @Override
    public DefaultPropertyCursor allocatePropertyCursor()
    {
        if ( propertyCursor == null )
        {
            return trace( new DefaultPropertyCursor( this ) );
        }

        try
        {
            return propertyCursor;
        }
        finally
        {
            propertyCursor = null;
        }
    }

    public void accept( DefaultPropertyCursor cursor )
    {
        if ( propertyCursor != null )
        {
            propertyCursor.release();
        }
        propertyCursor = cursor;
    }

    @Override
    public DefaultRelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        if ( relationshipGroupCursor == null )
        {
            return trace( new DefaultRelationshipGroupCursor( this, true ) );
        }

        try
        {
            return relationshipGroupCursor;
        }
        finally
        {
            relationshipGroupCursor = null;
        }
    }

    public void accept( DefaultRelationshipGroupCursor cursor )
    {
        if ( relationshipGroupCursor != null )
        {
            relationshipGroupCursor.release();
        }
        relationshipGroupCursor = cursor;
    }

    @Override
    public DefaultNodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        if ( nodeValueIndexCursor == null )
        {
            return trace( new DefaultNodeValueIndexCursor( this ) );
        }

        try
        {
            return nodeValueIndexCursor;
        }
        finally
        {
            nodeValueIndexCursor = null;
        }
    }

    public void accept( DefaultNodeValueIndexCursor cursor )
    {
        if ( nodeValueIndexCursor != null )
        {
            nodeValueIndexCursor.release();
        }
        nodeValueIndexCursor = cursor;
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        if ( nodeLabelIndexCursor == null )
        {
            return trace( new DefaultNodeLabelIndexCursor( this ) );
        }

        try
        {
            return nodeLabelIndexCursor;
        }
        finally
        {
            nodeLabelIndexCursor = null;
        }
    }

    public void accept( DefaultNodeLabelIndexCursor cursor )
    {
        if ( nodeLabelIndexCursor != null )
        {
            nodeLabelIndexCursor.release();
        }
        nodeLabelIndexCursor = cursor;
    }

    @Override
    public DefaultNodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        if ( nodeExplicitIndexCursor == null )
        {
            return trace( new DefaultNodeExplicitIndexCursor( this ) );
        }

        try
        {
            return nodeExplicitIndexCursor;
        }
        finally
        {
            nodeExplicitIndexCursor = null;
        }
    }

    public void accept( DefaultNodeExplicitIndexCursor cursor )
    {
        if ( nodeExplicitIndexCursor != null )
        {
            nodeExplicitIndexCursor.release();
        }
        nodeExplicitIndexCursor = cursor;
    }

    @Override
    public DefaultRelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        if ( relationshipExplicitIndexCursor == null )
        {
            return trace( new DefaultRelationshipExplicitIndexCursor( new DefaultRelationshipScanCursor( this, false ), this ) );
        }

        try
        {
            return relationshipExplicitIndexCursor;
        }
        finally
        {
            relationshipExplicitIndexCursor = null;
        }
    }

    public void accept( DefaultRelationshipExplicitIndexCursor cursor )
    {
        if ( relationshipExplicitIndexCursor != null )
        {
            relationshipExplicitIndexCursor.release();
        }
        relationshipExplicitIndexCursor = cursor;
    }

    @Override
    public void release()
    {
        if ( nodeCursor != null )
        {
            nodeCursor.release();
            nodeCursor = null;
        }
        if ( relationshipScanCursor != null )
        {
            relationshipScanCursor.release();
            relationshipScanCursor = null;
        }
        if ( relationshipTraversalCursor != null )
        {
            relationshipTraversalCursor.release();
            relationshipTraversalCursor = null;
        }
        if ( propertyCursor != null )
        {
            propertyCursor.release();
            propertyCursor = null;
        }
        if ( relationshipGroupCursor != null )
        {
            relationshipGroupCursor.release();
            relationshipGroupCursor = null;
        }
        if ( nodeValueIndexCursor != null )
        {
            nodeValueIndexCursor.release();
            nodeValueIndexCursor = null;
        }
        if ( nodeLabelIndexCursor != null )
        {
            nodeLabelIndexCursor.release();
            nodeLabelIndexCursor = null;
        }
        if ( nodeExplicitIndexCursor != null )
        {
            nodeExplicitIndexCursor.release();
            nodeExplicitIndexCursor = null;
        }
        if ( relationshipExplicitIndexCursor != null )
        {
            relationshipExplicitIndexCursor.release();
            relationshipExplicitIndexCursor = null;
        }
    }

    private <T extends AutoCloseablePlus> T trace( T closeable )
    {
        if ( DEBUG_CLOSING )
        {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            closeables.add( new CloseableStacktrace( closeable, Arrays.copyOfRange( stackTrace, 2, stackTrace.length ) ) );
        }
        return closeable;
    }

    @Override
    public void assertClosed()
    {
        if ( DEBUG_CLOSING )
        {
            for ( CloseableStacktrace c : closeables )
            {
                c.assertClosed();
            }
            closeables.clear();
        }
    }

    public TransactionState txState()
    {
        return transactionalDependencies.txState();
    }

    public boolean hasTxStateWithChanges()
    {
        return transactionalDependencies.hasTxStateWithChanges();
    }

    public void assertOpen()
    {
        transactionalDependencies.assertOpen();
    }

    @Override
    public void allNodesScan( NodeCursor cursor )
    {
        ((DefaultNodeCursor)cursor).scan();
    }

    @Override
    public void singleNode( long reference, NodeCursor cursor )
    {
        ((DefaultNodeCursor)cursor).single( reference );
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
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        ((DefaultRelationshipScanCursor) cursor).single( reference );
    }

    @Override
    public void nodeProperties( long nodeReference, long reference, PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initNode( nodeReference, reference );
    }

    @Override
    public void relationshipProperties( long relationshipReference, long reference, PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initRelationship( relationshipReference, reference );
    }

    @Override
    public void graphProperties( long reference, PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initGraph( reference );
    }

    @Override
    public IndexProgressor.NodeValueClient indexSeek( NodeValueIndexCursor cursor )
    {
        DefaultNodeValueIndexCursor indexCursor = (DefaultNodeValueIndexCursor) cursor;
        indexCursor.initialize( (Resource) null );
        return indexCursor;
    }

    @Override
    public IndexProgressor.NodeLabelClient labelSeek( NodeLabelIndexCursor cursor )
    {
        DefaultNodeLabelIndexCursor indexCursor = (DefaultNodeLabelIndexCursor) cursor;
        return indexCursor;
    }

    @Override
    public IndexProgressor.ExplicitClient explicitIndexSeek( NodeExplicitIndexCursor cursor )
    {
        DefaultNodeExplicitIndexCursor indexCursor = (DefaultNodeExplicitIndexCursor) cursor;
        return indexCursor;
    }

    @Override
    public IndexProgressor.ExplicitClient explicitIndexSeek( RelationshipExplicitIndexCursor cursor )
    {
        DefaultRelationshipExplicitIndexCursor indexCursor = (DefaultRelationshipExplicitIndexCursor) cursor;
        return indexCursor;
    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        ((DefaultRelationshipScanCursor) cursor).scan( -1/*include all types*/ );
    }

    @Override
    public void relationshipLabelScan( int label, RelationshipScanCursor cursor )
    {
        ((DefaultRelationshipScanCursor) cursor).scan( label );
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        /* There are 5 different ways a relationship traversal cursor can be initialized:
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
        int relationshipType;
        RelationshipReferenceEncoding encoding = RelationshipReferenceEncoding.parseEncoding( reference );
        DefaultRelationshipTraversalCursor internalCursor = (DefaultRelationshipTraversalCursor)cursor;

        switch ( encoding )
        {
        case NONE: // this is a normal relationship reference
            internalCursor.chain( nodeReference, reference );
            break;

        case FILTER: // this relationship chain needs to be filtered
            internalCursor.filtered( nodeReference, clearEncoding( reference ), true );
            break;

        case FILTER_TX_STATE: // tx-state changes should be filtered by the head of this chain
            internalCursor.filtered( nodeReference, clearEncoding( reference ), false );
            break;

        case GROUP: // this reference is actually to a group record
            internalCursor.groups( nodeReference, clearEncoding( reference ) );
            break;

        case NO_OUTGOING_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            internalCursor.filteredTxState( nodeReference, relationshipType, OUTGOING );
            break;

        case NO_INCOMING_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            internalCursor.filteredTxState( nodeReference, relationshipType, INCOMING );
            break;

        case NO_LOOP_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            internalCursor.filteredTxState( nodeReference, relationshipType, LOOP );
            break;

        default:
            throw new IllegalStateException( "Unknown encoding " + encoding );
        }
    }

    @Override
    public void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        // the relationships for this node are not grouped in the store
        if ( reference != NO_ID && isRelationship( reference ) )
        {
            ((DefaultRelationshipGroupCursor) cursor).buffer( nodeReference, clearEncoding( reference ) );
        }
        else // this is a normal group reference.
        {
            ((DefaultRelationshipGroupCursor) cursor).direct( nodeReference, reference );
        }
    }

    void relationshipFull( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        // We need to load forcefully for relationship chain traversal since otherwise we cannot
        // traverse over relationship records which have been concurrently deleted
        // (flagged as inUse = false).
        // see
        //      org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        //      org.neo4j.kernel.impl.locking.RelationshipCreateDeleteIT
        relationships.getRecordByCursor( reference, record, RecordLoad.FORCE, pageCursor );
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

    public AccessMode accessMode()
    {
        return transactionalDependencies.accessMode();
    }

    static class CloseableStacktrace
    {
        private final AutoCloseablePlus c;
        private final StackTraceElement[] stackTrace;

        CloseableStacktrace( AutoCloseablePlus c, StackTraceElement[] stackTrace )
        {
            this.c = c;
            this.stackTrace = stackTrace;
        }

        void assertClosed()
        {
            if ( !c.isClosed() )
            {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                PrintStream printStream = new PrintStream( out );

                for ( StackTraceElement traceElement : stackTrace )
                {
                    printStream.println( "\tat " + traceElement );
                }
                printStream.println();
                throw new IllegalStateException( format( "Closeable %s was not closed!\n%s", c, out.toString() ) );
            }
        }
    }
}
