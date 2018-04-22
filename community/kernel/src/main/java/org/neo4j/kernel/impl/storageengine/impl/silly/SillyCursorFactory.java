package org.neo4j.kernel.impl.storageengine.impl.silly;

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
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.storageengine.api.schema.IndexProgressor;

public class SillyCursorFactory implements CursorFactory
{
    private final SillyData data;
    private final TxStateHolder txStateHolder;
    private final AssertOpen assertOpen;
    private SecurityContext securityContext;

    public SillyCursorFactory( SillyData data, TxStateHolder txStateHolder, AssertOpen assertOpen )
    {
        this.data = data;
        this.txStateHolder = txStateHolder;
        this.assertOpen = assertOpen;
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return new SillyNodeCursor( data.nodes );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new SillyRelationshipScanCursor( data.relationships );
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return new SillyRelationshipTraversalCursor();
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return new SillyPropertyCursor();
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return new SillyRelationshipGroupCursor();
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return new SillyNodeValueIndexCursor();
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return new SillyNodeLabelIndexCursor();
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void assertClosed()
    {
    }

    @Override
    public void release()
    {
    }

    @Override
    public void initialize( SecurityContext securityContext )
    {
        this.securityContext = securityContext;
    }

    public void assertOpen()
    {
        assertOpen.assertOpen();
    }

    public TransactionState txState()
    {
        return txStateHolder.txState();
    }

    @Override
    public SecurityContext securityContext()
    {
        return securityContext;
    }

    public boolean hasTxStateWithChanges()
    {
        return txStateHolder.hasTxStateWithChanges();
    }

    public ExplicitIndexTransactionState explicitIndexTxState()
    {
        return txStateHolder.explicitIndexTxState();
    }

    @Override
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        ((SillyRelationshipScanCursor)cursor).single( this, reference );
    }

    @Override
    public void singleNode( long reference, NodeCursor cursor )
    {
        ((SillyNodeCursor)cursor).single( this, reference );
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        NodeData node = data.nodes.get( nodeReference );
        ((SillyRelationshipTraversalCursor)cursor).init( node.relationships() );
    }

    @Override
    public void relationshipProperties( long relationshipReference, long reference, PropertyCursor cursor )
    {
        RelationshipData relationship = data.relationships.get( relationshipReference );
        ((SillyPropertyCursor)cursor).init( relationship.properties() );
    }

    @Override
    public void relationshipLabelScan( int label, RelationshipScanCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long relationshipHighMark()
    {
        return data.nextRelationshipId.get();
    }

    @Override
    public void relationshipGroups( long relationshipReference, long reference, RelationshipGroupCursor group )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeProperties( long nodeReference, long reference, PropertyCursor cursor )
    {
        NodeData node = data.nodes.get( nodeReference );
        ((SillyPropertyCursor)cursor).init( node.properties() );
    }

    @Override
    public long nodeHighMark()
    {
        return data.nextNodeId.get();
    }

    @Override
    public void graphProperties( long reference, PropertyCursor cursor )
    {
        ((SillyPropertyCursor)cursor).init( data.graphProperties );
    }

    @Override
    public IndexProgressor.NodeValueClient indexSeek( NodeValueIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public IndexProgressor.NodeLabelClient labelSeek( NodeLabelIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public IndexProgressor.ExplicitClient explicitIndexSeek( NodeExplicitIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public IndexProgressor.ExplicitClient explicitIndexSeek( RelationshipExplicitIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void allNodesScan( NodeCursor cursor )
    {
        throw new UnsupportedOperationException();
    }
}
