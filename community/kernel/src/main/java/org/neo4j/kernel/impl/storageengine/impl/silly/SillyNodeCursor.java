package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.concurrent.ConcurrentMap;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class SillyNodeCursor implements NodeCursor
{
    private final ConcurrentMap<Long,NodeData> nodes;
    private long next = NO_ID;
    private NodeData current;

    SillyNodeCursor( ConcurrentMap<Long,NodeData> nodes )
    {
        this.nodes = nodes;
    }

    @Override
    public boolean next()
    {
        if ( next == NO_ID )
        {
            return false;
        }

        current = nodes.get( next );
        return current != null;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isClosed()
    {
        return false;
    }

    void single( long nodeId )
    {
        next = nodeId;
    }

    @Override
    public long nodeReference()
    {
        return current.id();
    }

    @Override
    public LabelSet labels()
    {
        return current.labelSet();
    }

    @Override
    public boolean hasProperties()
    {
        return !current.properties().isEmpty();
    }

    @Override
    public void relationships( RelationshipGroupCursor cursor )
    {
        ((SillyRelationshipGroupCursor)cursor).init( current.relationships() );
    }

    @Override
    public void allRelationships( RelationshipTraversalCursor cursor )
    {
        ((SillyRelationshipTraversalCursor)cursor).init( current.relationships() );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        ((SillyPropertyCursor)cursor).init( current.properties() );
    }

    @Override
    public long relationshipGroupReference()
    {
        return 0;
    }

    @Override
    public long allRelationshipsReference()
    {
        return 0;
    }

    @Override
    public long propertiesReference()
    {
        return 0;
    }

    @Override
    public boolean isDense()
    {
        return true;
    }
}
