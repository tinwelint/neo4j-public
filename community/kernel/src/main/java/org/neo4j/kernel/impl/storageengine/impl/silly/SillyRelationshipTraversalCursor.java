package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.concurrent.ConcurrentMap;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.impl.api.TwoPhaseNodeForRelationshipLockingTest.RelationshipData;

class SillyRelationshipTraversalCursor implements RelationshipTraversalCursor
{
    private ConcurrentMap<TypeAndDirection,ConcurrentMap<Long,RelationshipData>> relationships;

    @Override
    public long relationshipReference()
    {
        return 0;
    }

    @Override
    public int label()
    {
        return 0;
    }

    @Override
    public boolean hasProperties()
    {
        return false;
    }

    @Override
    public void source( NodeCursor cursor )
    {
    }

    @Override
    public void target( NodeCursor cursor )
    {
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
    }

    @Override
    public long sourceNodeReference()
    {
        return 0;
    }

    @Override
    public long targetNodeReference()
    {
        return 0;
    }

    @Override
    public long propertiesReference()
    {
        return 0;
    }

    @Override
    public Position suspend()
    {
        return null;
    }

    @Override
    public void resume( Position position )
    {
    }

    @Override
    public boolean next()
    {
        return false;
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

    @Override
    public void neighbour( NodeCursor cursor )
    {
    }

    @Override
    public long neighbourNodeReference()
    {
        return 0;
    }

    @Override
    public long originNodeReference()
    {
        return 0;
    }

    void init( ConcurrentMap<TypeAndDirection,ConcurrentMap<Long,RelationshipData>> relationships )
    {
        this.relationships = relationships;
    }
}
