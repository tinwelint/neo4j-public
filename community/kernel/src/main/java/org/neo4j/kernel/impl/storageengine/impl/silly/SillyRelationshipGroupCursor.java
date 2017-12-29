package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.concurrent.ConcurrentMap;

import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.impl.api.TwoPhaseNodeForRelationshipLockingTest.RelationshipData;

class SillyRelationshipGroupCursor implements RelationshipGroupCursor
{
    private ConcurrentMap<TypeAndDirection,ConcurrentMap<Long,RelationshipData>> relationships;

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
    public int relationshipLabel()
    {
        return 0;
    }

    @Override
    public int outgoingCount()
    {
        return 0;
    }

    @Override
    public int incomingCount()
    {
        return 0;
    }

    @Override
    public int loopCount()
    {
        return 0;
    }

    @Override
    public void outgoing( RelationshipTraversalCursor cursor )
    {
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
    }

    @Override
    public void loops( RelationshipTraversalCursor cursor )
    {
    }

    @Override
    public long outgoingReference()
    {
        return 0;
    }

    @Override
    public long incomingReference()
    {
        return 0;
    }

    @Override
    public long loopsReference()
    {
        return 0;
    }

    void init( ConcurrentMap<TypeAndDirection,ConcurrentMap<Long,RelationshipData>> relationships )
    {
        this.relationships = relationships;
    }
}
