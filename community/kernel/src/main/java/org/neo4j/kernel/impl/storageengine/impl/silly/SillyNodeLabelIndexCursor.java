package org.neo4j.kernel.impl.storageengine.impl.silly;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;

class SillyNodeLabelIndexCursor implements NodeLabelIndexCursor
{
    @Override
    public void node( NodeCursor cursor )
    {
    }

    @Override
    public long nodeReference()
    {
        return 0;
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
    public LabelSet labels()
    {
        return null;
    }
}
