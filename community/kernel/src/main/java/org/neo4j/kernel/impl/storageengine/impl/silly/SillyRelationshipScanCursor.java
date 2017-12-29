package org.neo4j.kernel.impl.storageengine.impl.silly;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;

class SillyRelationshipScanCursor implements RelationshipScanCursor
{
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
}
