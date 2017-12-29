package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.storageengine.api.RelationshipItem;

class RelationshipData implements RelationshipItem
{
    private final ConcurrentMap<Integer,PropertyData> properties = new ConcurrentHashMap<>();

    RelationshipData( long id, int type, long startNode, long endNode )
    {
    }

    ConcurrentMap<Integer,PropertyData> properties()
    {
        return properties;
    }

    public <EXCEPTION extends Exception> boolean visit( RelationshipVisitor<EXCEPTION> visitor )
    {
        return false;
    }

    @Override
    public long id()
    {
        return 0;
    }

    @Override
    public int type()
    {
        return 0;
    }

    @Override
    public long startNode()
    {
        return 0;
    }

    @Override
    public long endNode()
    {
        return 0;
    }

    @Override
    public long otherNode( long nodeId )
    {
        return 0;
    }

    @Override
    public long nextPropertyId()
    {
        return 0;
    }

    @Override
    public Lock lock()
    {
        return null;
    }
}
