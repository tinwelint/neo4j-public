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
package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;

class RelationshipData implements RelationshipItem
{
    private final ConcurrentMap<Integer,PropertyData> properties = new ConcurrentHashMap<>();
    private final long id;
    private final int type;
    private final long startNode;
    private final long endNode;

    RelationshipData( long id, int type, long startNode, long endNode )
    {
        this.id = id;
        this.type = type;
        this.startNode = startNode;
        this.endNode = endNode;
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
        return id;
    }

    @Override
    public int type()
    {
        return type;
    }

    @Override
    public long startNode()
    {
        return startNode;
    }

    @Override
    public long endNode()
    {
        return endNode;
    }

    @Override
    public long otherNode( long nodeId )
    {
        if ( nodeId == startNode )
        {
            return endNode;
        }
        else if ( nodeId == endNode )
        {
            return startNode;
        }
        throw new IllegalArgumentException( nodeId + " neither start node " + startNode + " nor end node " + endNode + " for " + id );
    }

    @Override
    public long nextPropertyId()
    {
        return 0;
    }

    @Override
    public Lock lock()
    {
        return NO_LOCK;
    }
}
