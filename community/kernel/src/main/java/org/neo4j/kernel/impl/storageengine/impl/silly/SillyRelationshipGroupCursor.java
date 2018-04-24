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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.impl.api.ExplicitIndexApplierLookup;
import org.neo4j.storageengine.api.Direction;

class SillyRelationshipGroupCursor implements RelationshipGroupCursor
{
    private ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> relationships;
    private Iterator<Map.Entry<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>>> iterator;
    private Map.Entry<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> current;

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
        if ( iterator.hasNext() )
        {
            current = iterator.next();
            return true;
        }
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
    public boolean seek( int relationshipLabel )
    {
        return false;
    }

    @Override
    public int type()
    {
        return current.getKey();
    }

    private int countOf( ConcurrentMap<Long,RelationshipData> relationships )
    {
        return relationships != null ? relationships.size() : 0;
    }

    @Override
    public int outgoingCount()
    {
        return countOf( current.getValue().get( Direction.OUTGOING ) );
    }

    @Override
    public int incomingCount()
    {
        return countOf( current.getValue().get( Direction.INCOMING ) );
    }

    @Override
    public int loopCount()
    {
        return countOf( current.getValue().get( Direction.BOTH ) );
    }

    @Override
    public int totalCount()
    {
        return countOf( current.getValue().get( Direction.OUTGOING ) ) +
                countOf( current.getValue().get( Direction.INCOMING ) ) +
                countOf( current.getValue().get( Direction.BOTH ) );
    }

    @Override
    public void outgoing( RelationshipTraversalCursor cursor )
    {
        int theType = current.getKey();
        ((SillyRelationshipTraversalCursor) cursor).init( relationships, type -> type == theType, dir -> dir == Direction.OUTGOING );
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
        int theType = current.getKey();
        ((SillyRelationshipTraversalCursor) cursor).init( relationships, type -> type == theType, dir -> dir == Direction.INCOMING );
    }

    @Override
    public void loops( RelationshipTraversalCursor cursor )
    {
        int theType = current.getKey();
        ((SillyRelationshipTraversalCursor) cursor).init( relationships, type -> type == theType, dir -> dir == Direction.BOTH );
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

    void init( ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> relationships )
    {
        this.relationships = relationships;
        this.iterator = relationships.entrySet().iterator();
    }
}
