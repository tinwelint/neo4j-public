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

import java.util.concurrent.ConcurrentMap;

import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.Direction;

class SillyRelationshipGroupCursor implements RelationshipGroupCursor
{
    private ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> relationships;

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

    void init( ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> relationships )
    {
        this.relationships = relationships;
    }
}
