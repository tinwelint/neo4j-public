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

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.Direction;

class SillyRelationshipTraversalCursor implements RelationshipTraversalCursor
{
    private ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> relationships;

    @Override
    public long relationshipReference()
    {
        return 0;
    }

    @Override
    public int type()
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

    void init( ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> relationships )
    {
        this.relationships = relationships;
    }
}
