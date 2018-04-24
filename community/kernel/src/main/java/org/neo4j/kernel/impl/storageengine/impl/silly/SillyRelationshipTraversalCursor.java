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
import java.util.concurrent.ConcurrentMap;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.Direction;

import static java.util.Collections.emptyIterator;

class SillyRelationshipTraversalCursor implements RelationshipTraversalCursor
{
    private ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> relationships;
    private Iterator<Map.Entry<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>>> typeIterator;
    private Iterator<Map.Entry<Direction,ConcurrentMap<Long,RelationshipData>>> directionIterator;
    private Map.Entry<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> currentType;
    private Map.Entry<Direction,ConcurrentMap<Long,RelationshipData>> currentDirection;
    private Iterator<RelationshipData> relationshipIterator;
    private RelationshipData current;
    private IntPredicate typeFilter;
    private Predicate<Direction> directionFilter;

    @Override
    public long relationshipReference()
    {
        return current.id();
    }

    @Override
    public int type()
    {
        return currentType.getKey();
    }

    @Override
    public boolean hasProperties()
    {
        return !current.properties().isEmpty();
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
        return current.startNode();
    }

    @Override
    public long targetNodeReference()
    {
        return current.endNode();
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
        while ( typeIterator.hasNext() || directionIterator.hasNext() || relationshipIterator.hasNext() )
        {
            if ( relationshipIterator.hasNext() )
            {
                current = relationshipIterator.next();
                return true;
            }
            else if ( directionIterator.hasNext() )
            {
                Map.Entry<Direction,ConcurrentMap<Long,RelationshipData>> candidate = directionIterator.next();
                if ( directionFilter.test( candidate.getKey() ) )
                {
                    currentDirection = candidate;
                    relationshipIterator = currentDirection.getValue().values().iterator();
                }
            }
            else if ( typeIterator.hasNext() )
            {
                Map.Entry<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> candidate = typeIterator.next();
                if ( typeFilter.test( candidate.getKey() ) )
                {
                    currentType = candidate;
                    directionIterator = currentType.getValue().entrySet().iterator();
                }
            }
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

    void init( ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> relationships, IntPredicate typeFilter,
            Predicate<Direction> directionFilter )
    {
        this.relationships = relationships;
        this.typeFilter = typeFilter;
        this.directionFilter = directionFilter;
        this.typeIterator = relationships.entrySet().iterator();
        this.directionIterator = emptyIterator();
        this.relationshipIterator = emptyIterator();
    }
}
