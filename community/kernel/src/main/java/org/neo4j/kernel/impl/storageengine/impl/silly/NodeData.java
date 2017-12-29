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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiPredicate;
import java.util.function.IntPredicate;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.helpers.collection.NestingIterator;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.newapi.Labels;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;

class NodeData implements NodeItem
{
    private final long id;
    private final PrimitiveIntSet labels = Primitive.intSet();
    private final ConcurrentMap<Integer,PropertyData> properties = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> relationships =
            new ConcurrentHashMap<>();

    NodeData( long id )
    {
        this.id = id;
    }

    @Override
    public long id()
    {
        return id;
    }

    LabelSet labelSet()
    {
        synchronized ( labels )
        {
            return Labels.from( labels );
        }
    }

    @Override
    public PrimitiveIntSet labels()
    {
        return labels;
    }

    ConcurrentMap<Integer,PropertyData> properties()
    {
        return properties;
    }

    ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> relationships()
    {
        return relationships;
    }

    @Override
    public boolean isDense()
    {
        return true;
    }

    @Override
    public boolean hasLabel( int labelId )
    {
        return labels.contains( labelId );
    }

    @Override
    public long nextGroupId()
    {
        return 0;
    }

    @Override
    public long nextRelationshipId()
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
        return NO_LOCK;
    }

    Iterator<RelationshipData> relationships( Direction direction )
    {
        return relationships( ( type, dir ) -> direction.matches( dir ) );
    }

    Iterator<RelationshipData> relationships( Direction direction, IntPredicate typeIds )
    {
        return relationships( ( type, dir ) -> direction.matches( dir ) && typeIds.test( type ) );
    }

    private Iterator<RelationshipData> relationships( BiPredicate<Integer,Direction> specification )
    {
        List<Collection<RelationshipData>> hits = new ArrayList<>();
        relationships.forEach( ( type, datas ) ->
        {
            datas.forEach( ( direction, data ) ->
            {
                if ( specification.test( type, direction ) )
                {
                    hits.add( data.values() );
                }
            } );
        } );

        return new NestingIterator<RelationshipData,Collection<RelationshipData>>( hits.iterator() )
        {
            @Override
            protected Iterator<RelationshipData> createNestedIterator( Collection<RelationshipData> item )
            {
                return item.iterator();
            }
        };
    }

    PrimitiveIntSet types()
    {
        PrimitiveIntSet types = Primitive.intSet( relationships.size() );
        relationships.keySet().forEach( key -> types.add( key ) );
        return types;
    }

    void visitDegrees( DegreeVisitor visitor )
    {
        relationships.forEach( ( type, datas ) ->
        {
            long loop = safeSizeOf( datas.get( Direction.BOTH ) );
            long out = safeSizeOf( datas.get( Direction.OUTGOING ) ) + loop;
            long in = safeSizeOf( datas.get( Direction.INCOMING ) ) + loop;
            visitor.visitDegree( type, out, in );
        } );
    }

    private static long safeSizeOf( ConcurrentMap<Long,RelationshipData> concurrentMap )
    {
        return concurrentMap != null ? concurrentMap.size() : 0;
    }
}
