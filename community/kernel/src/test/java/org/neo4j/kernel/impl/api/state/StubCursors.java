/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;

/**
 * Stub cursors to be used for testing.
 */
public class StubCursors
{
    private StubCursors()
    {
    }

    public static RelationshipItem relationship( long id, int type, long start, long end )
    {
        return new RelationshipItem()
        {
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
                return start;
            }

            @Override
            public long endNode()
            {
                return end;
            }

            @Override
            public long otherNode( long nodeId )
            {
                if ( nodeId == start )
                {
                    return end;
                }
                else if ( nodeId == end )
                {
                    return start;
                }
                throw new IllegalStateException();
            }

            @Override
            public long nextPropertyId()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Lock lock()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static Cursor<RelationshipItem> asRelationshipCursor( final long relId, final int type,
            final long startNode, final long endNode, long propertyId )
    {
        return cursor( new RelationshipItem()
        {
            @Override
            public long id()
            {
                return relId;
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
                return startNode == nodeId ? endNode : startNode;
            }

            @Override
            public long nextPropertyId()
            {
                return propertyId;
            }

            @Override
            public Lock lock()
            {
                return NO_LOCK;
            }
        } );
    }

    public static MutableLongSet labels( final long... labels )
    {
        return LongHashSet.newSetWith( labels );
    }

    public static Cursor<PropertyItem> asPropertyCursor( final PropertyKeyValue... properties )
    {
        return cursor( map( StubCursors::asPropertyItem, Arrays.asList( properties ) ) );
    }

    private static PropertyItem asPropertyItem( final PropertyKeyValue property )
    {
        return new PropertyItem()
        {
            @Override
            public int propertyKeyId()
            {
                return property.propertyKeyId();
            }

            @Override
            public Value value()
            {
                return property.value();
            }
        };
    }

    @SafeVarargs
    public static <T> Cursor<T> cursor( final T... items )
    {
        return cursor( Iterables.asIterable( items ) );
    }

    public static <T> Cursor<T> cursor( final Iterable<T> items )
    {
        return new Cursor<T>()
        {
            Iterator<T> iterator = items.iterator();

            T current;

            @Override
            public boolean next()
            {
                if ( iterator.hasNext() )
                {
                    current = iterator.next();
                    return true;
                }
                else
                {
                    return false;
                }
            }

            @Override
            public void close()
            {
                iterator = items.iterator();
                current = null;
            }

            @Override
            public T get()
            {
                if ( current == null )
                {
                    throw new IllegalStateException();
                }

                return current;
            }
        };
    }
}
