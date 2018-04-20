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

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

class AllRelationshipsIterator extends PrimitiveLongCollections.PrimitiveLongBaseIterator implements RelationshipIterator
{
    private final ConcurrentMap<Long,RelationshipData> relationships;
    private final Iterator<Long> iterator;

    public AllRelationshipsIterator( ConcurrentMap<Long,RelationshipData> relationships )
    {
        this.relationships = relationships;
        this.iterator = relationships.keySet().iterator();
    }

    @Override
    protected boolean fetchNext()
    {
        return iterator.hasNext() ? next( iterator.next() ) : false;
    }

    @Override
    public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId, RelationshipVisitor<EXCEPTION> visitor )
            throws EXCEPTION
    {
        return relationships.get( relationshipId ).visit( visitor );
    }
}
