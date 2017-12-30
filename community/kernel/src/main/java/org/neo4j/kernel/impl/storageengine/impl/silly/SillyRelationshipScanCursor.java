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
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.kernel.impl.storageengine.impl.silly.SillyStorageEngine.SillyCursorClient;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class SillyRelationshipScanCursor implements RelationshipScanCursor
{
    private final ConcurrentMap<Long,RelationshipData> relationships;
    private long next = NO_ID;
    private RelationshipData current;
    private SillyCursorClient cursors;

    SillyRelationshipScanCursor( ConcurrentMap<Long,RelationshipData> relationships )
    {
        this.relationships = relationships;
    }

    void single( SillyCursorClient cursors, long reference )
    {
        this.cursors = cursors;
        current = null;
        next = reference;
    }

    @Override
    public long relationshipReference()
    {
        return current.id();
    }

    @Override
    public int label()
    {
        return current.type();
    }

    @Override
    public boolean hasProperties()
    {
        return !current.properties().isEmpty();
    }

    @Override
    public void source( NodeCursor cursor )
    {
        ((SillyNodeCursor)cursor).single( cursors, sourceNodeReference() );
    }

    @Override
    public void target( NodeCursor cursor )
    {
        ((SillyNodeCursor)cursor).single( cursors, sourceNodeReference() );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        ((SillyPropertyCursor)cursor).init( current.properties() );
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
    public boolean next()
    {
        if ( next == NO_ID )
        {
            return false;
        }

        current = relationships.get( next );
        return current != null;
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
