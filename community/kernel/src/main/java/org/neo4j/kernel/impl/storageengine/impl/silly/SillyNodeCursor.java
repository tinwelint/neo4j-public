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

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.impl.storageengine.impl.silly.SillyStorageEngine.SillyCursorClient;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.NodeState;
import static org.neo4j.helpers.collection.Iterators.loop;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class SillyNodeCursor implements NodeCursor
{
    private final ConcurrentMap<Long,NodeData> nodes;
    private long next = NO_ID;
    private NodeData current;
    private SillyCursorClient cursors;

    SillyNodeCursor( ConcurrentMap<Long,NodeData> nodes )
    {
        this.nodes = nodes;
    }

    @Override
    public boolean next()
    {
        if ( next == NO_ID )
        {
            return false;
        }

        current = nodes.get( next );
        // Do a silly and horrible eager merge with tx state for simplicity
        if ( cursors.hasTxStateWithChanges() )
        {
            NodeState nodeState = cursors.txState().getNodeState( next );
            if ( !nodeState.labelDiffSets().isEmpty() || nodeState.hasPropertyChanges() )
            {
                current = current != null ? current.copy() : new NodeData( next );
                if ( !nodeState.labelDiffSets().isEmpty() )
                {
                    for ( int labelId : nodeState.labelDiffSets().getRemoved() )
                    {
                        current.labels().remove( labelId );
                    }
                    for ( int labelId : nodeState.labelDiffSets().getAdded() )
                    {
                        current.labels().add( labelId );
                    }
                }

                if ( nodeState.hasPropertyChanges() )
                {
                    for ( int key : loop( nodeState.removedProperties() ) )
                    {
                        current.properties().remove( key );
                    }
                    for ( StorageProperty property : loop( nodeState.addedProperties() ) )
                    {
                        current.properties().put( property.propertyKeyId(), new PropertyData( property.propertyKeyId(), property.value() ) );
                    }
                }

                // relationship tx changes aren't plugged in yet
            }
        }
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

    void single( SillyCursorClient cursors, long nodeId )
    {
        this.cursors = cursors;
        next = nodeId;
    }

    @Override
    public long nodeReference()
    {
        return current.id();
    }

    @Override
    public LabelSet labels()
    {
        return current.labelSet();
    }

    @Override
    public boolean hasProperties()
    {
        return !current.properties().isEmpty();
    }

    @Override
    public void relationships( RelationshipGroupCursor cursor )
    {
        ((SillyRelationshipGroupCursor)cursor).init( current.relationships() );
    }

    @Override
    public void allRelationships( RelationshipTraversalCursor cursor )
    {
        ((SillyRelationshipTraversalCursor)cursor).init( current.relationships() );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        ((SillyPropertyCursor)cursor).init( current.properties() );
    }

    @Override
    public long relationshipGroupReference()
    {
        return 0;
    }

    @Override
    public long allRelationshipsReference()
    {
        return 0;
    }

    @Override
    public long propertiesReference()
    {
        return 0;
    }

    @Override
    public boolean isDense()
    {
        return true;
    }
}
