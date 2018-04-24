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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.NodeState;

import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.Iterators.loop;
import static org.neo4j.kernel.impl.storageengine.impl.silly.SillyData.mergeProperties;

class SillyNodeCursor implements NodeCursor
{
    private final ConcurrentMap<Long,NodeData> nodes;
    private Iterator<NodeData> iterator;
    private NodeData current;
    private SillyCursorFactory cursors;

    SillyNodeCursor( ConcurrentMap<Long,NodeData> nodes )
    {
        this.nodes = nodes;
    }

    @Override
    public boolean next()
    {
        if ( !iterator.hasNext() )
        {
            return false;
        }

        current = iterator.next();
        return current != null;
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

    void single( SillyCursorFactory cursors, long nodeId )
    {
        this.cursors = cursors;
        current = null;
        NodeData nodeData = nodes.get( nodeId );
        // Do a silly and horrible eager merge with tx state for simplicity
        nodeData = decorateNode( nodeData, nodeId );
        iterator = iterator( nodeData );
    }

    void scan( SillyCursorFactory cursors )
    {
        this.cursors = cursors;
        current = null;
        iterator = nodes.values().iterator();
    }

    private NodeData decorateNode( NodeData node, long nodeId )
    {
        if ( cursors.hasTxStateWithChanges() )
        {
            NodeState nodeState = cursors.txState().getNodeState( nodeId );
            node = node != null ? node.copy() : new NodeData( nodeId );
            if ( !nodeState.labelDiffSets().isEmpty() || nodeState.hasPropertyChanges() )
            {
                if ( !nodeState.labelDiffSets().isEmpty() )
                {
                    for ( int labelId : nodeState.labelDiffSets().getRemoved() )
                    {
                        node.labels().remove( labelId );
                    }
                    for ( int labelId : nodeState.labelDiffSets().getAdded() )
                    {
                        node.labels().add( labelId );
                    }
                }
                mergeProperties( node.properties(), nodeState );
            }
        }
        return node;
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
        ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> rels = current.relationships();
        if ( cursors.hasTxStateWithChanges() )
        {
            NodeState nodeState = cursors.txState().getNodeState( current.id() );
            rels = mergeRelationships( rels, nodeState );
        }

        ((SillyRelationshipGroupCursor)cursor).init( rels );
    }

    private ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> mergeRelationships(
            ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> rels, NodeState nodeState )
    {
        // Deep-copy w/ removals from tx state
        ConcurrentMap<Integer,ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>>> merged = new ConcurrentHashMap<>();
        rels.forEach( ( type, level1 ) ->
        {
            ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>> copy1 = new ConcurrentHashMap<>();
            merged.put( type, copy1 );
            level1.forEach( ( direction, level2 ) ->
            {
                ConcurrentMap<Long,RelationshipData> copy2 = new ConcurrentHashMap<>();
                copy1.put( direction, copy2 );
                level2.forEach( ( id, level3 ) ->
                {
                    if ( !cursors.txState().relationshipIsDeletedInThisTx( id ) )
                    {
                        copy2.put( id, new RelationshipData( id, cursors.txState().getRelationshipState( id ) ) );
                    }
                } );
            } );
        } );

        // Add the added relationships
        PrimitiveLongIterator added = nodeState.getAddedRelationships();
        while ( added.hasNext() )
        {
            long id = added.next();
            RelationshipData relationshipData = new RelationshipData( id, cursors.txState().getRelationshipState( id ) );
            Direction direction = relationshipData.directionFor( current.id() );
            merged.computeIfAbsent( relationshipData.type(), type -> new ConcurrentHashMap<>() ).computeIfAbsent( direction,
                    dir -> new ConcurrentHashMap<>() ).put( id, relationshipData );
        }

        return merged;
    }

    @Override
    public void allRelationships( RelationshipTraversalCursor cursor )
    {
        ((SillyRelationshipTraversalCursor)cursor).init( current.relationships(), type -> true, dir -> true );
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
