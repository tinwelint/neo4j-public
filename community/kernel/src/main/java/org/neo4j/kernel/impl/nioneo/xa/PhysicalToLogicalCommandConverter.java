/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.collection.NestingIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.api.index.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;

public class PhysicalToLogicalCommandConverter
{
    private final PropertyStore propertyStore;
    private final NodeStore nodeStore;

    public PhysicalToLogicalCommandConverter( PropertyStore propertyStore, NodeStore nodeStore )
    {
        this.propertyStore = propertyStore;
        this.nodeStore = nodeStore;
    }

    public Iterator<NodeLabelUpdate> nodeLabels( Iterator<Command.NodeCommand> commands )
    {
        return new NestingIterator<NodeLabelUpdate, Command.NodeCommand>( commands )
        {
            @Override
            protected Iterator<NodeLabelUpdate> createNestedIterator( Command.NodeCommand command )
            {
                return nodeLabelRecordsToUpdate( command.getBefore(), command.getAfter() );
            }
        };
    }

    public Iterator<NodePropertyUpdate> nodeProperties( Iterator<Command.PropertyCommand> propertyCommands )
    {
        return new NestingIterator<NodePropertyUpdate, Command.PropertyCommand>( propertyCommands )
        {
            @Override
            protected Iterator<NodePropertyUpdate> createNestedIterator( Command.PropertyCommand command )
            {
                return nodePropertyRecordsToUpdate( command.getBefore(), command.getAfter() );
            }
        };
    }

    private Iterator<NodeLabelUpdate> nodeLabelRecordsToUpdate( NodeRecord before, NodeRecord after )
    {

        final Set<Long> beforeLabels = asSet( asIterable( nodeStore.getLabelsForNode( before ) ) );
        final Set<Long> afterLabels  = asSet( asIterable( nodeStore.getLabelsForNode( after ) ) );

        final long nodeId = after.getId();
        final Iterator<Long> allLabels = union( beforeLabels, afterLabels ).iterator();

        return new PrefetchingIterator<NodeLabelUpdate>()
        {
            @Override
            protected NodeLabelUpdate fetchNextOrNull()
            {
                while(allLabels.hasNext())
                {
                    Long currentLabel = allLabels.next();
                    if(afterLabels.contains( currentLabel ))
                    {
                        if(!beforeLabels.contains( currentLabel ))
                        {
                            return new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, currentLabel );
                        }
                    } else {

                        return new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.REMOVE, currentLabel );
                    }
                }
                return null;
            }
        };
    }

    private Iterator<NodePropertyUpdate> nodePropertyRecordsToUpdate( PropertyRecord before, PropertyRecord after )
    {
        assert before.getNodeId() == after.getNodeId();
        long nodeId = before.getNodeId();
        Map<Integer, PropertyBlock> beforeMap = mapBlocks( before );
        Map<Integer, PropertyBlock> afterMap = mapBlocks( after );

        @SuppressWarnings( "unchecked" )
        Set<Integer> allKeys = union( beforeMap.keySet(), afterMap.keySet() );

        Collection<NodePropertyUpdate> result = new ArrayList<NodePropertyUpdate>();
        for ( int key : allKeys )
        {
            PropertyBlock beforeBlock = beforeMap.get( key );
            PropertyBlock afterBlock = afterMap.get( key );

            if ( beforeBlock != null && afterBlock != null )
            {
                if ( !beforeBlock.hasSameContentsAs( afterBlock ) )
                    result.add( new NodePropertyUpdate( nodeId, key, valueOf( beforeBlock ), valueOf( beforeBlock ) ) );
            }
            else
            {
                result.add( new NodePropertyUpdate( nodeId, key, valueOf( beforeBlock ), valueOf( afterBlock ) ) );
            }
        }
        return result.iterator();
    }

    private <T> Set<T> union( Set<T>... sets )
    {
        Set<T> union = new HashSet<T>();
        for ( Set<T> set : sets )
            union.addAll( set );
        return union;
    }

    private Map<Integer, PropertyBlock> mapBlocks( PropertyRecord before )
    {
        HashMap<Integer, PropertyBlock> map = new HashMap<Integer, PropertyBlock>();
        for ( PropertyBlock block : before.getPropertyBlocks() )
            map.put( block.getKeyIndexId(), block );
        return map;
    }

    private Object valueOf( PropertyBlock block )
    {
        if ( block == null )
            return null;

        return block.getType().getValue( block, propertyStore );
    }
}
