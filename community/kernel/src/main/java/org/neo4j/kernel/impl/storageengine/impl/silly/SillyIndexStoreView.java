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

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator.MultipleIndexUpdater;
import org.neo4j.kernel.impl.api.index.NodeUpdates;
import org.neo4j.kernel.impl.api.index.NodeUpdates.Builder;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.register.Registers.newDoubleLongRegister;

class SillyIndexStoreView implements IndexStoreView
{
    private final SillyData data;

    SillyIndexStoreView( SillyData data )
    {
        this.data = data;
    }

    @Override
    public Value getPropertyValue( long nodeId, int propertyKeyId )
    {
        NodeData node = data.nodes.get( nodeId );
        if ( node != null )
        {
            PropertyData property = node.properties().get( propertyKeyId );
            if ( property != null )
            {
                return property.value();
            }
        }
        return Values.NO_VALUE;
    }

    @Override
    public void loadProperties( long nodeId, PrimitiveIntSet propertyIds, PropertyLoadSink sink )
    {
        NodeData node = data.nodes.get( nodeId );
        if ( node != null )
        {
            node.properties().forEach( ( key, property ) ->
            {
                if ( propertyIds.contains( key ) )
                {
                    sink.onProperty( key, property.value() );
                }
            } );
        }
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds, IntPredicate propertyKeyIdFilter,
            Visitor<NodeUpdates,FAILURE> propertyUpdateVisitor, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
            boolean forceStoreScan )
    {
        return new StoreScan<FAILURE>()
        {
            private final long[] sortedIds = sortedNodeIds();
            private int cursor;
            private volatile boolean stopped;

            @Override
            public void run() throws FAILURE
            {
                for ( ; cursor < sortedIds.length && !stopped; cursor++ )
                {
                    NodeData node = data.nodes.get( sortedIds[cursor] );
                    if ( node != null )
                    {
                        if ( matchesLabels( node, labelIds ) )
                        {
                            propertyUpdateVisitor.visit( nodeAsUpdates( node ) );
                        }
                    }
                }
            }

            private boolean matchesLabels( NodeData node, int[] labelIds )
            {
                for ( int labelId : labelIds )
                {
                    if ( node.hasLabel( labelId ) )
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void stop()
            {
                stopped = true;
            }

            @Override
            public void acceptUpdate( MultipleIndexUpdater updater, IndexEntryUpdate<?> update, long currentlyIndexedNodeId )
            {
                if ( update.getEntityId() <= currentlyIndexedNodeId )
                {
                    updater.process( update );
                }
            }

            @Override
            public PopulationProgress getProgress()
            {
                return new PopulationProgress( cursor, sortedIds.length );
            }
        };
    }

    private long[] sortedNodeIds()
    {
        long[] ids = new long[data.nodes.size()];
        int cursor = 0;
        Iterator<Long> iterator = data.nodes.keySet().iterator();
        while ( iterator.hasNext() )
        {
            long id = iterator.next();
            if ( cursor + 1 == ids.length )
            {
                ids = Arrays.copyOf( ids, cursor * 2 );
            }
            ids[cursor++] = id;
        }
        return ids.length != cursor ? Arrays.copyOf( ids, cursor ) : ids;
    }

    @Override
    public NodeUpdates nodeAsUpdates( long nodeId )
    {
        NodeData node = data.nodes.get( nodeId );
        return node != null ? nodeAsUpdates( node ) : null;
    }

    protected NodeUpdates nodeAsUpdates( NodeData node )
    {
        Builder builder = NodeUpdates.forNode( node.id(), node.labelArray() );
        Iterator<PropertyData> properties = node.properties().values().iterator();
        while ( properties.hasNext() )
        {
            PropertyData property = properties.next();
            builder = builder.added( property.propertyKeyId(), property.value() );
        }
        return builder.build();
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( long indexId, DoubleLongRegister output )
    {
        return newDoubleLongRegister();
    }

    @Override
    public DoubleLongRegister indexSample( long indexId, DoubleLongRegister output )
    {
        return newDoubleLongRegister();
    }

    @Override
    public void replaceIndexCounts( long indexId, long uniqueElements, long maxUniqueElements, long indexSize )
    {
    }

    @Override
    public void incrementIndexUpdates( long indexId, long updatesDelta )
    {
    }
}
