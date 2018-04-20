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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.WritableChannel;

abstract class SillyStorageCommand implements StorageCommand
{
    @Override
    public void serialize( WritableChannel channel ) throws IOException
    {
    }

    abstract void applyTo( SillyData data );

    static class DropIndex extends SillyStorageCommand
    {
        private final SchemaIndexDescriptor index;

        DropIndex( SchemaIndexDescriptor index )
        {
            this.index = index;
        }

        @Override
        void applyTo( SillyData data )
        {
            Iterator<Entry<Long,SchemaDescriptorSupplier>> entries = data.schema.entrySet().iterator();
            while ( entries.hasNext() )
            {
                Entry<Long,SchemaDescriptorSupplier> entry = entries.next();
                if ( entry.getValue().equals( index ) )
                {
                    data.schema.remove( entry.getKey() );
                    break;
                }
            }
        }
    }

    static class DropConstraint extends SillyStorageCommand
    {
        private final ConstraintDescriptor constraint;

        DropConstraint( ConstraintDescriptor constraint )
        {
            this.constraint = constraint;
        }

        @Override
        void applyTo( SillyData data )
        {
            Iterator<Entry<Long,SchemaDescriptorSupplier>> entries = data.schema.entrySet().iterator();
            while ( entries.hasNext() )
            {
                Entry<Long,SchemaDescriptorSupplier> entry = entries.next();
                if ( entry.getValue().equals( constraint ) )
                {
                    data.schema.remove( entry.getKey() );
                    break;
                }
            }
        }
    }

    static class CreateIndex extends SillyStorageCommand
    {
        private final long id;
        private final SchemaIndexDescriptor index;

        CreateIndex( long id, SchemaIndexDescriptor index )
        {
            this.id = id;
            this.index = index;
        }

        @Override
        void applyTo( SillyData data )
        {
            data.schema.put( id, index );
        }
    }

    static class CreateConstraint extends SillyStorageCommand
    {
        private final long id;
        private final ConstraintDescriptor constraint;

        CreateConstraint( long id, ConstraintDescriptor constraint )
        {
            this.id = id;
            this.constraint = constraint;
        }

        @Override
        void applyTo( SillyData data )
        {
            data.schema.put( id, constraint );
        }
    }

    static class SetRelationshipProperty extends SillyStorageCommand
    {
        private final long id;
        private final StorageProperty property;

        SetRelationshipProperty( long id, StorageProperty property )
        {
            this.id = id;
            this.property = property;
        }

        @Override
        void applyTo( SillyData data )
        {
            RelationshipData rel = data.relationships.get( id );
            rel.properties().put( property.propertyKeyId(), new PropertyData( property.propertyKeyId(), property.value() ) );
        }
    }

    static class RemoveRelationshipProperty extends SillyStorageCommand
    {
        private final long id;
        private final int key;

        RemoveRelationshipProperty( long id, int key )
        {
            this.id = id;
            this.key = key;
        }

        @Override
        void applyTo( SillyData data )
        {
            RelationshipData rel = data.relationships.get( id );
            rel.properties().remove( key );
        }
    }

    static class SetGraphProperty extends SillyStorageCommand
    {
        private final StorageProperty property;

        SetGraphProperty( StorageProperty property )
        {
            this.property = property;
        }

        @Override
        void applyTo( SillyData data )
        {
            data.graphProperties.put( property.propertyKeyId(), new PropertyData( property.propertyKeyId(), property.value() ) );
        }
    }

    static class RemoveGraphProperty extends SillyStorageCommand
    {
        private final int key;

        RemoveGraphProperty( int key )
        {
            this.key = key;
        }

        @Override
        void applyTo( SillyData data )
        {
            data.graphProperties.remove( key );
        }
    }

    static class SetNodeProperty extends SillyStorageCommand
    {
        private final long id;
        private final StorageProperty property;

        SetNodeProperty( long id, StorageProperty property )
        {
            this.id = id;
            this.property = property;
        }

        @Override
        void applyTo( SillyData data )
        {
            NodeData node = data.nodes.get( id );
            node.properties().put( property.propertyKeyId(), new PropertyData( property.propertyKeyId(), property.value() ) );
        }
    }

    static class RemoveNodeProperty extends SillyStorageCommand
    {
        private final long id;
        private final int key;

        RemoveNodeProperty( long id, int key )
        {
            this.id = id;
            this.key = key;
        }

        @Override
        void applyTo( SillyData data )
        {
            NodeData node = data.nodes.get( id );
            node.properties().remove( key );
        }
    }

    static class ChangeLabels extends SillyStorageCommand
    {
        private final long nodeId;
        private final Set<Integer> added;
        private final Set<Integer> removed;

        ChangeLabels( long nodeId, Set<Integer> added, Set<Integer> removed )
        {
            this.nodeId = nodeId;
            this.added = added;
            this.removed = removed;
        }

        @Override
        void applyTo( SillyData data )
        {
            NodeData node = data.nodes.get( nodeId );
            PrimitiveIntSet labels = node.labels();
            added.forEach( labels::add );
            removed.forEach( labels::remove );
        }
    }

    static class DeleteRelationship extends SillyStorageCommand
    {
        private final long id;
        private final int type;
        private final long startNode;
        private final long endNode;

        DeleteRelationship( long id, int type, long startNode, long endNode )
        {
            this.id = id;
            this.type = type;
            this.startNode = startNode;
            this.endNode = endNode;
        }

        @Override
        void applyTo( SillyData data )
        {
            data.relationships.remove( id );
            removeAll( data.nodes.get( startNode ).relationships().get( type ) );
            removeAll( data.nodes.get( endNode ).relationships().get( type ) );
        }

        private void removeAll( ConcurrentMap<Direction,ConcurrentMap<Long,RelationshipData>> types )
        {
            safeRemove( types.get( Direction.OUTGOING ) );
            safeRemove( types.get( Direction.INCOMING ) );
            safeRemove( types.get( Direction.BOTH ) );
        }

        private void safeRemove( ConcurrentMap<Long,RelationshipData> concurrentMap )
        {
            if ( concurrentMap != null )
            {
                concurrentMap.remove( id );
            }
        }
    }

    static class CreateRelationship extends SillyStorageCommand
    {
        private final long id;
        private final int type;
        private final long startNode;
        private final long endNode;

        CreateRelationship( long id, int type, long startNode, long endNode )
        {
            this.id = id;
            this.type = type;
            this.startNode = startNode;
            this.endNode = endNode;
        }

        @Override
        void applyTo( SillyData data )
        {
            RelationshipData rel = new RelationshipData( id, type, startNode, endNode );
            data.relationships.put( id, rel );

            NodeData start = data.nodes.computeIfAbsent( startNode, nodeId -> new NodeData( nodeId ) );
            Direction startDir = startNode == endNode ? Direction.BOTH : Direction.OUTGOING;
            start.relationships().computeIfAbsent( type, t -> new ConcurrentHashMap<>() )
                .computeIfAbsent( startDir, t -> new ConcurrentHashMap<>() ).put( id, rel );

            NodeData end = data.nodes.computeIfAbsent( endNode, nodeId -> new NodeData( nodeId ) );
            Direction endDir = startNode == endNode ? Direction.BOTH : Direction.INCOMING;
            end.relationships().computeIfAbsent( type, t -> new ConcurrentHashMap<>() )
                .computeIfAbsent( endDir, t -> new ConcurrentHashMap<>() ).put( id, rel );
        }
    }

    static class DeleteNode extends SillyStorageCommand
    {
        private final long id;

        DeleteNode( long id )
        {
            this.id = id;
        }

        @Override
        void applyTo( SillyData data )
        {
            data.nodes.remove( id );
        }
    }

    static class CreateNode extends SillyStorageCommand
    {
        private final long id;

        CreateNode( long id )
        {
            this.id = id;
        }

        @Override
        void applyTo( SillyData data )
        {
            data.nodes.putIfAbsent( id, new NodeData( id ) );
        }
    }

    static class CreateRelationshipTypeToken extends SillyStorageCommand
    {
        private final String name;
        private final int id;

        CreateRelationshipTypeToken( String name, int id )
        {
            this.name = name;
            this.id = id;
        }

        @Override
        void applyTo( SillyData data )
        {
            data.relationshipTypeTokens.addToken( new RelationshipTypeToken( name, id ) );
        }
    }

    static class CreateLabelToken extends SillyStorageCommand
    {
        private final String name;
        private final int id;

        CreateLabelToken( String name, int id )
        {
            this.name = name;
            this.id = id;
        }

        @Override
        void applyTo( SillyData data )
        {
            data.labelTokens.addToken( new Token( name, id ) );
        }
    }

    static class CreatePropertyKeyToken extends SillyStorageCommand
    {
        private final String name;
        private final int id;

        CreatePropertyKeyToken( String name, int id )
        {
            this.name = name;
            this.id = id;
        }

        @Override
        void applyTo( SillyData data )
        {
            data.propertyKeyTokenHolder.addToken( new Token( name, id ) );
        }
    }
}
