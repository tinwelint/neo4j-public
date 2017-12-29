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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;

class SillyData
{
    SillyData( LabelTokenHolder labelTokens, PropertyKeyTokenHolder propertyKeyTokenHolder,
            RelationshipTypeTokenHolder relationshipTypeTokens )
    {
        this.labelTokens = labelTokens;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.relationshipTypeTokens = relationshipTypeTokens;
    }

    final LabelTokenHolder labelTokens;
    final PropertyKeyTokenHolder propertyKeyTokenHolder;
    final RelationshipTypeTokenHolder relationshipTypeTokens;

    final ConcurrentMap<Long,NodeData> nodes = new ConcurrentHashMap<>();
    final ConcurrentMap<Long,RelationshipData> relationships = new ConcurrentHashMap<>();
    final ConcurrentMap<Long,SchemaDescriptor.Supplier> schema = new ConcurrentHashMap<>();
    final ConcurrentMap<Integer,PropertyData> graphProperties = new ConcurrentHashMap<>();

    final AtomicLong nextNodeId = new AtomicLong();
    final AtomicLong nextRelationshipId = new AtomicLong();
    final AtomicLong nextSchemaId = new AtomicLong();
}
