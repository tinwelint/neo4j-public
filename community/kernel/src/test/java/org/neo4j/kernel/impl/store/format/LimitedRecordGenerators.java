/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StandaloneDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.test.Randoms;

import static java.lang.Long.max;
import static java.lang.Math.abs;
import static java.lang.Math.toIntExact;

public class LimitedRecordGenerators implements RecordGenerators
{
    static final long NULL = -1;
    public static final float DEFAULT_FRACTION_NULL = 0.2f;

    private final Randoms random;
    private final long nullValue;
    private final float fractionNullValues;
    private final RecordFormat<RelationshipTypeTokenRecord> relationshipTypeTokenFormat;
    private final RecordFormat<RelationshipTypeTokenRecord> propertyKeyTokenFormat;
    private final RecordFormat<RelationshipTypeTokenRecord> labelTokenFormat;
    private final RecordFormat<NodeRecord> nodeFormat;
    private final RecordFormat<RelationshipRecord> relationshipFormat;
    private final RecordFormat<RelationshipGroupRecord> relationshipGroupFormat;
    private final RecordFormat<DynamicRecord> dynamicFormat;
    private final RecordFormat<PropertyRecord> propertyFormat;

    public LimitedRecordGenerators( Randoms random, long nullValue, float fractionNullValues, RecordFormats formats )
    {
        this.random = random;
        this.nullValue = nullValue;
        this.fractionNullValues = fractionNullValues;

        this.relationshipTypeTokenFormat = formats.relationshipTypeToken();
        this.propertyKeyTokenFormat = formats.relationshipTypeToken();
        this.labelTokenFormat = formats.relationshipTypeToken();
        this.nodeFormat = formats.node();
        this.relationshipFormat = formats.relationship();
        this.relationshipGroupFormat = formats.relationshipGroup();
        this.dynamicFormat = formats.dynamic();
        this.propertyFormat = formats.property();
    }

    @Override
    public Generator<RelationshipTypeTokenRecord> relationshipTypeToken()
    {
        return (recordSize, format, recordId) -> new RelationshipTypeTokenRecord( toIntExact( recordId ) ).initialize(
                random.nextBoolean(),
                randomInt( relationshipTypeTokenFormat ) );
    }

    @Override
    public Generator<RelationshipGroupRecord> relationshipGroup()
    {
        return (recordSize, format, recordId) -> new RelationshipGroupRecord( recordId ).initialize(
                random.nextBoolean(),
                randomInt( relationshipTypeTokenFormat ),
                randomLongOrOccasionallyNull( relationshipFormat ),
                randomLongOrOccasionallyNull( relationshipFormat ),
                randomLongOrOccasionallyNull( relationshipFormat ),
                randomLongOrOccasionallyNull( nodeFormat ),
                randomLongOrOccasionallyNull( relationshipGroupFormat ) );
    }

    @Override
    public Generator<RelationshipRecord> relationship()
    {
        return (recordSize, format, recordId) -> new RelationshipRecord( recordId ).initialize(
                random.nextBoolean(),
                randomLongOrOccasionallyNull( propertyFormat ),
                randomLong( nodeFormat ), randomLong( nodeFormat ), randomInt( relationshipTypeTokenFormat ),
                randomLongOrOccasionallyNull( relationshipFormat ), randomLongOrOccasionallyNull( relationshipFormat ),
                randomLongOrOccasionallyNull( relationshipFormat ), randomLongOrOccasionallyNull( relationshipFormat ),
                random.nextBoolean(), random.nextBoolean() );
    }

    @Override
    public Generator<PropertyKeyTokenRecord> propertyKeyToken()
    {
        return (recordSize, format, recordId) -> new PropertyKeyTokenRecord( toIntExact( recordId ) ).initialize(
                random.nextBoolean(),
                randomInt( propertyKeyTokenFormat ),
                abs( random.nextInt() ) );
    }

    @Override
    public Generator<PropertyRecord> property()
    {
        return (recordSize, format, recordId) -> {
            PropertyRecord record = new PropertyRecord( recordId );
            int maxProperties = random.intBetween( 1, 4 );
            StandaloneDynamicRecordAllocator stringAllocator = new StandaloneDynamicRecordAllocator();
            StandaloneDynamicRecordAllocator arrayAllocator = new StandaloneDynamicRecordAllocator();
            record.setInUse( true );
            int blocksOccupied = 0;
            for ( int i = 0; i < maxProperties && blocksOccupied < 4; )
            {
                PropertyBlock block = new PropertyBlock();
                // Dynamic records will not be written and read by the property record format,
                // that happens in the store where it delegates to a "sub" store.
                PropertyStore.encodeValue( block, randomInt( propertyKeyTokenFormat ), random.propertyValue(),
                        stringAllocator, arrayAllocator );
                int tentativeBlocksWithThisOne = blocksOccupied + block.getValueBlocks().length;
                if ( tentativeBlocksWithThisOne <= 4 )
                {
                    record.addPropertyBlock( block );
                    blocksOccupied = tentativeBlocksWithThisOne;
                }
            }
            record.setPrevProp( randomLongOrOccasionallyNull( propertyFormat ) );
            record.setNextProp( randomLongOrOccasionallyNull( propertyFormat ) );
            return record;
        };
    }

    @Override
    public Generator<NodeRecord> node()
    {
        return (recordSize, format, recordId) -> new NodeRecord( recordId ).initialize(
                random.nextBoolean(),
                randomLongOrOccasionallyNull( propertyFormat ),
                random.nextBoolean(),
                randomLongOrOccasionallyNull( relationshipFormat ),
                randomLongOrOccasionallyNull( dynamicFormat, 0 ) );
    }

    @Override
    public Generator<LabelTokenRecord> labelToken()
    {
        return (recordSize, format, recordId) -> new LabelTokenRecord( toIntExact( recordId ) ).initialize(
                random.nextBoolean(),
                randomInt( labelTokenFormat ) );
    }

    @Override
    public Generator<DynamicRecord> dynamic()
    {
        return (recordSize, format, recordId) -> {
            int dataSize = recordSize - format.getRecordHeaderSize();
            int length = random.nextBoolean() ? dataSize : random.nextInt( dataSize );
            long next = length == dataSize ? randomLong( propertyFormat ) : nullValue;
            DynamicRecord record = new DynamicRecord( max( 1, recordId ) ).initialize( random.nextBoolean(),
                    random.nextBoolean(), next, random.nextInt( PropertyType.values().length ), length );
            byte[] data = new byte[record.getLength()];
            random.nextBytes( data );
            record.setData( data );
            return record;
        };
    }

    private int randomInt( RecordFormat<?> format )
    {
        return random.nextInt( toIntExact( format.getMaxId() - format.getMinId() ) ) + format.getMinId();
    }

    private long randomLong( RecordFormat<?> format )
    {
        return random.nextLong( format.getMaxId() - format.getMinId() ) + format.getMinId();
    }

    private long randomLongOrOccasionallyNull( RecordFormat<?> format )
    {
        return randomLongOrOccasionallyNull( format, NULL );
    }

    private long randomLongOrOccasionallyNull( RecordFormat<?> format, long nullValue )
    {
        return random.nextFloat() < fractionNullValues ? nullValue : randomLong( format );
    }
}
