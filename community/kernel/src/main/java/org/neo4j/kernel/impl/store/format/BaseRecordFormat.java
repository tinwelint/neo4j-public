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

import java.util.function.Function;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;

/**
 * Basic abstract implementation of a {@link RecordFormat} implementing most functionality except
 * {@link RecordFormat#read(AbstractBaseRecord, PageCursor, org.neo4j.kernel.impl.store.record.RecordLoad, int)} and
 * {@link RecordFormat#write(AbstractBaseRecord, PageCursor, int)}.
 *
 * @param <RECORD> type of record.
 */
public abstract class BaseRecordFormat<RECORD extends AbstractBaseRecord> implements RecordFormat<RECORD>
{
    public static final int IN_USE_BIT = 0b0000_0001;
    public static final Function<StoreHeader,Integer> INT_STORE_HEADER_READER =
            (header) -> ((IntStoreHeader)header).value();
    /**
     * Reserved ID which is used in some record implementations
     */
    public static final long INTEGER_MINUS_ONE = 0xFFFFFFFFL;  // 4294967295L;

    public static Function<StoreHeader,Integer> fixedRecordSize( int recordSize )
    {
        return (header) -> recordSize;
    }

    private final Function<StoreHeader,Integer> recordSize;
    private final int recordHeaderSize;
    private final long maxId;
    private final int minId;

    protected BaseRecordFormat( Function<StoreHeader,Integer> recordSize, int recordHeaderSize, int idBits, int minId )
    {
        this.recordSize = recordSize;
        this.recordHeaderSize = recordHeaderSize;
        this.minId = minId;
        this.maxId = (1L << idBits) - 1;
    }

    @Override
    public int getRecordSize( StoreHeader header )
    {
        return recordSize.apply( header );
    }

    @Override
    public int getRecordHeaderSize()
    {
        return recordHeaderSize;
    }

    @Override
    public long getNextRecordReference( RECORD record )
    {
        return Record.NULL_REFERENCE.intValue();
    }

    /**
     * Formats using this has got a special ID (0xFFFFFFFF - 1) marking a null ID, so special checking is
     * required when reading.
     *
     * @param base low 4 bytes
     * @param modifier high bits
     * @return combined base and modified into ID, or -1 if combination is the special ID.
     */
    public static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == INTEGER_MINUS_ONE ? Record.NULL_REFERENCE.longValue() :
                zeroBasedLongFromIntAndMod( base, modifier );
    }

    /**
     * Formats using this should also have {@link Capability#ID_ZERO_RESERVED}, i.e. not have the special
     * reserved (0xFFFFFFFF - 1) ID for null, instead 0. Reading 0, e.g. base == 0 && modifier == 0 will
     * have this method return -1 since on {@link AbstractBaseRecord record-instance-level} we still compare
     * with {@link Record#NULL_REFERENCE} which isn't tied to a particular format.
     *
     * @param base low 4 bytes
     * @param modifier high bits
     * @return combined base and modified into ID, or -1 if both base and modifier are 0.
     */
    public static long zeroBasedLongFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == 0 ? Record.NULL_REFERENCE.longValue() : base | modifier;
    }

    @Override
    public void prepare( RECORD record, int recordSize, IdSequence idSequence )
    {   // Do nothing by default
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && getClass().equals( obj.getClass() );
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public final long getMaxId()
    {
        return maxId;
    }

    @Override
    public int getMinId()
    {
        return minId;
    }
}
