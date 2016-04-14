/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitDecode;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitEncode;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitLength;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitSetHeader;

/**
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * 2B   relationship type
 * VB   first outgoing relationships
 * VB   first incoming relationships
 * VB   first loop relationships
 * VB   owning node
 * VB   next relationship group record
 *
 * => 18B-43B
 */
class RelationshipGroupRecordFormat extends BaseHighLimitRecordFormat<RelationshipGroupRecord>
{
    static final int RECORD_SIZE = 32;

    // 2-byte header bits specific to this format
    private static final int OUTGOING_ENCODING = 2; //0b0000_0000__0000_1100;
    private static final int INCOMING_ENCODING = 4; //0b0000_0000__0011_0000;
    private static final int LOOP_ENCODING     = 6; //0b0000_0000__1100_0000;
    private static final int OWNER_ENCODING    = 8; //0b0000_0011__0000_0000;
    private static final int NEXT_ENCODING     = 10;//0b0000_1100__0000_0000;

    public RelationshipGroupRecordFormat()
    {
        this( RECORD_SIZE );
    }

    RelationshipGroupRecordFormat( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0, true );
    }

    @Override
    public RelationshipGroupRecord newRecord()
    {
        return new RelationshipGroupRecord( -1 );
    }

    @Override
    protected void doReadInternal( RelationshipGroupRecord record, PageCursor cursor, int recordSize, long header,
                                   boolean inUse )
    {
        record.initialize( inUse,
                cursor.getShort() & 0xFFFF,
                _2bitDecode( cursor, NULL, header, OUTGOING_ENCODING ),
                _2bitDecode( cursor, NULL, header, INCOMING_ENCODING ),
                _2bitDecode( cursor, NULL, header, LOOP_ENCODING ),
                _2bitDecode( cursor, NULL, header, OWNER_ENCODING ),
                _2bitDecode( cursor, NULL, header, NEXT_ENCODING ) );
    }

    @Override
    protected long headerBits( RelationshipGroupRecord record )
    {
        long header = 0;
        header = _2bitSetHeader( record.getFirstOut(), NULL, header, OUTGOING_ENCODING );
        header = _2bitSetHeader( record.getFirstIn(), NULL, header, INCOMING_ENCODING );
        header = _2bitSetHeader( record.getFirstLoop(), NULL, header, LOOP_ENCODING );
        header = _2bitSetHeader( record.getOwningNode(), NULL, header, OWNER_ENCODING );
        header = _2bitSetHeader( record.getNext(), NULL, header, NEXT_ENCODING );
        return header;
    }

    @Override
    protected int requiredDataLength( RelationshipGroupRecord record )
    {
        return  2 + // type
                _2bitLength( record.getFirstOut(), NULL ) +
                _2bitLength( record.getFirstIn(), NULL ) +
                _2bitLength( record.getFirstLoop(), NULL ) +
                _2bitLength( record.getOwningNode(), NULL ) +
                _2bitLength( record.getNext(), NULL );
    }

    @Override
    protected void doWriteInternal( RelationshipGroupRecord record, PageCursor cursor, long header )
            throws IOException
    {
        cursor.putShort( (short) record.getType() );
        _2bitEncode( cursor, record.getFirstOut(), header, OUTGOING_ENCODING );
        _2bitEncode( cursor, record.getFirstIn(), header, INCOMING_ENCODING );
        _2bitEncode( cursor, record.getFirstLoop(), header, LOOP_ENCODING );
        _2bitEncode( cursor, record.getOwningNode(), header, OWNER_ENCODING );
        _2bitEncode( cursor, record.getNext(), header, NEXT_ENCODING );
    }
}
