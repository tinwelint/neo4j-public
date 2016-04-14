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
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat.has;
import static org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat.set;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._1bitDecode;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._1bitEncode;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._1bitLength;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._1bitSetHeader;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._1bitSignedDecode;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._1bitSignedEncode;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._1bitSignedLength;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._1bitSignedSetHeader;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitDecode;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitEncode;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitLength;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitSetHeader;
import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toAbsolute;
import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toRelative;

/**
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * 2B   relationship type
 * VB   first property
 * VB   start node
 * VB   end node
 * VB   start node chain previous relationship
 * VB   start node chain next relationship
 * VB   end node chain previous relationship
 * VB   end node chain next relationship
 *
 * => 24B-59B
 */
class RelationshipRecordFormat extends BaseHighLimitRecordFormat<RelationshipRecord>
{
    static final int RECORD_SIZE = 32;

    // 2-byte header bits specific to this format
    // TODO encode prev pointers differently depending on whether they are first or not
    // TODO how would these look in a 3-byte header instead?
    private static final int FIRST_IN_FIRST_CHAIN_BIT   =      0b0000_0000__0000_0100;
    private static final int FIRST_IN_SECOND_CHAIN_BIT  =      0b0000_0000__0000_1000;
    private static final int FIRST_CHAIN_NEXT_ENCODING  = 4; //0b0000_0000__0011_0000; // <-- sign+1b 3/6
    private static final int SECOND_CHAIN_NEXT_ENCODING = 6; //0b0000_0000__1100_0000; // <-- sign+1b 3/6
    private static final int PROPERTY_ENCODING          = 8; //0b0000_0011__0000_0000;
    private static final int FIRST_CHAIN_PREV_ENCODING  = 10;//0b0000_1100__0000_0000; // <-- sign+1b 3/6
    private static final int SECOND_CHAIN_PREV_ENCODING = 12;//0b0011_0000__0000_0000; // <-- sign+1b 3/6
    private static final int FIRST_NODE_ENCODING        = 14;//0b0100_0000__0000_0000; // <-- 4/6
    private static final int SECOND_NODE_ENCODING       = 15;//0b1000_0000__0000_0000; // <-- 4/6

    public RelationshipRecordFormat()
    {
        this( RECORD_SIZE );
    }

    RelationshipRecordFormat( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0, true );
    }

    @Override
    public RelationshipRecord newRecord()
    {
        return new RelationshipRecord( -1 );
    }

    @Override
    protected void doReadInternal( RelationshipRecord record, PageCursor cursor, int recordSize, long header,
            boolean inUse )
    {
        int type = cursor.getShort() & 0xFFFF;
        long recordId = record.getId();
        record.initialize( inUse,
                _2bitDecode( cursor, NULL, header, PROPERTY_ENCODING ),
                _1bitDecode( cursor, header, FIRST_NODE_ENCODING ),
                _1bitDecode( cursor, header, SECOND_NODE_ENCODING ),
                type,
                decodeAbsoluteOrRelative( cursor, header, FIRST_CHAIN_PREV_ENCODING, FIRST_IN_FIRST_CHAIN_BIT, recordId ),
                toAbsolute( _1bitSignedDecode( cursor, header, FIRST_CHAIN_NEXT_ENCODING ), recordId ),
                decodeAbsoluteOrRelative( cursor, header, SECOND_CHAIN_PREV_ENCODING, FIRST_IN_SECOND_CHAIN_BIT, recordId ),
                toAbsolute( _1bitSignedDecode( cursor, header, SECOND_CHAIN_NEXT_ENCODING ), recordId ),
                has( header, FIRST_IN_FIRST_CHAIN_BIT ),
                has( header, FIRST_IN_SECOND_CHAIN_BIT ) );
    }

    private long decodeAbsoluteOrRelative( PageCursor cursor, long header, int shift,
            int firstInChainBit, long recordId )
    {
        long decoded = _1bitSignedDecode( cursor, header, shift );
        return has( header, firstInChainBit ) ? decoded : toAbsolute( decoded, recordId );
    }

    @Override
    protected long headerBits( RelationshipRecord record )
    {
        long header = 0;
        header = set( header, FIRST_IN_FIRST_CHAIN_BIT, record.isFirstInFirstChain() );
        header = set( header, FIRST_IN_SECOND_CHAIN_BIT, record.isFirstInSecondChain() );
        header = _2bitSetHeader( record.getNextProp(), NULL, header, PROPERTY_ENCODING );
        // first node
        header = _1bitSetHeader( record.getFirstNode(), header, FIRST_NODE_ENCODING );
        header = _1bitSignedSetHeader( getFirstPrevReference( record ),
                header, FIRST_CHAIN_PREV_ENCODING );
        header = _1bitSignedSetHeader( toRelative( record.getFirstNextRel(), record.getId() ),
                header, FIRST_CHAIN_NEXT_ENCODING );
        // second node
        header = _1bitSetHeader( record.getSecondNode(), header, SECOND_NODE_ENCODING );
        header = _1bitSignedSetHeader( getSecondPrevReference( record ),
                header, SECOND_CHAIN_PREV_ENCODING );
        header = _1bitSignedSetHeader( toRelative( record.getSecondNextRel(), record.getId() ),
                header, SECOND_CHAIN_NEXT_ENCODING );
        return header;
    }

    @Override
    protected int requiredDataLength( RelationshipRecord record )
    {
        long recordId = record.getId();
        return Short.BYTES + // type
               _2bitLength( record.getNextProp(), NULL ) +
               _1bitLength( record.getFirstNode() ) +
               _1bitLength( record.getSecondNode() ) +
               _1bitSignedLength( getFirstPrevReference( record ) ) +
               _1bitSignedLength( toRelative( record.getFirstNextRel(), recordId ) ) +
               _1bitSignedLength( getSecondPrevReference( record ) ) +
               _1bitSignedLength( toRelative( record.getSecondNextRel(), recordId ) );
    }

    @Override
    protected void doWriteInternal( RelationshipRecord record, PageCursor cursor, long header )
            throws IOException
    {
        cursor.putShort( (short) record.getType() );
        long recordId = record.getId();
        _2bitEncode( cursor, record.getNextProp(), header, PROPERTY_ENCODING );
        _1bitEncode( cursor, record.getFirstNode(), header, FIRST_NODE_ENCODING );
        _1bitEncode( cursor, record.getSecondNode(), header, SECOND_NODE_ENCODING );
        _1bitSignedEncode( cursor, getFirstPrevReference( record ), header, FIRST_CHAIN_PREV_ENCODING );
        _1bitSignedEncode( cursor, toRelative( record.getFirstNextRel(), recordId ), header, FIRST_CHAIN_NEXT_ENCODING );
        _1bitSignedEncode( cursor, getSecondPrevReference( record ), header, SECOND_CHAIN_PREV_ENCODING );
        _1bitSignedEncode( cursor, toRelative( record.getSecondNextRel(), recordId ), header, SECOND_CHAIN_NEXT_ENCODING );
    }

    private long getPrevReference( boolean firstInChain, long prevId, long recordId )
    {
        return firstInChain ? prevId : toRelative( prevId, recordId );
    }

    private long getSecondPrevReference( RelationshipRecord record )
    {
        return getPrevReference( record.isFirstInSecondChain(), record.getSecondPrevRel(), record.getId() );
    }

    private long getFirstPrevReference( RelationshipRecord record )
    {
        return getPrevReference( record.isFirstInFirstChain(), record.getFirstPrevRel(), record.getId() );
    }
}
