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
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat.has;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitDecode;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitEncode;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitLength;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding._2bitSetHeader;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding.read5ByteValue;
import static org.neo4j.kernel.impl.store.format.SimpleNumberEncoding.write5ByteValue;

/**
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * VB   first relationship
 * VB   first property
 * 5B   labels
 *
 * => 12B-22B
 */
class NodeRecordFormat extends BaseHighLimitRecordFormat<NodeRecord>
{
    static final int RECORD_SIZE = 16;

    // 1-byte header bits specific to this format
    private static final int DENSE_NODE_BIT        =     0b0000_0100;
    private static final int RELATIONSHIP_ENCODING = 3;//0b0001_1000;
    private static final int PROPERTY_ENCODING     = 5;//0b0110_0000;

    public NodeRecordFormat()
    {
        this( RECORD_SIZE );
    }

    NodeRecordFormat( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0, false );
    }

    @Override
    public NodeRecord newRecord()
    {
        return new NodeRecord( -1 );
    }

    @Override
    protected void doReadInternal( NodeRecord record, PageCursor cursor, int recordSize, long header,
                                   boolean inUse )
    {
        // Interpret the header
        boolean dense = has( header, DENSE_NODE_BIT );

        // Now read the rest of the data. The adapter will take care of moving the cursor over to the
        // other unit when we've exhausted the first one.
        long nextRel = _2bitDecode( cursor, NULL, header, RELATIONSHIP_ENCODING );
        long nextProp = _2bitDecode( cursor, NULL, header, PROPERTY_ENCODING );
        long labelField = read5ByteValue( cursor );
        record.initialize( inUse, nextProp, dense, nextRel, labelField );
    }

    @Override
    public int requiredDataLength( NodeRecord record )
    {
        return  5 +
                _2bitLength( record.getNextRel(), NULL ) +
                _2bitLength( record.getNextProp(), NULL );
    }

    @Override
    protected long headerBits( NodeRecord record )
    {
        long header = 0;
        header ^= record.isDense() ? DENSE_NODE_BIT : 0;
        header = _2bitSetHeader( record.getNextRel(), NULL, header, RELATIONSHIP_ENCODING );
        header = _2bitSetHeader( record.getNextProp(), NULL, header, PROPERTY_ENCODING );
        return header;
    }

    @Override
    protected void doWriteInternal( NodeRecord record, PageCursor cursor, long header )
            throws IOException
    {
        _2bitEncode( cursor, record.getNextRel(), header, RELATIONSHIP_ENCODING );
        _2bitEncode( cursor, record.getNextProp(), header, PROPERTY_ENCODING );
        write5ByteValue( cursor, record.getLabelField() );
    }
}
