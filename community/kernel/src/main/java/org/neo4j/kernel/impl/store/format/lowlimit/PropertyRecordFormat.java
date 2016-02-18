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
package org.neo4j.kernel.impl.store.format.lowlimit;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.neo4j.kernel.impl.store.record.PropertyRecord.PROPERTY_BLOCK_SIZE;

public class PropertyRecordFormat extends BaseRecordFormat<PropertyRecord>
{
    public static final int DEFAULT_DATA_BLOCK_SIZE = 120;
    public static final int NUMBER_OF_BLOCKS = 4;
    public static final int DEFAULT_PAYLOAD_SIZE = NUMBER_OF_BLOCKS * PROPERTY_BLOCK_SIZE;

    public static final int RECORD_SIZE = 1/*next and prev high bits*/
            + 4/*next*/
            + 4/*prev*/
            + (NUMBER_OF_BLOCKS * PROPERTY_BLOCK_SIZE) /*property blocks*/;
         // = 41

    public PropertyRecordFormat()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, 0 );
    }

    @Override
    public PropertyRecord newRecord()
    {
        return new PropertyRecord( -1 );
    }

    @Override
    public int getRecordHeaderSize()
    {
        return 1 + 4 + 4;
    }

    @Override
    public void read( PropertyRecord record, PageCursor cursor, RecordLoad mode, int recordSize, PagedFile storeFile )
    {
        int offsetAtBeginning = cursor.getOffset();

        /*
         * [pppp,nnnn] previous, next high bits
         */
        byte modifiers = cursor.getByte();
        long prevMod = (modifiers & 0xF0L) << 28;
        long nextMod = (modifiers & 0x0FL) << 32;
        long prevProp = cursor.getUnsignedInt();
        long nextProp = cursor.getUnsignedInt();
        record.initialize( false,
                BaseRecordFormat.longFromIntAndMod( prevProp, prevMod ),
                BaseRecordFormat.longFromIntAndMod( nextProp, nextMod ) );
        while ( cursor.getOffset() - offsetAtBeginning < RECORD_SIZE )
        {
            long block = cursor.getLong();
            PropertyType type = PropertyType.getPropertyType( block, true );
            if ( type == null )
            {
                // We assume that storage is defragged
                break;
            }

            record.setInUse( true );
            record.addLoadedBlock( block );
            int additionalBlocks = type.calculateNumberOfBlocksUsed( block ) - 1;
            while ( additionalBlocks --> 0 )
            {
                record.addLoadedBlock( cursor.getLong() );
            }
        }
    }

    @Override
    public void write( PropertyRecord record, PageCursor cursor, int recordSize, PagedFile storeFile )
    {
        if ( record.inUse() )
        {
            // Set up the record header
            int startOffset = cursor.getOffset();
            short prevModifier = record.getPrevProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ((record.getPrevProp() & 0xF00000000L) >> 28);
            short nextModifier = record.getNextProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ((record.getNextProp() & 0xF00000000L) >> 32);
            byte modifiers = (byte) (prevModifier | nextModifier);
            /*
             * [pppp,nnnn] previous, next high bits
             */
            cursor.putByte( modifiers );
            cursor.putInt( (int) record.getPrevProp() );
            cursor.putInt( (int) record.getNextProp() );

            // Then go through the blocks
            int longsAppended = 0; // For marking the end of blocks
            int maxBlocks = (recordSize - (cursor.getOffset() - startOffset)) / PROPERTY_BLOCK_SIZE;
            for ( PropertyBlock block : record )
            {
                long[] propBlockValues = block.getValueBlocks();
                for ( long propBlockValue : propBlockValues )
                {
                    cursor.putLong( propBlockValue );
                }

                longsAppended += propBlockValues.length;
            }
            if ( longsAppended < maxBlocks )
            {
                cursor.putLong( 0 );
            }
        }
        else
        {
            // skip over the record header, nothing useful there
            cursor.setOffset( cursor.getOffset() + 9 );
            cursor.putLong( 0 );
        }
    }

    @Override
    public long getNextRecordReference( PropertyRecord record )
    {
        return record.getNextProp();
    }

    /**
     * For property records there's no "inUse" byte and we need to read the whole record to
     * see if there are any PropertyBlocks in use in it.
     */
    @Override
    public boolean isInUse( PageCursor cursor, int recordSize )
    {
        cursor.setOffset( cursor.getOffset() /*skip...*/ + 1/*mod*/ + 4/*prev*/ + 4/*next*/ );
        int blocks = (recordSize - 9) / 8;
        for ( int i = 0; i < blocks; i++ )
        {
            long block = cursor.getLong();
            if ( PropertyType.getPropertyType( block, true ) != null )
            {
                return true;
            }
        }
        return false;
    }
}
