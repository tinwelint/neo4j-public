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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.nio.file.OpenOption;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.store.NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT;

/**
 * Implementation of the relationship store.
 */
public class RelationshipStore extends CommonAbstractStore<RelationshipRecord,NoStoreHeader>
{
    public static final String TYPE_DESCRIPTOR = "RelationshipStore";

    public RelationshipStore(
            File fileName,
            Config configuration,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            RecordFormats recordFormats,
            OpenOption... openOptions )
    {
        super( fileName, configuration, IdType.RELATIONSHIP, idGeneratorFactory,
                pageCache, logProvider, TYPE_DESCRIPTOR, recordFormats.relationship(), NO_STORE_HEADER_FORMAT,
                recordFormats.storeVersion(), openOptions );
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, RelationshipRecord record )
            throws FAILURE
    {
        processor.processRelationship( this, record );
    }

//    private long accessCount = 0;
//    private long previousPage = 0;
//    private long pageChangeCount = 0;
//    private long currentConsecutivePageAccess = 0;
//    private long longestConsecutivePageAccess = 0;
//
//    @Override
//    protected void readIntoRecord( long id, RelationshipRecord record, RecordLoad mode, long pageId, int offset,
//            PageCursor cursor ) throws IOException
//    {
//        accessCount++;
//        if ( pageId != previousPage )
//        {
//            pageChangeCount++;
//            previousPage = pageId;
//            longestConsecutivePageAccess = Long.max( longestConsecutivePageAccess, currentConsecutivePageAccess );
//            currentConsecutivePageAccess = 0;
//        }
//        else
//        {
//            currentConsecutivePageAccess++;
//        }
//        super.readIntoRecord( id, record, mode, pageId, offset, cursor );
//    }
//
//    @Override
//    public void close()
//    {
//        System.out.println( "access:" + accessCount + ", pageChange:" + pageChangeCount +
//                ", " + ((double) pageChangeCount / accessCount) + " longestPageAccess:" + longestConsecutivePageAccess );
//        super.close();
//    }
}
