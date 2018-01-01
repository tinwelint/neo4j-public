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

import java.util.Iterator;
import java.util.function.IntPredicate;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Lock;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.neo4j.helpers.collection.Iterators.iterator;

class SillyStorageStatement implements StorageStatement, LabelScanReader
{
    private final SillyData data;
    private final IndexingService indexingService;

    private boolean acquired;
    private IndexReaderFactory indexReaderFactory;

    SillyStorageStatement( SillyData data, IndexingService indexingService )
    {
        this.data = data;
        this.indexingService = indexingService;
    }

    @Override
    public void acquire()
    {
        assert !acquired;
        acquired = true;
    }

    @Override
    public void release()
    {
        assert acquired;
        acquired = false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public Cursor<NodeItem> acquireSingleNodeCursor( long nodeId )
    {
        return Cursors.cursorOf( Iterators.cast( orEmpty( data.nodes.values().iterator(), NodeData.EMPTY ) ) );
    }

    /**
     * There's a very weird behaviour between tx cursors and store cursors where sometimes it's expected that
     * a cursors can respond to a {@link Cursor#get()} which will return an instance with empty values on its following
     * call to {@link Cursor#next()}. It's a weird design, but let's adhere to it by doing this little trick.
     *
     * @param iterator {@link Cursor} to call {@link Cursor#next()} on.
     * @return the cursor.
     */
    private static <T> Iterator<T> orEmpty( Iterator<T> iterator, T empty )
    {
        return iterator.hasNext() ? iterator : iterator( empty );
    }

    @Override
    public Cursor<RelationshipItem> acquireSingleRelationshipCursor( long relationshipId )
    {
        return Cursors.cursorOf( iterator( data.relationships.get( relationshipId ) ) );
    }

    @Override
    public Cursor<RelationshipItem> acquireNodeRelationshipCursor( boolean isDense, long nodeId, long relationshipId, Direction direction,
            IntPredicate relTypeFilter )
    {
        NodeData node = data.nodes.get( nodeId );
        return Cursors.cursorOf( Iterators.cast( node.relationships( direction, relTypeFilter ) ) );
    }

    @Override
    public Cursor<RelationshipItem> relationshipsGetAllCursor()
    {
        return Cursors.cursorOf( Iterators.cast( data.relationships.values().iterator() ) );
    }

    @Override
    public Cursor<PropertyItem> acquirePropertyCursor( long propertyId, Lock shortLivedReadLock, AssertOpen assertOpen )
    {
        return null;
    }

    @Override
    public Cursor<PropertyItem> acquireSinglePropertyCursor( long propertyId, int propertyKeyId, Lock shortLivedReadLock,
            AssertOpen assertOpen )
    {
        return null;
    }

    @Override
    public LabelScanReader getLabelScanReader()
    {
        return this;
    }

    @Override
    public PrimitiveLongResourceIterator nodesWithLabel( int labelId )
    {
        return SillyStorageEngine.nodeIds( data, node -> node.hasLabel( labelId ) );
    }

    @Override
    public PrimitiveLongResourceIterator nodesWithAnyOfLabels( int... labelIds )
    {
        return SillyStorageEngine.nodeIds( data, node ->
        {
            for ( int labelId : labelIds )
            {
                if ( node.hasLabel( labelId ) )
                {
                    return true;
                }
            }
            return false;
        } );
    }

    @Override
    public PrimitiveLongResourceIterator nodesWithAllLabels( int... labelIds )
    {
        return SillyStorageEngine.nodeIds( data, node ->
        {
            for ( int labelId : labelIds )
            {
                if ( !node.hasLabel( labelId ) )
                {
                    return false;
                }
            }
            return true;
        } );
    }

    @Override
    public IndexReader getIndexReader( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        return indexReaders().newReader( index );
    }

    private IndexReaderFactory indexReaders()
    {
        if ( indexReaderFactory == null )
        {
            indexReaderFactory = new IndexReaderFactory.Caching( indexingService );
        }
        return indexReaderFactory;
    }

    @Override
    public IndexReader getFreshIndexReader( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        return indexReaders().newUnCachedReader( index );
    }

    @Override
    public long reserveNode()
    {
        return data.nextNodeId.getAndIncrement();
    }

    @Override
    public long reserveRelationship()
    {
        return data.nextRelationshipId.getAndIncrement();
    }
}
