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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * {@link LabelScanReader} for reading data from {@link NativeLabelScanStore}.
 * Each {@link PrimitiveLongIterator} returned from each of the methods is backed by {@link RawCursor}
 * directly from {@link GBPTree#seek(Seeker, Object, Object)}.
 * <p>
 * The returned {@link PrimitiveLongIterator} aren't closable so the cursors retrieved are managed
 * inside of this reader and closed between each new query and on {@link #close()}.
 */
class NativeLabelScanReader implements LabelScanReader
{
    /**
     * {@link Index} which is queried when calling the methods below.
     */
    private final GBPTree<LabelScanKey,LabelScanValue> index;

    /**
     * Currently open {@link RawCursor} from query methods below. Open cursors are closed when calling
     * new query methods or when {@link #close() closing} this reader.
     */
    private final Set<Seeker<LabelScanKey,LabelScanValue>> openSeekers;

    NativeLabelScanReader( GBPTree<LabelScanKey,LabelScanValue> index )
    {
        this.index = index;
        this.openSeekers = new HashSet<>();
    }

    /**
     * Closes all currently open {@link RawCursor cursors} from last query method call.
     */
    @Override
    public void close()
    {
        try
        {
            ensureOpenCursorsClosed();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public PrimitiveLongIterator nodesWithLabel( int labelId )
    {
        Seeker<LabelScanKey,LabelScanValue> cursor;
        try
        {
            cursor = seekerForLabel( labelId );
            openSeekers.add( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }

        return new LabelScanValueIterator( cursor, openSeekers );
    }

    @Override
    public PrimitiveLongIterator nodesWithAnyOfLabels( int... labelIds )
    {
        List<PrimitiveLongIterator> iterators = iteratorsForLabels( labelIds );
        return new CompositeLabelScanValueIterator( iterators, false );
    }

    @Override
    public PrimitiveLongIterator nodesWithAllLabels( int... labelIds )
    {
        List<PrimitiveLongIterator> iterators = iteratorsForLabels( labelIds );
        return new CompositeLabelScanValueIterator( iterators, true );
    }

    private List<PrimitiveLongIterator> iteratorsForLabels( int[] labelIds )
    {
        List<PrimitiveLongIterator> iterators = new ArrayList<>();
        try
        {
            for ( int labelId : labelIds )
            {
                Seeker<LabelScanKey,LabelScanValue> cursor = seekerForLabel( labelId );
                openSeekers.add( cursor );
                iterators.add( new LabelScanValueIterator( cursor, openSeekers ) );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return iterators;
    }

    private Seeker<LabelScanKey,LabelScanValue> seekerForLabel( int labelId ) throws IOException
    {
        LabelScanKey from = new LabelScanKey( labelId, 0 );
        LabelScanKey to = new LabelScanKey( labelId, Long.MAX_VALUE );
        return index.seek( index.allocateSeeker(), from, to );
    }

    private void ensureOpenCursorsClosed() throws IOException
    {
        for ( Seeker<LabelScanKey,LabelScanValue> cursor : openSeekers )
        {
            cursor.close();
        }
        openSeekers.clear();
    }
}
