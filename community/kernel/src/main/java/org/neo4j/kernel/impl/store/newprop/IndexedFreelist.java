/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.newprop;

import java.io.IOException;
import java.util.BitSet;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.io.ByteUnit;

import static org.neo4j.io.ByteUnit.kibiBytes;

/**
 * Thinking behind this is that there's a GB+Tree in here storing things like how label index does, in small (although a little bigger?) bit sets.
 * Writes that allocate or free records takes the hit of updating this free-list, i.e. the index on the apply-commit path.
 * Reads will almost never hit index, instead some readers now and then will read-and-scan a little and cache slots based on size.
 *
 * Reuse marker can be an in-memory bit-set/index.
 *
 * Perhaps have highId as a state data thing? Otherwise figure out from real store at startup somehow.
 *
 * DESIGN PROBLEMS:
 * - Cached slots handed out to committing transactions (while they're building up their state to commit),
 *   how can we make sure that a loop-around scan won't hand them out again? Use reuse-marker for this too?
 */
class IndexedFreelist implements Freelist
{
    @Override
    public long allocate( int slots ) throws IOException
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Marker commitMarker() throws IOException
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Marker reuseMarker() throws IOException
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void close() throws IOException
    {
    }

    /**
     * A roaring bitmap? Problem the 2^32 id limit, right?
     * A PrimitiveLongSet? Slow... but too slow?
     */
    private static class InMemoryBitSetIndex implements Marker
    {
        private final PrimitiveLongSet set = Primitive.longSet( (int) kibiBytes( 1024 ) ); // i.e. 8k memory initially

        @Override
        public void mark( long id, int slots, boolean inUse )
        {

        }

        @Override
        public void close()
        {
            set.close();
        }
    }
}
