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

import java.io.Closeable;
import java.io.IOException;

/**
 * Manages both high id and free-list.
 */
interface Freelist extends Closeable
{
    long allocate( int slots ) throws IOException;

    Marker commitMarker() throws IOException;

    Marker reuseMarker() throws IOException;

    interface Marker extends Closeable
    {
        void mark( long id, int slots, boolean inUse ) throws IOException;
    }

    Marker NULL_MARKER = new Marker()
    {
        @Override
        public void mark( long id, int slots, boolean inUse ) throws IOException
        {
            // don't
        }

        @Override
        public void close() throws IOException
        {
            // nothing to close
        }
    };
}
