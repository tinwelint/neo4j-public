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

import org.neo4j.values.storable.Value;

/**
 * A bare-bone interface to accessing, both reading and writing, property data. Implementations are
 * assumed to be thread-safe, although not regarding multiple threads (transactions) manipulating
 * same records simultaneously. External synchronization/avoidance is required.
 *
 * The {@code long entity} references are generated and managed externally.
 */
public interface SimplePropertyStoreAbstraction extends Closeable
{
    interface Read extends Closeable
    {
        /**
         * @return whether or not the property exists.
         */
        boolean has( long id, int key ) throws IOException;

        /**
         * @return the {@link Value}.
         */
        Value get( long id, int key ) throws IOException;

        /**
         * The idea with this method is to get the data, i.e. read it byte for byte or whatever, but
         * not do deserialization, because we're not really interested in that deserialization cost since
         * it's more or less the same in all conceivable implementations and will most likely tower above
         * the cost of getting to the property data.
         *
         * @return number of property data bytes visited.
         */
        int getWithoutDeserializing( long id, int key ) throws IOException;

        /**
         * @return number of properties visited.
         */
        int all( long id, PropertyVisitor visitor ) throws IOException;
    }

    interface Write extends Read
    {
        /**
         * The (first) property record id.
         *
         * @return the new record id, potentially the same {@code id} that got passed in.
         */
        long set( long id, int key, Value value ) throws IOException;

        /**
         * @return the new record id, potentially the same {@code id} that got passed in.
         */
        long remove( long id, int key ) throws IOException;
    }

    Write newWrite() throws IOException;

    Read newRead() throws IOException;

    long storeSize() throws IOException;

    interface PropertyVisitor
    {
        void accept( long id, int key );
    }
}
