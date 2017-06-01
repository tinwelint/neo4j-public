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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;

import org.neo4j.cursor.RawCursor;

/**
 * Seeks and makes keys and values in leaves accessible using {@link #key()} and {@link #value()}, or {@link #get()}.
 * The key/value instances provided by {@link Hit} and {@link #key()} and {@link #value()} instance are mutable
 * and overwritten with new values for every call to {@link #next()} so user cannot keep references to
 * key/value instances, expecting them to keep their values intact.
 *
 * @param <KEY> type of keys.
 * @param <VALUE> type of values.
 */
public interface Seeker<KEY,VALUE> extends RawCursor<Hit<KEY,VALUE>,IOException>
{
    /**
     * @return the key of the current hit, i.e. the most recent call to {@link #next()}. If no call to {@link #next()}
     * has been made an {@link IllegalStateException} should be thrown.
     */
    default KEY key()
    {
        return get().key();
    }

    /**
     * @return the value of the current hit, i.e. the most recent call to {@link #next()}. If no call to {@link #next()}
     * has been made an {@link IllegalStateException} should be thrown.
     */
    default VALUE value()
    {
        return get().value();
    }
}
