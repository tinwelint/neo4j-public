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

public interface ValueStructure
{
    /**
     * Nothing read, i.e. no additional consistency checking measure needs to be made.
     */
    int READ_NOTHING = 0;

    /**
     * Something was read, i.e. additional consistency checking measures needs to be made after this read
     * before acting on the read data.
     */
    int READ = 1;

    /**
     * Something was read, but was detected to be inconsistent.
     */
    int READ_INCONSISTENT = 2;

    void integralValue( long value );

    long integralValue();

    void value( Object value );

    Object value();

    byte[] byteArray( int length );
}
