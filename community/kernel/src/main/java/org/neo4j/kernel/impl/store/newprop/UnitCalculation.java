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

import static org.neo4j.io.ByteUnit.kibiBytes;

class UnitCalculation
{
    static final int PAGE_SIZE = (int) kibiBytes( 8 );
    static final int UNIT_SIZE = 64;
    static final int EFFECTIVE_PAGE_SIZE = PAGE_SIZE - UNIT_SIZE; // one-unit header
    static final int UNITS_PER_PAGE = EFFECTIVE_PAGE_SIZE / UNIT_SIZE;
    static final int EFFECTIVE_UNITS_PER_PAGE = UNITS_PER_PAGE - 1; // one-unit header

    static long pageIdForRecord( long id )
    {
        return id / EFFECTIVE_UNITS_PER_PAGE;
    }

    static int offsetForId( long id )
    {
        return (int) ((id % EFFECTIVE_UNITS_PER_PAGE) * UNIT_SIZE) + UNIT_SIZE;
    }

    static int unitInPage( long id )
    {
        return (int) (id % UNITS_PER_PAGE);
    }
}
