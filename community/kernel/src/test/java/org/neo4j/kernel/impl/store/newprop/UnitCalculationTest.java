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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.EFFECTIVE_UNITS_PER_PAGE;
import static org.neo4j.kernel.impl.store.newprop.UnitCalculation.UNIT_SIZE;

public class UnitCalculationTest
{
    @Test
    public void shouldCalculatePageAndOffsetForFirstRecordInSecondPage() throws Exception
    {
        // given
        long recordId = EFFECTIVE_UNITS_PER_PAGE;

        // when
        long pageId = UnitCalculation.pageIdForRecord( recordId );
        int offset = UnitCalculation.offsetForId( recordId );

        // then
        assertEquals( 1, pageId );
        assertEquals( UNIT_SIZE, offset );
    }
}
