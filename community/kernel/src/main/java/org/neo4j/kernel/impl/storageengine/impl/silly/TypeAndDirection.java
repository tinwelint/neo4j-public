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

import java.util.function.IntPredicate;

import org.neo4j.storageengine.api.Direction;

class TypeAndDirection
{
    private final Direction direction;
    private final int type;

    TypeAndDirection( Direction direction, int type )
    {
        this.direction = direction;
        this.type = type;
    }

    boolean isDirection( Direction direction )
    {
        return this.direction == Direction.BOTH ? true : direction == this.direction;
    }

    boolean isType( IntPredicate typeIds )
    {
        return typeIds.test( type );
    }

    int type()
    {
        return type;
    }
}
