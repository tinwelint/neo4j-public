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
package org.neo4j.storageengine.api.schema;

import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;

/**
 * Visits added and removed elements of a {@link ReadableDiffSets}.
 */
public interface DiffSetsVisitor<T>
{
    void visitAdded( T element ) throws ConstraintValidationException, CreateConstraintFailureException;

    void visitRemoved( T element ) throws ConstraintValidationException;

    class Adapter<T> implements DiffSetsVisitor<T>
    {
        @Override
        public void visitAdded( T element )
        {   // Ignore
        }

        @Override
        public void visitRemoved( T element )
        {   // Ignore
        }
    }
}
