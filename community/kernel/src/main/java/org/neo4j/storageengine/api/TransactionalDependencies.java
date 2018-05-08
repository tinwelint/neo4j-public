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
package org.neo4j.storageengine.api;

import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;

/**
 * Dependencies that a {@link StorageReader} needs in order to implement transaction-aware cursors that it provides.
 * These dependencies are provided from e.g. the kernel and its transaction sub-system.
 */
public interface TransactionalDependencies extends TxStateHolder, AssertOpen
{
    SecurityContext securityContext();

    TransactionalDependencies EMPTY = new TransactionalDependencies()
    {
        @Override
        public SecurityContext securityContext()
        {
            return SecurityContext.AUTH_DISABLED;
        }

        @Override
        public void assertOpen()
        {
        }

        @Override
        public TransactionState txState()
        {
            throw new UnsupportedOperationException( "Should not be called" );
        }

        @Override
        public ExplicitIndexTransactionState explicitIndexTxState()
        {
            throw new UnsupportedOperationException( "Should not be called" );
        }

        @Override
        public boolean hasTxStateWithChanges()
        {
            return false;
        }
    };
}
