/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import static org.neo4j.helpers.Exceptions.launderedException;

import java.util.concurrent.Callable;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.kernel.impl.api.LockHolder;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;

/**
 * After an index is populated an {@link IndexPopulationCompletor} will mark it as ONLINE
 * while holding a schema write lock.  
 */
public class IndexPopulationCompletor
{
    private final LockManager lockManager;
    private final PersistenceManager persistenceManager;
    private final TransactionManager txManager;
    private final IndexRule index;

    public IndexPopulationCompletor( IndexRule index, LockManager lockManager, PersistenceManager persistenceManager,
            TransactionManager txManager )
    {
        this.index = index;
        this.lockManager = lockManager;
        this.persistenceManager = persistenceManager;
        this.txManager = txManager;
    }
    
    /**
     * Marks the index state as {@link IndexState#ONLINE}. Guarantees that no index updates will
     * get to the index during this call.
     * 
     * @param runnable final work required on the index given the guarantees of this method.
     */
    public void complete( Callable<IndexSnapshot> runnable )
    {
        LockHolder lockHolder = null;
        boolean success = false;
        try
        {
            txManager.begin();
            lockHolder = new LockHolder( lockManager, txManager.getTransaction() );
            lockHolder.acquireSchemaWriteLock();
            IndexSnapshot snapshot = runnable.call();
            persistenceManager.completeIndexCreation( index, snapshot );
            success = true;
        }
        catch ( Exception e )
        {
            throw launderedException( e );
        }
        finally
        {
            try
            {
                if ( success )
                    txManager.commit();
                else
                    txManager.rollback();
            }
            catch ( Exception e )
            {
                throw launderedException( e );
            }
            finally
            {
                lockHolder.releaseLocks();
            }
        }
    }

    public interface IndexSnapshot
    {
        void write( LogBuffer target );
    }
    
    public static final IndexSnapshot NO_SNAPSHOT = new IndexSnapshot()
    {
        @Override
        public void write( LogBuffer target )
        {
        }
    };
}
