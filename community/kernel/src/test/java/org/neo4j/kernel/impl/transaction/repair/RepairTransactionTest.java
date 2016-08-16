/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.repair;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import org.neo4j.function.Consumer;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.impl.api.CapturingCommitProcess;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;
import static org.neo4j.kernel.impl.api.CommandExtractor.commandsOf;

public class RepairTransactionTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule( false );

    private final Locks.Client locks = mock( Locks.Client.class );
    private final CapturingCommitProcess commitProcess = new CapturingCommitProcess();
    private final TransactionIdStore txIdStore = new DeadSimpleTransactionIdStore();
    private NeoStores stores;

    @Before
    public void before()
    {
        EphemeralFileSystemAbstraction fs = fsRule.get();
        File storeDir = new File( "dir" ).getAbsoluteFile();
        fs.mkdirs( storeDir );
        stores = new StoreFactory( fs, storeDir, pageCacheRule.getPageCache( fs ), NullLogProvider.getInstance() )
                .openAllNeoStores( true );
    }

    @After
    public void after()
    {
        stores.close();
    }

    @Test
    public void shouldRepairNode() throws Exception
    {
        // GIVEN
        NodeStore store = stores.getNodeStore();
        long id = store.nextId();
        store.updateRecord( new NodeRecord( id, true, false, 4, 5, 6 ) );

        // WHEN
        try ( RepairTransaction tx = newRepairTransaction() )
        {
            tx.repairNode( 0, new Consumer<NodeRecord>()
            {
                @Override
                public void accept( NodeRecord node )
                {
                    node.setNextRel( 10 );
                    node.setNextProp( 11 );
                    node.setLabelField( 12, Collections.<DynamicRecord>emptyList() );
                }
            } );
            tx.success();
        }

        // THEN
        verify( locks ).acquireExclusive( ResourceTypes.NODE, 0L );
        Command[] commands = commandsOf( commitProcess.getLastCommittedTransaction() );
        assertEquals( 1, commands.length );
        assertTrue( commands[0] instanceof NodeCommand );
        NodeCommand nodeCommand = (NodeCommand) commands[0];
        NodeRecord repaired = nodeCommand.getAfter();
        assertEquals( 10L, repaired.getNextRel() );
        assertEquals( 11L, repaired.getNextProp() );
        assertEquals( 12L, repaired.getLabelField() );
    }

    private RepairTransaction newRepairTransaction()
    {
        return new RepairTransaction( locks, stores, commitProcess, SYSTEM_CLOCK, txIdStore );
    }
}
