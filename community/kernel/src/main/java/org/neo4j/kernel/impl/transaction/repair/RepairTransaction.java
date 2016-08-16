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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.Consumer;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.neo4j.kernel.impl.api.TransactionApplicationMode.EXTERNAL;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;

/**
 * Normally transactions are made by observing data in records, producing logical changes via in-memory
 * transaction state, which gets converted into physical changes and committed. In cases where the data
 * in the store may be inconsistent the data cannot be read through normal high-level abstractions
 * because it cannot make sense on that level. In these cases there may be opportunity for directly
 * repairing those records. Whereas it's easy to simply change and update records directly onto the stores
 * it doesn't work well in a clustered environment and isn't recovery safe either. The best thing to
 * do is to make the changes as if they were a normal transaction, committed as such.
 *
 * This class helps in doing these repairs.
 */
public class RepairTransaction implements AutoCloseable
{
    private final Locks.Client locks;
    private final NeoStores stores;
    private final TransactionCommitProcess commitProcess;
    private final Map<AbstractBaseRecord,Command> repairs = new HashMap<>();
    private final Clock clock;
    private final TransactionIdStore txIdStore;
    private final long startTime;
    private final long startTx;

    private boolean successful;

    public RepairTransaction( Locks.Client locks, NeoStores stores, TransactionCommitProcess commitProcess,
            Clock clock, TransactionIdStore txIdStore )
    {
        this.locks = locks;
        this.stores = stores;
        this.commitProcess = commitProcess;
        this.clock = clock;
        this.txIdStore = txIdStore;
        this.startTime = clock.currentTimeMillis();
        this.startTx = txIdStore.getLastCommittedTransactionId();
    }

    public void repairRelationship( long id, Consumer<RelationshipRecord> repair )
    {
        locks.acquireExclusive( ResourceTypes.RELATIONSHIP, id );
        RelationshipRecord record = stores.getRelationshipStore().getRecord( id );
        locks.acquireExclusive( ResourceTypes.NODE, record.getFirstNode() );
        locks.acquireExclusive( ResourceTypes.NODE, record.getSecondNode() );
        repair.accept( record );

        RelationshipCommand command = new RelationshipCommand();
        command.init( record );
        repairs.put( record, command );
    }

    public void repairNode( long id, Consumer<NodeRecord> repair )
    {
        locks.acquireExclusive( ResourceTypes.NODE, id );
        NodeRecord stored = stores.getNodeStore().getRecord( id );
        NodeRecord changed = stored.clone();
        repair.accept( changed );

        NodeCommand command = new NodeCommand();
        command.init( stored, changed );
        repairs.put( stored, command );
    }

    public void repairRelationshipGroup( long id, Consumer<RelationshipGroupRecord> repair )
    {
        RelationshipGroupRecord record = stores.getRelationshipGroupStore().getRecord( id );
        locks.acquireExclusive( ResourceTypes.NODE, record.getOwningNode() );
        repair.accept( record );

        RelationshipGroupCommand command = new RelationshipGroupCommand();
        command.init( record );
        repairs.put( record, command );
    }

    public void repairProperty( long id, Consumer<PropertyRecord> repair )
    {
        PropertyRecord stored = stores.getPropertyStore().getRecord( id );
        PropertyRecord changed = stored.clone();
        repair.accept( changed );

        PropertyCommand command = new PropertyCommand();
        command.init( stored, changed );
        repairs.put( stored, command );
    }

    public void success()
    {
        this.successful = true;
    }

    @Override
    public void close() throws TransactionFailureException
    {
        try
        {
            if ( successful )
            {
                PhysicalTransactionRepresentation representation =
                        new PhysicalTransactionRepresentation( repairs.values() );
                TransactionHeaderInformation header = TransactionHeaderInformationFactory.DEFAULT.create();
                representation.setHeader( header.getAdditionalHeader(), header.getMasterId(), header.getAuthorId(),
                        startTime, startTx, clock.currentTimeMillis(), -1 );
                try ( LockGroup locks = new LockGroup() )
                {
                    commitProcess.commit( representation, locks, NULL, EXTERNAL );
                }
            }
        }
        finally
        {
            locks.close();
        }
    }
}
