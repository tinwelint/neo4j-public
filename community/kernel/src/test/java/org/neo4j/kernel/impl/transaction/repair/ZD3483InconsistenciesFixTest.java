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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;

public class ZD3483InconsistenciesFixTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule( getClass() );

    private NeoStores stores;
    private RelationshipStore relationshipStore;
    private NodeStore nodeStore;

    @Before
    public void before()
    {
        stores = db.getDependencyResolver().resolveDependency( NeoStores.class );
        relationshipStore = stores.getRelationshipStore();
        nodeStore = stores.getNodeStore();
    }

    @Test
    public void shouldRepairTheirStore() throws Throwable
    {
        createGistOfInconsistency();

        // fake a chain in front of 663219, we're building simply:
        // Node[663219] -> Relationship[1] -> Relationship[2] -> Relationship[74306456]
        nodeStore.updateRecord( new NodeRecord( 663219, false, 1, -1, true ) );
        nodeStore.updateRecord( new NodeRecord( 1, false, 1, -1, true ) );
        relationshipStore.updateRecord( new RelationshipRecord( 1, true, 1, 663219, 2, 2/*degree*/, 2,
                2/*degree*/, 2, true, true ) );
        relationshipStore.updateRecord( new RelationshipRecord( 2, true, 1, 663219, 2, 1, -1,
                1, 74306456, false, false ) );

        // WHEN running the repair tool
        new ZD3483InconsistenciesFix( db ).doRepair();

        // THEN it should be fixed
        printTransactionLog();
        verifyFixed();
    }

    private void printTransactionLog() throws NoSuchTransactionException, IOException
    {
        LogicalTransactionStore txStore = db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
        try ( IOCursor<CommittedTransactionRepresentation> cursor =
                txStore.getTransactions( TransactionIdStore.BASE_TX_ID + 1 ) )
        {
            while ( cursor.next() )
            {
                System.out.println( cursor.get() );
            }
        }
    }

    private void verifyFixed()
    {
        assertEquals( Record.NO_NEXT_RELATIONSHIP.intValue(),
                relationshipStore.getRecord( 74753399 ).getSecondNextRel() );
        assertEquals( Record.NO_NEXT_RELATIONSHIP.intValue(),
                relationshipStore.getRecord( 74430152 ).getFirstNextRel() );
        assertEquals( Record.NO_NEXT_RELATIONSHIP.intValue(),
                relationshipStore.getRecord( 74306456 ).getSecondPrevRel() );
    }

    private void createGistOfInconsistency()
    {
        relationshipStore.setHighId( 75_000_000 );
        nodeStore.setHighId( 670_000 );
        relationshipStore.updateRecord( new RelationshipRecord( 74753399, true,
                26656330, 663261, 2, 74419400, 74430137, 74753399, 74306456, false, false ) );
        relationshipStore.updateRecord( new RelationshipRecord( 74430152, true,
                26656330, 1345167, 2, 74430165, 74430148, 80981547, 51551984, false, false ) );
        relationshipStore.updateRecord( new RelationshipRecord( 74430148, true,
                26656330, 663261, 2, 74419400, 74430137, 74753399, 74306456, false, false ) );
        relationshipStore.updateRecord( new RelationshipRecord( 74306456, true,
                27582139, 663219, 2, 74306455, 74306461, 74430148, 74298862, false, false ) );
    }
}
