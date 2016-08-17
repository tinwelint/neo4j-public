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

import java.io.File;
import org.neo4j.function.Consumer;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

/**
 * Fix for relationship inconsistencies in https://neotechnology.zendesk.com/agent/tickets/3483
 *
 * Written assuming those records haven't changed further (which they naturally shouldn't have done
 * since even traversing them produces exceptions). Also without store or logs to look at so best
 * (although good) guesses, although not a lot of guessing was needed.
 */
public class ZD3483InconsistenciesFix
{
    private final GraphDatabaseAPI db;

    public ZD3483InconsistenciesFix( GraphDatabaseAPI db )
    {
        this.db = db;
    }

    public static void main( String[] args ) throws Throwable
    {
        if ( args.length == 0 )
        {
            System.out.println( "Please provide store directory" );
            System.exit( 1 );
        }

        File storeDir = new File( args[0] );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            new ZD3483InconsistenciesFix( db ).doRepair();
        }
        finally
        {
            db.shutdown();
        }
    }

    public void doRepair() throws Throwable
    {
        DependencyResolver resolver = db.getDependencyResolver();
        NeoStores stores = resolver.resolveDependency( NeoStores.class );

        // Get records and validate them
        // Perform the changes
        try ( Locks.Client locks = resolver.resolveDependency( Locks.class ).newClient();
                RepairTransaction tx = new RepairTransaction( locks, stores,
                        resolver.resolveDependency( TransactionCommitProcess.class ), SYSTEM_CLOCK,
                        resolver.resolveDependency( TransactionIdStore.class ) ) )
        {
            tx.repairRelationship( 74753399, new Consumer<RelationshipRecord>()
            {
                @Override
                public void accept( RelationshipRecord relationship )
                {
                    validateRelationshipAssumptions( relationship,
                            26656330, 663261, 2, 74419400, 74430137, 74753399, 74306456 );
                    relationship.setSecondNextRel( NO_NEXT_RELATIONSHIP.intValue() );
                }
            } );
            tx.repairRelationship( 74430152, new Consumer<RelationshipRecord>()
            {
                @Override
                public void accept( RelationshipRecord relationship )
                {
                    validateRelationshipAssumptions( relationship,
                            26656330, 1345167, 2, 74430165, 74430148, 80981547, 51551984 );
                    relationship.setFirstNextRel( NO_NEXT_RELATIONSHIP.intValue() );
                }
            } );
            tx.repairRelationship( 74306456, new Consumer<RelationshipRecord>()
            {
                @Override
                public void accept( RelationshipRecord relationship )
                {
                    validateRelationshipAssumptions( relationship,
                            27582139, 663219, 2, 74306455, 74306461, 74430148, 74298862 );
                    relationship.setSecondPrevRel( NO_NEXT_RELATIONSHIP.intValue() );
                }
            } );
            tx.repairRelationship( 74430148, new Consumer<RelationshipRecord>()
            {
                @Override
                public void accept( RelationshipRecord relationship )
                {
                    validateRelationshipAssumptions( relationship,
                            26656330, 663261, 2, 74419400, 74430137, 74753399, 74306456 );
                    relationship.setInUse( false );
                }
            } );
            tx.success();
        }
    }

    private static void validateRelationshipAssumptions( RelationshipRecord relationship,
            long sourceNode, long targetNode, int type,
            long sourcePrev, long sourceNext, long targetPrev, long targetNext )
    {
        validateAssumption( "Source node", sourceNode, relationship.getFirstNode() );
        validateAssumption( "Source prev", sourcePrev, relationship.getFirstPrevRel() );
        validateAssumption( "Source next", sourceNext, relationship.getFirstNextRel() );
        validateAssumption( "Target node", targetNode, relationship.getSecondNode() );
        validateAssumption( "Target prev", targetPrev, relationship.getSecondPrevRel() );
        validateAssumption( "Target next", targetNext, relationship.getSecondNextRel() );
        validateAssumption( "Type", type, relationship.getType() );
    }

    /**
     * This method is here because this patching application performs small incisions, each one getting
     * a particular record and assuming things about it before making changes. This method will validate
     * those assumptions before making those changes.
     *
     * @param message to print if an assumption didn't validate.
     * @param expectedValue
     * @param actualValue
     */
    private static void validateAssumption( String message, long expectedValue, long actualValue )
    {
        if ( expectedValue != actualValue )
        {
            throw new AssertionError( "False assumption about " + message + ", expected " + expectedValue +
                    " but was " + actualValue );
        }
    }
}
