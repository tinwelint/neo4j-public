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

import java.util.function.Function;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;

/**
 * Abstraction for accessing data from a {@link StorageEngine}.
 * <p>
 * A {@link StorageReader} must be {@link #initialize(TransactionalDependencies) initialized} before use in a
 * and {@link #acquire() acquired} before use in a statement, followed by {@link #release()} after statement is completed.
 * <p>
 * Creating and closing {@link StorageReader} can be somewhat costly, so there are benefits keeping these readers open
 * during a longer period of time, with the assumption that it's still one thread at a time using each.
 */
public interface StorageReader extends AutoCloseable, Read, ExplicitIndexRead, SchemaRead, CursorFactory
{
    /**
     * Initializes some dependencies that this reader needs. Typically called once per transaction.
     *
     * @param dependencies {@link TransactionalDependencies} needed to implement transaction-aware cursors.
     */
    void initialize( TransactionalDependencies dependencies );

    /**
     * Acquires this statement so that it can be used, should later be {@link #release() released}.
     * Typically called once per statement.
     * Since a {@link StorageReader} can be reused after {@link #release() released}, this call should
     * do initialization/clearing of state whereas data structures can be kept between uses.
     */
    void acquire();

    /**
     * Releases resources tied to this statement and makes this reader able to be {@link #acquire() acquired} again.
     */
    void release();

    /**
     * Closes this reader and all resources so that it can no longer be used nor {@link #acquire() acquired}.
     */
    @Override
    void close();

    /**
     * Reserves a node id for future use to store a node. The reason for it being exposed here is that
     * internal ids of nodes and relationships are publicly accessible all the way out to the user.
     * This will likely change in the future though.
     *
     * @return a reserved node id for future use.
     */
    long reserveNode();

    /**
     * Reserves a relationship id for future use to store a relationship. The reason for it being exposed here is that
     * internal ids of nodes and relationships are publicly accessible all the way out to the user.
     * This will likely change in the future though.
     *
     * @return a reserved relationship id for future use.
     */
    long reserveRelationship();

    int reserveLabelTokenId();

    int reservePropertyKeyTokenId();

    int reserveRelationshipTypeTokenId();

    /**
     * Releases a previously {@link #reserveNode() reserved} node id if it turns out to not actually being used,
     * for example in the event of a transaction rolling back.
     *
     * @param id reserved node id to release.
     */
    void releaseNode( long id );

    /**
     * Releases a previously {@link #reserveRelationship() reserved} relationship id if it turns out to not
     * actually being used, for example in the event of a transaction rolling back.
     *
     * @param id reserved relationship id to release.
     */
    void releaseRelationship( long id );

    <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StorageReader, T> factory );

    /*
     * Allocates cursors which are transaction-state-unaware
     *
     * Reading from storage in combination with transaction-state is a bit entangle now, since the introduction of the new kernel API,
     * where transaction-state awareness happens inside the actual cursors. The current approach is to disable transaction-state
     * awareness per cursor so that those few places that need reading only from storage will allocate cursors using the methods below.
     */

    NodeCursor allocateNodeCursorCommitted();

    PropertyCursor allocatePropertyCursorCommitted();

    RelationshipScanCursor allocateRelationshipScanCursorCommitted();

    RelationshipGroupCursor allocateRelationshipGroupCursorCommitted();
}
