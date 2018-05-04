package org.neo4j.storageengine.api;

import java.util.Collection;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

public interface TransactionInterface {

	CommandCreationContext allocateCommandCreationContext();

    /**
     * Generates a list of {@link StorageCommand commands} representing the changes in the given transaction state
     * ({@code state}.
     * The returned commands can be used to form {@link CommandsToApply} batches, which can be applied to this
     * storage using {@link #apply(CommandsToApply, TransactionApplicationMode)}.
     * The reason this is separated like this is that the generated commands can be used for other things
     * than applying to storage, f.ex replicating to another storage engine.
     * @param target {@link Collection} to put {@link StorageCommand commands} into.
     * @param state {@link ReadableTransactionState} representing logical store changes to generate commands for.
     * @param storageStatement {@link StorageStatement} to use for reading store state during creation of commands.
     * @param locks {@link ResourceLocker} can grab additional locks.
     * This locks client still have the potential to acquire more locks at this point.
     * TODO we should try to get rid of this locking mechanism during creation of commands
     * The reason it's needed is that some relationship changes in the record storage engine
     * needs to lock prev/next relationships and these changes happens when creating commands
     * The EntityLocker interface is a subset of Locks.Client interface, just to fit in while it's here.
     * @param lastTransactionIdWhenStarted transaction id which was seen as last committed when this
     * transaction started, i.e. before any changes were made and before any data was read.
     * TODO Transitional (Collection), might be {@link Stream} or whatever.
     *
     * @throws TransactionFailureException if command generation fails or some prerequisite of some command
     * didn't validate, for example if trying to delete a node that still has relationships.
     * @throws CreateConstraintFailureException if this transaction was set to create a constraint and that failed.
     * @throws ConstraintValidationException if this transaction was set to create a constraint
     * and some data violates that constraint.     */
    void createCommands(
            Collection<StorageCommand> target,
            ReadableTransactionState state,
            StorageStatement storageStatement,
            ResourceLocker locks,
            long lastTransactionIdWhenStarted )
            throws TransactionFailureException, ConstraintValidationException, CreateConstraintFailureException;

    /**
     * Apply a batch of groups of commands to this storage.
     *
     * @param batch batch of groups of commands to apply to storage.
     * @param mode {@link TransactionApplicationMode} when applying.
     * @throws Exception if an error occurs during application.
     */
    void apply( CommandsToApply batch, TransactionApplicationMode mode ) throws Exception;

    /**
     * @return a {@link CommandReaderFactory} capable of returning {@link CommandReader commands readers}
     * for specific log entry versions.
     */
    CommandReaderFactory commandReaderFactory();
}
