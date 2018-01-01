package org.neo4j.kernel.impl.transaction.command;

/**
 * Many commands have before/after versions of their records. In some scenarios there's a need
 * to parameterize which of those to work with.
 */
public enum StorageCommandVersion
{
    /**
     * The "before" version of a command's record. I.e. the record how it looked before changes took place.
     */
    BEFORE,
    /**
     * The "after" version of a command's record. I.e. the record how it looks after changes took place.
     */
    AFTER
}
