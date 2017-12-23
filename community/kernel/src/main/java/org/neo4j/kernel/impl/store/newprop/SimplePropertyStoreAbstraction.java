package org.neo4j.kernel.impl.store.newprop;

import java.io.Closeable;

import org.neo4j.values.storable.Value;

/**
 * A bare-bone interface to accessing, both reading and writing, property data. Implementations are
 * assumed to be thread-safe, although not regarding multiple threads (transactions) manipulating
 * same records simultaneously. External synchronization/avoidance is required.
 *
 * The {@code long entity} references are generated and managed externally.
 */
public interface SimplePropertyStoreAbstraction extends Closeable
{
    // Writing
    /**
     * The (first) property record id.
     *
     * @return the new record id, potentially the same {@code id} that got passed in.
     */
    long set( long id, int key, Value value );

    /**
     * @return {@code true} if this removal causes the record to be empty, in which case the id could
     * be freed for later reuse. The freeing and management in general is done externally.
     */
    boolean remove( long id, int key );

    // Reading
    /**
     * @return whether or not the property exists.
     */
    boolean has( long id, int key );

    /**
     * The idea with this method is to get the data, i.e. read it byte for byte or whatever, but
     * not do deserialization, because we're not really interested in that deserialization cost since
     * it's more or less the same in all conceivable implementations and will most likely tower above
     * the cost of getting to the property data.
     *
     * @return number of property data bytes visited.
     */
    Value get( long id, int key );

    /**
     * @return number of properties visited.
     */
    int all( long id, PropertyVisitor visitor );

    interface PropertyVisitor
    {
        void accept( long id, int key );
    }
}
