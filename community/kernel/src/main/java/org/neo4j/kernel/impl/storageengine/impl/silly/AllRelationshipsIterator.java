package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

class AllRelationshipsIterator extends PrimitiveLongCollections.PrimitiveLongBaseIterator implements RelationshipIterator
{
    private final ConcurrentMap<Long,RelationshipData> relationships;
    private final Iterator<Long> iterator;

    public AllRelationshipsIterator( ConcurrentMap<Long,RelationshipData> relationships )
    {
        this.relationships = relationships;
        this.iterator = relationships.keySet().iterator();
    }

    @Override
    protected boolean fetchNext()
    {
        return iterator.hasNext() ? next( iterator.next() ) : false;
    }

    @Override
    public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId, RelationshipVisitor<EXCEPTION> visitor )
            throws EXCEPTION
    {
        return relationships.get( relationshipId ).visit( visitor );
    }
}
