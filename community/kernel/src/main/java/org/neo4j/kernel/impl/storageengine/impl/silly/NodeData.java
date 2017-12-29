package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.helpers.collection.NestingIterator;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.newapi.Labels;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;

import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;

class NodeData implements NodeItem
{
    private final long id;
    private final PrimitiveIntSet labels = Primitive.intSet();
    private final ConcurrentMap<Integer,PropertyData> properties = new ConcurrentHashMap<>();
    private final ConcurrentMap<TypeAndDirection,ConcurrentMap<Long,RelationshipData>> relationships = new ConcurrentHashMap<>();

    NodeData( long id )
    {
        this.id = id;
    }

    @Override
    public long id()
    {
        return id;
    }

    LabelSet labelSet()
    {
        synchronized ( labels )
        {
            return Labels.from( labels );
        }
    }

    @Override
    public PrimitiveIntSet labels()
    {
        return labels;
    }

    ConcurrentMap<Integer,PropertyData> properties()
    {
        return properties;
    }

    ConcurrentMap<TypeAndDirection,ConcurrentMap<Long,RelationshipData>> relationships()
    {
        return relationships;
    }

    @Override
    public boolean isDense()
    {
        return true;
    }

    @Override
    public boolean hasLabel( int labelId )
    {
        return labels.contains( labelId );
    }

    @Override
    public long nextGroupId()
    {
        return 0;
    }

    @Override
    public long nextRelationshipId()
    {
        return 0;
    }

    @Override
    public long nextPropertyId()
    {
        return 0;
    }

    @Override
    public Lock lock()
    {
        return NO_LOCK;
    }

    Iterator<RelationshipData> relationships( Direction direction )
    {
        return relationships( key -> key.isDirection( direction ) );
    }

    Iterator<RelationshipData> relationships( Direction direction, IntPredicate typeIds )
    {
        return relationships( key -> key.isDirection( direction ) && key.isType( typeIds ) );
    }

    private Iterator<RelationshipData> relationships( Predicate<? super TypeAndDirection> specification )
    {
        return new NestingIterator<RelationshipData,TypeAndDirection>( filter( specification,
                relationships.keySet().iterator() ) )
        {
            @Override
            protected Iterator<RelationshipData> createNestedIterator( TypeAndDirection key )
            {
                return relationships.get( key ).values().iterator();
            }
        };
    }
}
