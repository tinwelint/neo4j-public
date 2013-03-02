package org.neo4j.kernel.impl.api.index;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;

/**
 * Keeps track of index rules.
 */
public class IndexRuleRepository
{
    private final Map<Long, Set<Long>> indexedProperties = new HashMap<Long, Set<Long>>();

    public Iterator<Long> getIndexedProperties( long labelId )
    {
        return indexedProperties.containsKey( labelId ) ?
                indexedProperties.get( labelId ).iterator() :
                emptyIterator();
    }

    public void add( IndexRule rule )
    {
        if(!indexedProperties.containsKey( rule.getLabel() ))
        {
            indexedProperties.put( rule.getLabel(), new HashSet<Long>());
        }

        indexedProperties.get( rule.getLabel() ).add( rule.getPropertyKey() );
    }

    public void remove( IndexRule rule )
    {
        if(indexedProperties.containsKey( rule.getLabel() ))
        {
            indexedProperties.get( rule.getLabel() ).remove( rule.getPropertyKey() );
        }
    }
}
