package org.neo4j.kernel.impl.api.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class IndexingService
{
    private final Map<Long, AtomicDelegatingIndexContext> contexts =
            new ConcurrentHashMap<Long, AtomicDelegatingIndexContext>();
    
    public IndexContext getContext( IndexRule index )
    {
    }
}
