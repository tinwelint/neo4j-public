package org.neo4j.kernel.impl.api.index;

public class FlippingIndexContext extends AtomicDelegatingIndexContext
{
    public FlippingIndexContext( FlipAwareIndexContext background, IndexContext online )
    {
        super( background );
    }
}
