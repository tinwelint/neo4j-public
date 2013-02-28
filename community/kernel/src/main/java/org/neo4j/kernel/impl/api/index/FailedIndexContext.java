package org.neo4j.kernel.impl.api.index;

import java.util.Iterator;

import org.neo4j.kernel.api.InternalIndexState;

public class FailedIndexContext implements IndexContext
{
    private final IndexPopulator writer;
    private final Throwable cause;

    public FailedIndexContext( IndexPopulator writer )
    {
        this.writer = writer;
        this.cause = null;
    }

    public FailedIndexContext( IndexPopulator writer, Throwable cause )
    {
        this.writer = writer;
        this.cause = cause;
    }

    @Override
    public void create()
    {
        throw new UnsupportedOperationException( "Unable to create index, it is in a failed state.", cause );
    }

    @Override
    public void update( Iterator<NodePropertyUpdate> updates )
    {
        // intentionally swallow updates, we're failed and nothing but repopulation or dropIndex will solve this
    }

    @Override
    public void drop()
    {
        writer.dropIndex();
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.FAILED;
    }

    @Override
    public void force()
    {
    }

}
