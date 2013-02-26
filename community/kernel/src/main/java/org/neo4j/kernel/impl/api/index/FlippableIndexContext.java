package org.neo4j.kernel.impl.api.index;

public class FlippableIndexContext extends AbstractLockingIndexContext
{
    private IndexContext delegate;

    public FlippableIndexContext( IndexContext delegate )
    {
        this.delegate = delegate;
    }

    public IndexContext getDelegate()
    {
        getLock().readLock().lock();
        try {
             return delegate;
        }
        finally {
             getLock().readLock().unlock();
        }
    }

    public void setDelegate( IndexContext delegate )
    {
        getLock().writeLock().lock();
        try {
            this.delegate = delegate;
        }
        finally {
            getLock().writeLock().unlock();
        }
    }


}
