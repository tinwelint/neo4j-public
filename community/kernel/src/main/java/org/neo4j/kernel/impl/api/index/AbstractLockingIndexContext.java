package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public abstract class AbstractLockingIndexContext extends AbstractDelegatingIndexContext
{
    private final ReadWriteLock lock;

    public AbstractLockingIndexContext()
    {
        this.lock = new ReentrantReadWriteLock( );
    }

    protected ReadWriteLock getLock()
    {
        return lock;
    }

    public void create()
   {
       lock.readLock().lock();
       try {
           super.create();
       }
       finally {
           lock.readLock().unlock();
       }
   }

   public void ready()
   {
       lock.readLock().lock();
       try {
           super.ready();
       }
       finally {
           lock.readLock().unlock();
       }
   }

   public void update( Iterable<NodePropertyUpdate> updates )
   {
       lock.readLock().lock();
       try {
           super.update( updates );
       }
       finally {
           lock.readLock().unlock();
       }
   }

   public void drop()
   {
       lock.readLock().lock();
       try {
           super.drop();
       }
       finally {
           lock.readLock().unlock();
       }
   }

   public IndexRule getIndexRule()
   {
       lock.readLock().lock();
       try {
          return super.getIndexRule();
       }
       finally {
           lock.readLock().unlock();
       }
   }    
}
