/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tooling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.Locks.Client;
import org.neo4j.kernel.impl.locking.Locks.ResourceType;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;

import static org.neo4j.helpers.Format.date;
import static org.neo4j.helpers.Format.time;

/**
 * The purpose of this lock tracker is to aid in finding issues where locks gets stuck in HA clusters.
 * It's a global map of all locks for all instances, and it's required that all cluster members run inside
 * the same JVM, i.e. the same JVM as this lock tracker does.
 */
public class LockTracker
{
    private LockTracker()
    {
    }
    
    public static final LockTracker INSTANCE = new LockTracker();
    
    /**
     * Only master locks managers are spied upon.
     */
    private final ConcurrentMap<Integer/*serverId*/,
                        ConcurrentMap<Locks,
                            ConcurrentMap<Client,
                                Map<SpyLock, AtomicInteger>>>> locks = new ConcurrentHashMap<>();

    // For tracking inside Client/Server
    private final AtomicInteger nextRequestId = new AtomicInteger();
    private final ConcurrentMap<Integer, Thread> requestByThread = new ConcurrentHashMap<>();
    private final ConcurrentMap<Thread, Thread> actualToWorkerThread = new ConcurrentHashMap<>();
    
    public int trackClient()
    {
        int requestId = nextRequestId.incrementAndGet();
        requestByThread.put( requestId, currentThread() );
        return requestId;
    }
    
    public void mapWorker( int requestId )
    {
        Thread actual = requestByThread.get( requestId );
        actualToWorkerThread.put( currentThread(), actual );
    }
    
    public void untrackClient( int requestId )
    {
        requestByThread.remove( requestId );
    }
    
    public Locks spyOnLockManager( Locks manager, int serverId )
    {
        ConcurrentMap<Locks, ConcurrentMap<Client, Map<SpyLock, AtomicInteger>>> locksMap = locks.get( serverId );
        if ( locksMap == null )
        {
            locks.putIfAbsent( serverId, new ConcurrentHashMap<Locks, ConcurrentMap<Client, Map<SpyLock, AtomicInteger>>>() );
        }
        ConcurrentMap<Client, Map<SpyLock, AtomicInteger>> clientMap = new ConcurrentHashMap<>();
        Locks spy = new LocksSpy( manager, clientMap );
        locks.get( serverId ).put( spy, clientMap );
        return spy;
    }
    
    public void printAllIKnow( StringLogger logger )
    {
        for ( Map.Entry<Integer,ConcurrentMap<Locks,ConcurrentMap<Client,Map<SpyLock, AtomicInteger>>>> entryLevel1 :
            locks.entrySet() )
        {
            logger.info( "Server " + entryLevel1.getKey() );
            for ( Map.Entry<Locks,ConcurrentMap<Client,Map<SpyLock, AtomicInteger>>> entryLevel2 : entryLevel1.getValue().entrySet() )
            {
                logger.info( "  " + entryLevel2.getKey() );
                for ( Map.Entry<Client,Map<SpyLock, AtomicInteger>> entryLevel3 : entryLevel2.getValue().entrySet() )
                {
                    if ( !entryLevel3.getValue().isEmpty() )
                    {
                        logger.info( "    " + entryLevel3.getKey() );
                        for ( Map.Entry<SpyLock, AtomicInteger> entryLevel4 : entryLevel3.getValue().entrySet() )
                        {
                            logger.info( "      " + entryLevel4.getKey() + ": " + entryLevel4.getValue().get() );
                        }
                    }
                }
                
                logger.info( "  History" );
                for ( String entry : ((LocksSpy)entryLevel2.getKey()).lockHistory )
                {
                    logger.info( "    " + entry );
                }
            }
        }
        
        logger.info( "\nNotifications" );
        for ( String notification : notifications )
        {
            logger.info( "  " + notification );
        }
    }
    
    private class LocksSpy implements Locks
    {
        private final Locks delegate;
        private final ConcurrentMap<Client, Map<SpyLock, AtomicInteger>> clientMap;
        private final Exception createdAt = new Exception( "Created at " + date() );
        private final List<String> lockHistory = Collections.synchronizedList( new ArrayList<String>() );

        public LocksSpy( Locks delegate, ConcurrentMap<Client, Map<SpyLock, AtomicInteger>> clientMap )
        {
            this.delegate = delegate;
            this.clientMap = clientMap;
        }
        
        @Override
        public String toString()
        {
            return format( "Locks %d created at %s", delegate.hashCode(), createdAt.getMessage() );
        }

        @Override
        public void init() throws Throwable
        {
            delegate.init();
        }

        @Override
        public void start() throws Throwable
        {
            delegate.start();
        }

        @Override
        public void stop() throws Throwable
        {
            delegate.stop();
        }

        @Override
        public void shutdown() throws Throwable
        {
            delegate.shutdown();
            lockHistory.clear();
        }

        @Override
        public Client newClient()
        {
            Map<SpyLock, AtomicInteger> resources = new HashMap<>();
            Client spy = new ClientSpy( delegate.newClient(), resources, clientMap, lockHistory );
            clientMap.put( spy, resources );
            return spy;
        }

        @Override
        public void accept( Visitor visitor )
        {
            delegate.accept( visitor );
        }
    }
    
    private class ClientSpy implements Client
    {
        private final Client delegate;
        private final Map<SpyLock, AtomicInteger> lockedEntities;
        private final Map<Client, Map<SpyLock, AtomicInteger>> clientMap;
        private final long createdAt = currentTimeMillis();
        private Thread lastTouchedBy;
        private String lastTouchedMethod;
        private Exception closedAt;
        private final List<String> lockHistory;

        public ClientSpy( Client delegate, Map<SpyLock, AtomicInteger> resources, Map<Client,
                Map<SpyLock, AtomicInteger>> clientMap, List<String> lockHistory )
        {
            this.delegate = delegate;
            this.lockedEntities = resources;
            this.clientMap = clientMap;
            this.lockHistory = lockHistory;
        }
        
        @Override
        public String toString()
        {
            String lastTouchedByString = actualToWorkerThread.containsKey( lastTouchedBy ) ?
                    lastTouchedBy.toString() + ", ACTUAL: " + actualToWorkerThread.get( lastTouchedBy ) :
                    lastTouchedBy.toString();
            return format( "Client %d %s at %d, last touched by %s '%s' %s", hashCode(), delegate.toString(),
                    createdAt,
                    lastTouchedByString,
                    lastTouchedMethod,
                    closedAt != null ? "CLOSED" : "" );
        }

        @Override
        public void acquireShared( ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
        {
            touch( "BEFORE acquireShared " + resourceType + ", " + Arrays.toString( resourceIds ) );
            delegate.acquireShared( resourceType, resourceIds );
            touch( "AFTER acquireShared " + resourceType + ", " + Arrays.toString( resourceIds ) );
            for ( long id : resourceIds )
            {
                addLock( new SpyLock( this, LockType.READ, resourceType, id ) );
            }
        }

        private void touch( String method )
        {
            if ( closedAt != null )
            {
                throw new IllegalStateException( "Tried to acquire a lock in a client that is already closed" );
            }
            
            lastTouchedBy = currentThread();
            lastTouchedMethod = method;
            lockHistory.add( time() + " " + this + " " + method );
        }

        @Override
        public void acquireExclusive( ResourceType resourceType, long... resourceIds )
                throws AcquireLockTimeoutException
        {
            touch( "BEFORE acquireExclusive " + resourceType + ", " + Arrays.toString( resourceIds ) );
            delegate.acquireExclusive( resourceType, resourceIds );
            touch( "AFTER acquireExclusive " + resourceType + ", " + Arrays.toString( resourceIds ) );
            for ( long id : resourceIds )
            {
                addLock( new SpyLock( this, LockType.WRITE, resourceType, id ) );
            }
        }

        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long... resourceIds )
        {
            touch( "tryExclusiveLock " + resourceType + ", " + Arrays.toString( resourceIds ) );
            if ( delegate.tryExclusiveLock( resourceType, resourceIds ) )
            {
                for ( long id : resourceIds )
                {
                    addLock( new SpyLock( this, LockType.WRITE, resourceType, id ) );
                }
                return true;
            }
            return false;
        }

        private void addLock( SpyLock spyLock )
        {
            AtomicInteger count = lockedEntities.get( spyLock );
            if ( count == null )
            {
                lockedEntities.put( spyLock, count = new AtomicInteger() );
            }
            count.incrementAndGet();
            lockHistory.add( time() + " +" + spyLock );
        }

        @Override
        public boolean trySharedLock( ResourceType resourceType, long... resourceIds )
        {
            touch( "trySharedLock " + resourceType + ", " + Arrays.toString( resourceIds ) );
            if ( delegate.trySharedLock( resourceType, resourceIds ) )
            {
                for ( long id : resourceIds )
                {
                    addLock( new SpyLock( this, LockType.READ, resourceType, id ) );
                }
                return true;
            }
            return false;
        }

        @Override
        public void releaseShared( ResourceType resourceType, long... resourceIds )
        {
            touch( "releaseShared" );
            delegate.releaseShared( resourceType, resourceIds );
            for ( long id : resourceIds )
            {
                removeLock( new SpyLock( this, LockType.READ, resourceType, id ) );
            }
        }

        private void removeLock( SpyLock spyLock )
        {
            AtomicInteger count = lockedEntities.get( spyLock );
            int current = count.decrementAndGet();
            assert current >= 0;
            if ( current == 0 )
            {
                lockedEntities.remove( spyLock );
            }
            lockHistory.add( time() + " -" + spyLock.toString() );
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long... resourceIds )
        {
            touch( "releaseExclusive" );
            delegate.releaseExclusive( resourceType, resourceIds );
            for ( long id : resourceIds )
            {
                removeLock( new SpyLock( this, LockType.WRITE, resourceType, id ) );
            }
        }

        @Override
        public void releaseAllShared()
        {
            touch( "releaseAllShared" );
            delegate.releaseAllShared();
            releaseAllByType( LockType.READ );
        }

        private void releaseAllByType( LockType type )
        {
            Iterator<SpyLock> lockIterator = lockedEntities.keySet().iterator();
            while ( lockIterator.hasNext() )
            {
                SpyLock lock = lockIterator.next();
                if ( lock.type == type )
                {
                    lockIterator.remove();
                }
            }
        }

        @Override
        public void releaseAllExclusive()
        {
            touch( "releaseAllExclusive" );
            delegate.releaseAllExclusive();
            releaseAllByType( LockType.WRITE );
        }

        @Override
        public void releaseAll()
        {
            touch( "releaseAll" );
            delegate.releaseAll();
            lockedEntities.clear();
            lockHistory.add( "CLEAR " + this );
        }

        @Override
        public void close()
        {
            touch( "close" );
            delegate.close();
            lockedEntities.clear();
            closedAt = new Exception( "closed " + time() );
            lockHistory.add( "CLOSE " + this );
        }

        @Override
        public long getIdentifier()
        {
            return delegate.getIdentifier();
        }
    }
    
    private class SpyLock
    {
        private final LockType type;
        private final ResourceType resourceType;
        private final long resourceId;
        private final long created = currentTimeMillis();
        private final ClientSpy clientSpy;
        
        public SpyLock( ClientSpy clientSpy, LockType type, ResourceType resourceType, long resourceId )
        {
            this.clientSpy = clientSpy;
            this.type = type;
            this.resourceType = resourceType;
            this.resourceId = resourceId;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (resourceId ^ (resourceId >>> 32));
            result = prime * result + resourceType.typeId();
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj == null )
            {
                return false;
            }
            if ( getClass() != obj.getClass() )
            {
                return false;
            }
            SpyLock other = (SpyLock) obj;
            if ( resourceId != other.resourceId )
            {
                return false;
            }
            if ( resourceType != other.resourceType )
            {
                return false;
            }
            if ( type != other.type )
            {
                return false;
            }
            return true;
        }

        @Override
        public String toString()
        {
            return "Lock [" + type + ", " + resourceType + ", " + resourceId + ", " + date( created ) +
                    " by " + clientSpy.toString() + "]";
        }
    }
    
    private final List<String> notifications = Collections.synchronizedList( new ArrayList<String>() );

    public void notifiedWaitingThread( Thread waitingThread, Object resource, LockType lockType )
    {
        notifications.add( time() + " " + currentThread() + " actual " + actualToWorkerThread.get( currentThread() ) +
                " notified " + waitingThread + " about " + resource + " " + lockType );
    }
}
