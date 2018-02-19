/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt.runtime;

import io.netty.channel.Channel;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.bolt.v1.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.v1.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.util.FeatureToggles;

import static java.util.concurrent.TimeUnit.SECONDS;

public class DefaultBoltConnection implements BoltConnection
{
    protected static final int DEFAULT_MAX_BATCH_SIZE = FeatureToggles.getInteger( BoltKernelExtension.class, "max_batch_size", 100 );

    private final String id;

    private final BoltChannel channel;
    private final BoltStateMachine machine;
    private final BoltConnectionLifetimeListener listener;
    private final BoltConnectionQueueMonitor queueMonitor;

    private final Log log;
    private final Log userLog;

    private final int maxBatchSize;
    private final List<Job> batch;
    private final LinkedBlockingQueue<Job> queue = new LinkedBlockingQueue<>();

    private final AtomicBoolean shouldClose = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public DefaultBoltConnection( BoltChannel channel, BoltStateMachine machine, LogService logService, BoltConnectionLifetimeListener listener,
            BoltConnectionQueueMonitor queueMonitor )
    {
        this( channel, machine, logService, listener, queueMonitor, DEFAULT_MAX_BATCH_SIZE );
    }

    public DefaultBoltConnection( BoltChannel channel, BoltStateMachine machine, LogService logService, BoltConnectionLifetimeListener listener,
            BoltConnectionQueueMonitor queueMonitor, int maxBatchSize )
    {
        this.id = channel.id();
        this.channel = channel;
        this.machine = machine;
        this.listener = listener;
        this.queueMonitor = queueMonitor;
        this.log = logService.getInternalLog( getClass() );
        this.userLog = logService.getUserLog( getClass() );
        this.maxBatchSize = maxBatchSize;
        this.batch = new ArrayList<>( maxBatchSize );
    }

    @Override
    public String id()
    {
        return id;
    }

    @Override
    public SocketAddress localAddress()
    {
        return channel.serverAddress();
    }

    @Override
    public SocketAddress remoteAddress()
    {
        return channel.clientAddress();
    }

    @Override
    public Channel channel()
    {
        return channel.rawChannel();
    }

    @Override
    public String principal()
    {
        return machine.owner();
    }

    @Override
    public boolean hasPendingJobs()
    {
        return !queue.isEmpty();
    }

    @Override
    public void start()
    {
        notifyCreated();
    }

    @Override
    public void enqueue( Job job )
    {
        queue.offer( job );
        notifyEnqueued( job );
    }

    @Override
    public boolean processNextBatch()
    {
        return processNextBatch( maxBatchSize );
    }

    private boolean processNextBatch( int batchCount )
    {
        try
        {
            boolean waitForMessage = false;
            boolean loop = false;
            do
            {
                if ( !shouldClose.get() )
                {
                    if ( waitForMessage || !queue.isEmpty() )
                    {
                        queue.drainTo( batch, batchCount );
                        if ( batch.size() == 0 )
                        {
                            while ( true )
                            {
                                Job nextJob = queue.poll( 10, SECONDS );
                                if ( nextJob != null )
                                {
                                    batch.add( nextJob );

                                    break;
                                }
                            }
                        }
                        notifyDrained( batch );

                        while ( batch.size() > 0 )
                        {
                            Job current = batch.remove( 0 );

                            current.perform( machine );
                        }

                        loop = machine.shouldStickOnThread();
                        waitForMessage = loop;
                    }
                }
            }
            while ( loop );

            assert !machine.hasOpenStatement();
        }
        catch ( BoltConnectionAuthFatality ex )
        {
            // do not log
            shouldClose.set( true );
        }
        catch ( BoltProtocolBreachFatality ex )
        {
            shouldClose.set( true );
            log.error( String.format( "Protocol breach detected in bolt session '%s'.", id() ), ex );
        }
        catch ( Throwable t )
        {
            shouldClose.set( true );
            userLog.error( String.format( "Unexpected error detected in bolt session '%s'.", id() ), t );
        }
        finally
        {
            if ( shouldClose.get() )
            {
                close();
            }
        }

        return !closed.get();
    }

    @Override
    public void handleSchedulingError( Throwable t )
    {
        Neo4jError error;
        if ( ExceptionUtils.hasCause( t, RejectedExecutionException.class ) )
        {
            error = Neo4jError.from( Status.Request.NoThreadsAvailable, Status.Request.NoThreadsAvailable.code().description() );
        }
        else
        {
            error = Neo4jError.fatalFrom( t );
        }

        userLog.error( String.format( "Unexpected error during scheduling of bolt session '%s'.", id() ), t );
        machine.markFailed( error );
        processNextBatch( 1 );
    }

    @Override
    public void interrupt()
    {
        machine.interrupt();
    }

    @Override
    public void stop()
    {
        if ( shouldClose.compareAndSet( false, true ) )
        {
            machine.terminate();

            if ( !hasPendingJobs() )
            {
                processNextBatch( 0 );
            }
        }
    }

    private void close()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            try
            {
                machine.close();
            }
            catch ( Throwable t )
            {
                log.error( String.format( "Unable to close bolt session '%s'.", id() ), t );
            }
            finally
            {
                notifyDestroyed();
            }
        }
    }

    private void notifyCreated()
    {
        if ( listener != null )
        {
            listener.created( this );
        }
    }

    private void notifyDestroyed()
    {
        if ( listener != null )
        {
            listener.closed( this );
        }
    }

    private void notifyEnqueued( Job job )
    {
        if ( queueMonitor != null )
        {
            queueMonitor.enqueued( this, job );
        }
    }

    private void notifyDrained( List<Job> jobs )
    {
        if ( queueMonitor != null )
        {
            queueMonitor.drained( this, jobs );
        }
    }
}
