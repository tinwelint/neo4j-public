/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.concurrent;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.function.Consumer;

import static java.lang.Thread.currentThread;

/**
 * {@code AsyncEvents} is a mechanism for queueing up events to be processed asynchronously in a background thread.
 *
 * The {@code AsyncEvents} object implements {@link Runnable}, so it can be passed to a thread pool, or given to a
 * dedicated thread. The runnable will then occupy a thread and dedicate it to background processing of events, until
 * the {@code AsyncEvents} is {@link AsyncEvents#shutdown()}.
 *
 * If events are sent to an {@code AsyncEvents} that has been shut down, then those events will be processed in the
 * foreground as a fall-back.
 *
 * Note, however, that no events are processed until the background thread is started.
 *
 * The {@code AsyncEvents} is given a {@link Consumer} of the specified event type upon construction, and will use it
 * for doing the actual processing of events once they have been collected.
 *
 * @param <T> The type of events the {@code AsyncEvents} will process.
 */
public abstract class AsyncEvents<T extends AsyncEvent> implements AsyncEventSender<T>, Runnable
{
    /**
     * Construct a new {@code AsyncEvents} instance, that will use the given consumer to process the events.
     *
     * @param eventConsumer The {@link Consumer} used for processing the events that are sent in.
     */
    public static <T extends AsyncEvent> AsyncEvents<T> newAsyncEvents( Consumer<T> eventConsumer )
    {
        return new PostPadding<>( eventConsumer );
    }

    private static final AsyncEvent endSentinel = new Sentinel();
    private static final AsyncEvent shutdownSentinel = new Sentinel();

    private final Consumer<T> eventConsumer;
    private volatile Thread runner;
    private volatile boolean shutdown;

    private AsyncEvents( Consumer<T> eventConsumer )
    {
        this.eventConsumer = eventConsumer;
    }

    @Override
    public void send( T event )
    {
        AsyncEvent prev = getAndSet( event );
        assert prev != null;
        event.next = prev;
        if ( prev == endSentinel )
        {
            LockSupport.unpark( runner );
        }
        else if ( prev == shutdownSentinel )
        {
            swapAndProcess( shutdownSentinel, eventConsumer );
        }
    }

    abstract AsyncEvent getAndSet( T event );

    @Override
    public void run()
    {
        assert runner == null : "A thread is already running " + runner;
        runner = currentThread();

        try
        {
            do
            {
                if ( swapAndProcess( endSentinel, eventConsumer ) && !shutdown )
                {
                    LockSupport.park( this );
                }
            }
            while ( !shutdown );

            swapAndProcess( shutdownSentinel, eventConsumer );
        }
        finally
        {
            runner = null;
        }
    }

    /**
     * Sets the top event of the stack to the sentinel value, then processes the events in the stack.
     *
     * @param sentinel the sentinel to use as a marker in the stack.
     * @param eventConsumer the consumer that should consume the events.
     * @return {@code true} if the stack still holds the sentinel value after processing completes.
     */
    abstract boolean swapAndProcess( AsyncEvent sentinel, Consumer<T> eventConsumer );

    /**
     * Initiate the shut down process of this {@code AsyncEvents} instance.
     *
     * This call does not block or otherwise wait for the background thread to terminate.
     */
    public void shutdown()
    {
        assert runner != null : "Already shut down";
        shutdown = true;
        LockSupport.unpark( runner );
    }

    public void awaitTermination() throws InterruptedException
    {
        while ( runner != null )
        {
            Thread.sleep( 10 );
        }
    }

    private static class Sentinel extends AsyncEvent
    {
        {
            next = this;
        }
    }

    private static abstract class PrePadding<T extends AsyncEvent> extends AsyncEvents<T>
    {
        public long b0, b1, b2, b3, b4, b5, b6, b8;

        private PrePadding( Consumer<T> eventConsumer )
        {
            super( eventConsumer );
        }
    }

    private static abstract class HotPart<T extends AsyncEvent> extends PrePadding<T>
    {
        // TODO use VarHandles in Java 9
        private static final AtomicReferenceFieldUpdater<HotPart,AsyncEvent> STACK =
                AtomicReferenceFieldUpdater.newUpdater( HotPart.class, AsyncEvent.class, "stack" );
        @SuppressWarnings( {"unused", "FieldCanBeLocal"} )
        private volatile AsyncEvent stack; // Accessed via AtomicReferenceFieldUpdater

        private HotPart( Consumer<T> eventConsumer )
        {
            super( eventConsumer );
            stack = endSentinel;
        }

        @Override
        AsyncEvent getAndSet( T event )
        {
            return STACK.getAndSet( this, event );
        }

        @Override
        boolean swapAndProcess( AsyncEvent sentinel, Consumer<T> eventConsumer )
        {
                AsyncEvent events = STACK.getAndSet( this, sentinel );
                process( events, eventConsumer );
                return stack == sentinel;
        }

        private void process( AsyncEvent events, Consumer<T> eventConsumer )
        {
            events = reverseAndStripEndMark( events );

            while ( events != null )
            {
                @SuppressWarnings( "unchecked" )
                T event = (T) events;
                eventConsumer.accept( event );
                events = events.next;
            }
        }
    }

    private static class PostPadding<T extends AsyncEvent> extends HotPart<T>
    {
        public long a0, a1, a2, a3, a4, a5, a6, a8;

        private PostPadding( Consumer<T> eventConsumer )
        {
            super( eventConsumer );
        }
    }

    static AsyncEvent reverseAndStripEndMark( AsyncEvent events )
    {
        AsyncEvent result = null;
        while ( events != endSentinel && events != shutdownSentinel )
        {
            AsyncEvent tmp;
            do
            {
                tmp = events.next;
            }
            while ( tmp == null );
            events.next = result;
            result = events;
            events = tmp;
        }
        return result;
    }
}
