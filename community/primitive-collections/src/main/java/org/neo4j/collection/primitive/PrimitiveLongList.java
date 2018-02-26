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
package org.neo4j.collection.primitive;

import java.util.Arrays;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

/**
 * List implementation that holds primitive longs in array that grows on demand.
 */
public class PrimitiveLongList implements PrimitiveLongCollection
{
    private static final int DEFAULT_SIZE = 8;

    private long[] elements;
    private final int initialSize;
    private int size;

    PrimitiveLongList()
    {
        this( DEFAULT_SIZE );
    }

    PrimitiveLongList( int size )
    {
        initialSize = size;
        clear();
    }

    public void add( long element )
    {
        if ( elements.length == size )
        {
            ensureCapacity();
        }
        elements[size++] = element;
    }

    public long get( int position )
    {
        validatePosition( position );
        return elements[position];
    }

    private void validatePosition( int position )
    {
        if ( position >= size )
        {
            throw new IndexOutOfBoundsException( "Requested element: " + position + ", list size: " + size );
        }
    }

    public long remove( int position )
    {
        validatePosition( position );
        long result = elements[position];
        if ( position < size - 1 )
        {
            System.arraycopy( elements, position + 1, elements, position, size - (position + 1) );
        }
        size--;
        return result;
    }

    @Override
    public boolean isEmpty()
    {
        return size == 0;
    }

    @Override
    public void clear()
    {
        size = 0;
        elements = new long[initialSize];
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void close()
    {
        size = 0;
        elements = EMPTY_LONG_ARRAY;
    }

    @Override
    public PrimitiveLongIterator iterator()
    {
        return new PrimitiveLongListIterator();
    }

    @Override
    public <E extends Exception> void visitKeys( PrimitiveLongVisitor<E> visitor ) throws E
    {
        throw new UnsupportedOperationException();
    }

    public long[] toArray()
    {
        return Arrays.copyOf( elements, size );
    }

    private void ensureCapacity()
    {
        int newCapacity = elements.length << 1;
        if ( newCapacity < 0 )
        {
            throw new IllegalStateException( "Fail to increase list capacity." );
        }
        elements = Arrays.copyOf( elements, newCapacity );
    }

    private class PrimitiveLongListIterator extends PrimitiveLongCollections.PrimitiveLongBaseIterator
    {
        int cursor;

        @Override
        protected boolean fetchNext()
        {
            return cursor < size && next( elements[cursor++] );
        }
    }
}
