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
package org.neo4j.kernel.impl.store.newprop;

import java.io.File;
import java.io.IOException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.io.ByteUnit.kibiBytes;

public class ProposedFormat implements SimplePropertyStoreAbstraction
{
    static final int HEADER_ENTRY_SIZE = Integer.BYTES;
    static final int RECORD_HEADER_SIZE = Short.BYTES;

    private final Store store;

    public ProposedFormat( PageCache pageCache, File directory ) throws IOException
    {
        this.store = new Store( pageCache, directory, "main" );
    }

    class Reader implements Read
    {
        protected final PageCursor cursor;
        private final Visitor hasVisitor = new HasVisitor( store );
        private final Visitor getVisitor = new GetVisitor( store );
        private final Visitor getLightVisitor = new GetLightVisitor( store );

        Reader() throws IOException
        {
            cursor = store.readCursor();
        }

        // For Writer subclass
        Reader( PageCursor cursor )
        {
            this.cursor = cursor;
        }

        @Override
        public boolean has( long id, int key ) throws IOException
        {
            hasVisitor.setKey( key );
            store.access( id, cursor, hasVisitor );
            return hasVisitor.booleanState;
        }

        @Override
        public Value get( long id, int key ) throws IOException
        {
            getVisitor.setKey( key );
            store.access( id, cursor, getVisitor );
            return getVisitor.readValue != null ? getVisitor.readValue : Values.NO_VALUE;
        }

        @Override
        public int getWithoutDeserializing( long id, int key ) throws IOException
        {
            getLightVisitor.setKey( key );
            store.access( id, cursor, getLightVisitor );
            return (int) getLightVisitor.longState;
        }

        @Override
        public int all( long id, PropertyVisitor visitor ) throws IOException
        {
            return 0;
        }

        @Override
        public void close() throws IOException
        {
            cursor.close();
        }
    }

    class Writer extends Reader implements Write
    {
        private final SetVisitor setVisitor = new SetVisitor( store );
        private final Visitor removeVisitor = new RemoveVisitor( store );

        Writer() throws IOException
        {
            super( store.writeCursor() );
        }

        @Override
        public long set( long id, int key, Value value ) throws IOException
        {
            if ( Record.NULL_REFERENCE.is( id ) )
            {
                // Allocate room, some number of units to start off with and then grow from there.
                // In a real scenario we'd probably have a method setting multiple properties and so
                // we'd know how big our record would be right away. This is just to prototype the design
                id = store.allocate( 1 );
            }
            // Read header and see if property by the given key already exists
            // For now let's store the number of header entries as a 2B entry first
            setVisitor.setKey( key );
            setVisitor.setValue( value );
            store.access( id, cursor, setVisitor );
            return setVisitor.longState;
        }

        @Override
        public long remove( long id, int key ) throws IOException
        {
            removeVisitor.setKey( key );
            store.access( id, cursor, removeVisitor );
            return id;
        }
    }

    @Override
    public Write newWrite() throws IOException
    {
        return new Writer();
    }

    @Override
    public Read newRead() throws IOException
    {
        return new Reader();
    }

    @Override
    public void close() throws IOException
    {
        store.close();
    }

    @Override
    public long storeSize() throws IOException
    {
        return store.storeFile.getLastPageId() * kibiBytes( 8 );
    }
}
