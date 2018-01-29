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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.newprop.BitsetFreelist.Marker;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;

import static org.junit.Assert.assertEquals;

import static java.nio.file.StandardOpenOption.CREATE;

import static org.neo4j.test.rule.PageCacheAndDependenciesRule.pageCacheAndDependencies;
import static org.neo4j.test.rule.PageCacheRule.config;

public class BitsetFreelistTest
{
    @Rule
    public final PageCacheAndDependenciesRule storage = pageCacheAndDependencies().pageCacheConfig( config().withInconsistentReads( false ) ).build();
    private BitsetFreelist freelist;

    @Before
    public void setUp() throws IOException
    {
        PageCache pageCache = storage.pageCache();
        freelist = new BitsetFreelist( pageCache.map( storage.directory().file( "index" ), pageCache.pageSize(), CREATE ) );
    }

    @After
    public void shutDown() throws IOException
    {
        freelist.close();
    }

    @Test
    public void shouldPickOneUnitFromHighIdIfNoIdInFreelist() throws Exception
    {
        // given

        // when
        long id = freelist.allocate( 1 );

        // then
        assertEquals( 0, id );
    }

    @Test
    public void shouldPickOneUnitFromFreelist() throws Exception
    {
        // given
        int units = 1;
        long id = freelist.allocate( units );
        try ( Marker marker = freelist.marker() )
        {
            marker.mark( id, units, true );
        }

        // when
        try ( Marker marker = freelist.marker() )
        {
            marker.mark( id, units, false );
        }
        long secondId = freelist.allocate( units );

        // then
        assertEquals( id, secondId );
    }

    @Test
    public void shouldCreateLotsAndDeleteEveryOtherShouldFindThem() throws Exception
    {
        // given
        int count = 10_000;
        long[] ids = new long[count];
        int slotSize = 2;
        try ( Marker marker = freelist.marker() )
        {
            for ( int i = 0; i < count; i++ )
            {
                ids[i] = freelist.allocate( slotSize );
                marker.mark( ids[i], slotSize, true );
            }
        }

        // when
        try ( Marker marker = freelist.marker() )
        {
            for ( int i = 0; i < count; i += 2 )
            {
                marker.mark( ids[i], slotSize, false );
            }
        }

        // then
        for ( int i = 0; i < count; i += 2 )
        {
            long id = freelist.allocate( slotSize );
            assertEquals( "" + i, ids[i], id );
        }
    }
}
