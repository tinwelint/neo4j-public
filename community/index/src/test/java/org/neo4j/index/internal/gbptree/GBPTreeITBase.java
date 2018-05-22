/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.index.internal.gbptree;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;

import org.neo4j.cursor.RawCursor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Integer.max;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.test.rule.PageCacheRule.config;

public abstract class GBPTreeITBase<KEY,VALUE>
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    private TestLayout<KEY,VALUE> layout;
    private GBPTree<KEY,VALUE> index;

    private GBPTree<KEY,VALUE> createIndex()
            throws IOException
    {
        // some random padding
        layout = getLayout( random );
        PageCache pageCache = pageCacheRule.getPageCache( fs.get(), config().withPageSize( 512 ).withAccessChecks( true ) );
        return index = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout ).build();
    }

    abstract TestLayout<KEY,VALUE> getLayout( RandomRule random );

    @Test
    public void shouldStayCorrectAfterRandomModifications() throws Exception
    {
        // GIVEN
        try ( GBPTree<KEY,VALUE> index = createIndex() )
        {
            Comparator<KEY> keyComparator = layout;
            GBPTreeData<KEY,VALUE> data = new GBPTreeData<>( layout );
            int count = 100;
            int totalNumberOfRounds = 10;

            // WHEN
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < count; i++ )
                {
                    data.put( randomKey( random.random() ), randomValue( random.random() ), writer );
                }
            }

            for ( int round = 0; round < totalNumberOfRounds; round++ )
            {
                // THEN
                for ( int i = 0; i < count; i++ )
                {
                    KEY first = randomKey( random.random() );
                    KEY second = randomKey( random.random() );
                    KEY from;
                    KEY to;
                    if ( layout.keySeed( first ) < layout.keySeed( second ) )
                    {
                        from = first;
                        to = second;
                    }
                    else
                    {
                        from = second;
                        to = first;
                    }
                    data.assertEquals( index, from, to );
                }

                index.checkpoint( IOLimiter.unlimited() );
                randomlyModifyIndex( index, data, random.random(), (double) round / totalNumberOfRounds );
            }

            // and finally
            index.consistencyCheck();
        }
    }

    @Test
    public void shouldHandleRemoveEntireTree() throws Exception
    {
        // given
        try ( GBPTree<KEY,VALUE> index = createIndex() )
        {
            int numberOfNodes = 200_000;
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < numberOfNodes; i++ )
                {
                    writer.put( key( i ), value( i ) );
                }
            }

            // when
            BitSet removed = new BitSet();
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < numberOfNodes - numberOfNodes / 10; i++ )
                {
                    int candidate;
                    do
                    {
                        candidate = random.nextInt( max( 1, random.nextInt( numberOfNodes ) ) );
                    }
                    while ( removed.get( candidate ) );
                    removed.set( candidate );

                    writer.remove( key( candidate ) );
                }
            }

            int next = 0;
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < numberOfNodes / 10; i++ )
                {
                    next = removed.nextClearBit( next );
                    removed.set( next );
                    writer.remove( key( next ) );
                }
            }

            // then
            try ( RawCursor<Hit<KEY,VALUE>,IOException> seek = index.seek( key( 0 ), key( numberOfNodes ) ) )
            {
                assertFalse( seek.next() );
            }

            // and finally
            index.consistencyCheck();
        }
    }

    private void randomlyModifyIndex( GBPTree<KEY,VALUE> index, GBPTreeData<KEY,VALUE> data, Random random, double removeProbability )
            throws IOException
    {
        int changeCount = random.nextInt( 10 ) + 10;
        try ( Writer<KEY,VALUE> writer = index.writer() )
        {
            for ( int i = 0; i < changeCount; i++ )
            {
                if ( random.nextDouble() < removeProbability && data.size() > 0 )
                {   // remove
                    KEY key = data.getRandomKey( random );
                    data.remove( key, writer );
                }
                else
                {   // put
                    KEY key = randomKey( random );
                    VALUE value = randomValue( random );
                    data.put( key, value, writer );
                }
            }
        }
    }

    private KEY randomKey( Random random )
    {
        return key( random.nextInt( 1_000 ) );
    }

    private VALUE randomValue( Random random )
    {
        return value( random.nextInt( 1_000 ) );
    }

    private VALUE value( long seed )
    {
        return layout.value( seed );
    }

    private KEY key( long seed )
    {
        return layout.key( seed );
    }

    // KEEP even if unused
    @SuppressWarnings( "unused" )
    private void printTree() throws IOException
    {
        index.printTree( false, false, false, false );
    }

    @SuppressWarnings( "unused" )
    private void printNode( @SuppressWarnings( "SameParameterValue" ) int id ) throws IOException
    {
        index.printNode( id );
    }
}
