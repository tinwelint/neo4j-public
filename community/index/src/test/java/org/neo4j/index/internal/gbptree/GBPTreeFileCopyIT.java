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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.neo4j.io.pagecache.IOLimiter.unlimited;

public class GBPTreeFileCopyIT
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule( DefaultFileSystemRule::new, getClass() );
    private SimpleLongLayout layout = SimpleLongLayout.longLayout().build();

    @Test
    public void shouldCopyConsistentDetachedIndexThroughCheckpoints() throws IOException
    {
        // given
        File sourceFile = storage.directory().file( "index" );
        File targetFile = storage.directory().file( "copy" );
        PageCache pageCache = storage.pageCache();
        GBPTreeData<MutableLong,MutableLong> data = new GBPTreeData<>( layout );
        GBPTreeData<MutableLong,MutableLong> initialData;
        try ( GBPTree<MutableLong,MutableLong> tree = new GBPTreeBuilder<>( pageCache, sourceFile, layout ).build() )
        {
            insertData( tree, data, 100_000 );
            initialData = data.copy();
            tree.checkpoint( unlimited() );

            // when
            tree.detach();
            try ( StoreChannel sourceChannel = storage.fileSystem().open( sourceFile, OpenMode.READ );
                  StoreChannel targetChannel = storage.fileSystem().open( targetFile, OpenMode.READ_WRITE ) )
            {
                do
                {
                    doSomeModifications( tree, data );
                    tree.checkpoint( unlimited() );
                }
                while ( copySomeData( sourceChannel, targetChannel ) );
            }
            finally
            {
                tree.attach();
            }
        }

        // then
        try ( GBPTree<MutableLong,MutableLong> tree = new GBPTreeBuilder<>( pageCache, targetFile, layout ).build() )
        {
            tree.consistencyCheck();
            initialData.assertEquals( tree, layout.key( Long.MIN_VALUE ), layout.key( Long.MAX_VALUE ) );
        }
    }

    private boolean copySomeData( StoreChannel sourceChannel, StoreChannel targetChannel ) throws IOException
    {
        int bytes = random.nextInt( 10, 10_000 );
        ByteBuffer buffer = ByteBuffer.allocate( bytes );
        int read = sourceChannel.read( buffer );
        if ( read == -1 )
        {
            return false;
        }

        buffer.flip();
        targetChannel.writeAll( buffer );
        return true;
    }

    private void doSomeModifications( GBPTree<MutableLong,MutableLong> tree, GBPTreeData<MutableLong,MutableLong> data ) throws IOException
    {
        if ( random.nextFloat() > 0.1 )
        {
            int count = random.nextInt( 100 );
            try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
            {
                for ( int i = 0; i < count; i++ )
                {
                    if ( !data.isEmpty() && random.nextFloat() < 0.1 )
                    {
                        data.remove( data.getRandomKey( random.random() ), writer );
                    }
                    else
                    {
                        insertRandomKey( writer, data );
                    }
                }
            }
        }
    }

    private void insertRandomKey( Writer<MutableLong,MutableLong> writer, GBPTreeData<MutableLong,MutableLong> data ) throws IOException
    {
        MutableLong key = layout.key( random.nextLong() );
        MutableLong value = layout.value( random.nextLong() );
        data.put( key, value, writer );
    }

    private void insertData( GBPTree<MutableLong,MutableLong> tree, GBPTreeData<MutableLong,MutableLong> data, int count ) throws IOException
    {
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            for ( int i = 0; i < count; i++ )
            {
                insertRandomKey( writer, data );
            }
        }
    }
}
