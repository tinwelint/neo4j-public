/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools.dump;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.function.Function;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.impl.storemigration.StoreFile.NODE_STORE;
import static org.neo4j.kernel.impl.storemigration.StoreFile.RELATIONSHIP_STORE;

public class DumpStoreTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Test
    public void dumpStoreShouldPrintBufferWithContent() throws Exception
    {
        // Given
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        DumpStore dumpStore = new DumpStore( out );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        for ( byte i = 0; i < 10; i++ )
        {
            buffer.put( i );
        }
        buffer.flip();

        AbstractBaseRecord record = Mockito.mock( AbstractBaseRecord.class );

        // When
        //when( record.inUse() ).thenReturn( true );
        dumpStore.dumpHex( record, buffer, 2, 4 );

        // Then
        Assert.assertEquals( String.format( "@ 0x00000008: 00 01 02 03  04 05 06 07  08 09%n" ), outStream.toString() );
    }

    @Test
    public void dumpStoreShouldPrintShorterMessageForAllZeroBuffer() throws Exception
    {
        // Given
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        DumpStore dumpStore = new DumpStore( out );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        AbstractBaseRecord record = Mockito.mock( AbstractBaseRecord.class );

        // When
        //when( record.inUse() ).thenReturn( true );
        dumpStore.dumpHex( record, buffer, 2, 4 );

        // Then
        Assert.assertEquals( String.format( ": all zeros @ 0x8 - 0xc%n" ), outStream.toString() );
    }

    @Test
    public void shouldDumpIdsFoundInCCReport() throws Exception
    {
        // GIVEN
        File storeDir = new File( "/dir" );
        fs.get().mkdirs( storeDir );
        GraphDatabaseService db =
                new TestGraphDatabaseFactory().setFileSystem( fs.get() ).newImpermanentDatabase( storeDir );
        long[] nodeIds = new long[5];
        long[] relationshipIds = new long[5];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodeIds.length; i++ )
            {
                Node node = db.createNode();
                nodeIds[i] = node.getId();
            }
            for ( int i = 0; i < relationshipIds.length; i++ )
            {
                Relationship relationship = db.getNodeById( nodeIds[i%nodeIds.length] ).createRelationshipTo(
                        db.getNodeById( nodeIds[(i+1)%nodeIds.length] ), MyRelTypes.TEST );
                relationshipIds[i] = relationship.getId();
            }
            tx.success();
        }
        db.shutdown();

        PageCache pageCache = pageCacheRule.getPageCache( fs.get() );
        Function<File,StoreFactory> createStoreFactory = file -> new StoreFactory( file.getParentFile(),
                Config.defaults(), new DefaultIdGeneratorFactory( fs.get() ), pageCache, fs.get(),
                NullLogProvider.getInstance() );

        // WHEN
        IdTrackingInconsistencies inconsistencies = new IdTrackingInconsistencies();
        inconsistencies.nodeIds.add( nodeIds[0] );
        inconsistencies.nodeIds.add( nodeIds[2] );
        inconsistencies.relationshipIds.add( relationshipIds[0] );
        inconsistencies.relationshipIds.add( relationshipIds[2] );
        ByteArrayOutputStream capturedOutput = new ByteArrayOutputStream();
        PrintStream capture = new PrintStream( capturedOutput );
        DumpStore.dumpFile( capture, createStoreFactory, new File( storeDir,
                NODE_STORE.storeFileName() ).getAbsolutePath(), inconsistencies );
        DumpStore.dumpFile( capture, createStoreFactory, new File( storeDir,
                RELATIONSHIP_STORE.storeFileName() ).getAbsolutePath(), inconsistencies );

        // THEN
        capture.flush();
        String output = capturedOutput.toString();
        assertThat( output, containsString( "Node[" + nodeIds[0] + "," ) );
        assertThat( output, not( containsString( "Node[" + nodeIds[1] + "," ) ) );
        assertThat( output, containsString( "Node[" + nodeIds[2] + "," ) );
        assertThat( output, containsString( "Relationship[" + relationshipIds[0] + "," ) );
        assertThat( output, not( containsString( "Relationship[" + relationshipIds[1] + "," ) ) );
        assertThat( output, containsString( "Relationship[" + relationshipIds[2] + "," ) );
    }
}
