/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asIterator;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.single;

import java.io.File;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.UpdateMode;
import org.neo4j.kernel.impl.api.index.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class PhysicalToLogicalCommandConverterTest
{

    @Test
    public void shouldConvertInlinedAddedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        int value = 12345;
        PropertyRecord before = propertyRecord();
        PropertyRecord after = propertyRecord( property( key, value ) );

        // WHEN
        NodePropertyUpdate update = single( converter.nodeProperties( commands( before, after ) ) );

        // THEN
        assertEquals( UpdateMode.ADDED, update.getUpdateMode() );
    }

    @Test
    public void shouldConvertInlinedChangedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        int valueBefore = 12341, valueAfter = 738;
        PropertyRecord before = propertyRecord( property( key, valueBefore ) );
        PropertyRecord after = propertyRecord( property( key, valueAfter ) );

        // WHEN
        NodePropertyUpdate update = single( converter.nodeProperties( commands( before, after ) ) );

        // THEN
        assertEquals( UpdateMode.CHANGED, update.getUpdateMode() );
    }
    
    @Test
    public void shouldIgnoreInlinedUnchangedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        int value = 12341;
        PropertyRecord before = propertyRecord( property( key, value ) );
        PropertyRecord after = propertyRecord( property( key, value ) );

        // WHEN
        assertEquals( 0, count( converter.nodeProperties( commands( before, after ) ) ) );
    }
    
    @Test
    public void shouldConvertInlinedRemovedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        int value = 12341;
        PropertyRecord before = propertyRecord( property( key, value ) );
        PropertyRecord after = propertyRecord();

        // WHEN
        NodePropertyUpdate update = single( converter.nodeProperties( commands( before, after ) ) );

        // THEN
        assertEquals( UpdateMode.REMOVED, update.getUpdateMode() );
    }

    @Test
    public void shouldConvertDynamicAddedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        PropertyRecord before = propertyRecord();
        PropertyRecord after = propertyRecord( property( key, longString ) );

        // WHEN
        NodePropertyUpdate update = single( converter.nodeProperties( commands( before, after ) ) );

        // THEN
        assertEquals( UpdateMode.ADDED, update.getUpdateMode() );
    }

    @Test
    public void shouldConvertDynamicChangedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        PropertyRecord before = propertyRecord( property( key, longString ) );
        PropertyRecord after = propertyRecord( property( key, longerString ) );

        // WHEN
        NodePropertyUpdate update = single( converter.nodeProperties( commands( before, after ) ) );

        // THEN
        assertEquals( UpdateMode.CHANGED, update.getUpdateMode() );
    }
    
    @Test
    public void shouldConvertDynamicInlinedRemovedProperty() throws Exception
    {
        // GIVEN
        long key = 10;
        PropertyRecord before = propertyRecord( property( key, longString ) );
        PropertyRecord after = propertyRecord();

        // WHEN
        NodePropertyUpdate update = single( converter.nodeProperties( commands(before, after) ) );

        // THEN
        assertEquals( UpdateMode.REMOVED, update.getUpdateMode() );
    }

    @Test
    public void shouldConvertOnlyLabelsAdded() throws Exception
    {
        // Given
        NodeRecord before = nodeRecord();
        NodeRecord after = nodeRecord(1,2,3,4);
        long nodeId = after.getId();

        // When
        Iterator<NodeLabelUpdate> rawUpdates = converter.nodeLabels( commands( before, after ) );

        // Then
        Set<NodeLabelUpdate> updates = asSet(asIterable( rawUpdates ));
        assertThat( updates, hasItems( new NodeLabelUpdate[]{
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, 1 ),
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, 2 ),
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, 3 ),
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, 4 )}) );
        assertThat( updates.size(), is( 4 ) );
    }

    @Test
    public void shouldConvertNewLabelsAddedWhenSomeAlreadyExisted() throws Exception
    {
        // Given
        NodeRecord before = nodeRecord(1);
        NodeRecord after = nodeRecord(1,2,3,4);
        long nodeId = after.getId();

        // When
        Iterator<NodeLabelUpdate> rawUpdates = converter.nodeLabels( commands( before, after ) );

        // Then
        Set<NodeLabelUpdate> updates = asSet(asIterable( rawUpdates ));
        assertThat( updates, hasItems( new NodeLabelUpdate[]{
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, 2 ),
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, 3 ),
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, 4 )}) );
        assertThat( updates.size(), is( 3 ) );
    }

    @Test
    public void shouldConvertAddingAndRemoving() throws Exception
    {
        // Given
        NodeRecord before = nodeRecord(1,6,7);
        NodeRecord after = nodeRecord(1,2,3,4);
        long nodeId = after.getId();

        // When
        Iterator<NodeLabelUpdate> rawUpdates = converter.nodeLabels( commands( before, after ) );

        // Then
        Set<NodeLabelUpdate> updates = asSet(asIterable( rawUpdates ));
        assertThat( updates, hasItems( new NodeLabelUpdate[]{
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, 2 ),
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, 3 ),
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.ADD, 4 ),
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.REMOVE, 6 ),
                new NodeLabelUpdate( nodeId, NodeLabelUpdate.Mode.REMOVE, 7 )}) );
        assertThat( updates.size(), is( 5 ) );
    }

    private NodeRecord nodeRecord( long ... labels )
    {
        NodeRecord before = new NodeRecord(1337, 1, 2);
        NodeLabelRecordLogic labelLogic = new NodeLabelRecordLogic( before, nodeStore );
        for(long label : labels)
            labelLogic.add( label );
        return before;
    }

    private Iterator<Command.NodeCommand> commands( NodeRecord before, NodeRecord after )
    {
        return asIterator( new Command.NodeCommand( nodeStore, before, after ) );
    }

    private Iterator<Command.PropertyCommand> commands( PropertyRecord before, PropertyRecord after )
    {
        return asIterator( new Command.PropertyCommand(null, before, after) );
    }

    private PropertyRecord propertyRecord( PropertyBlock... propertyBlocks )
    {
        PropertyRecord record = new PropertyRecord( 0 );
        if ( propertyBlocks != null )
        {
            record.setInUse( true );
            for ( PropertyBlock propertyBlock : propertyBlocks )
                record.addPropertyBlock( propertyBlock );
        }
        return record;
    }

    private PropertyBlock property( long key, Object value )
    {
        PropertyBlock block = new PropertyBlock();
        propertyStore.encodeValue( block, (int) key, value );
        return block;
    }
    
    private EphemeralFileSystemAbstraction fs;
    private PropertyStore propertyStore;
    private NodeStore nodeStore;
    private final String longString = "my super looooooooooooooooooooooooooooooooooooooong striiiiiiiiiiiiiiiiiiiiiiing";
    private final String longerString = "my super looooooooooooooooooooooooooooooooooooooong striiiiiiiiiiiiiiiiiiiiiiingdd";
    private PhysicalToLogicalCommandConverter converter;
    
    @Before
    public void before() throws Exception
    {
        fs = new EphemeralFileSystemAbstraction();
        StoreFactory storeFactory = new StoreFactory( new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fs, StringLogger.DEV_NULL, new DefaultTxHook() );
        File propStoreFile = new File( "propertystore" );
        File nodeStoreFile = new File( "nodestore" );
        storeFactory.createPropertyStore( propStoreFile );
        storeFactory.createNodeStore( nodeStoreFile );
        propertyStore = storeFactory.newPropertyStore( propStoreFile );
        nodeStore = storeFactory.newNodeStore( nodeStoreFile );
        converter = new PhysicalToLogicalCommandConverter( propertyStore, nodeStore );
    }

    @After
    public void after() throws Exception
    {
        propertyStore.close();
        nodeStore.close();
        fs.shutdown();
    }
}
