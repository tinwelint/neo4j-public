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

import org.junit.Assert;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.neo4j.cursor.RawCursor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Simple utility to easily keep track of data in a tree. This is both a map and a list, where random keys can be selected and also removed.
 */
class GBPTreeData<KEY,VALUE>
{
    private final TestLayout<KEY,VALUE> layout;
    private final Map<KEY,VALUE> data;

    GBPTreeData( TestLayout<KEY,VALUE> layout )
    {
        this( layout, new TreeMap<>( layout ) );
    }

    private GBPTreeData( TestLayout<KEY,VALUE> layout, Map<KEY,VALUE> initialData )
    {
        this.layout = layout;
        this.data = initialData;
    }

    void put( KEY key, VALUE value, Writer<KEY,VALUE> writer ) throws IOException
    {
        writer.put( key, value );
        data.put( key, value );
    }

    VALUE remove( KEY key, Writer<KEY,VALUE> writer ) throws IOException
    {
        VALUE expected = data.remove( key );
        VALUE actual = writer.remove( key );
        assertValueEquals( key, expected, actual );
        return actual;
    }

    KEY getRandomKey( Random random )
    {
        Object[] allKeys = allKeys();
        return (KEY) allKeys[random.nextInt( allKeys.length )];
    }

    void assertEquals( GBPTree<KEY,VALUE> tree, KEY from, KEY to ) throws IOException
    {
        Iterator<Map.Entry<KEY,VALUE>> entries = data.entrySet().iterator();
        try ( RawCursor<Hit<KEY,VALUE>,IOException> seeker = tree.seek( from, to ) )
        {
            while ( entries.hasNext() )
            {
                Map.Entry<KEY,VALUE> expected = entries.next();
                boolean matchesRange = (layout.compare( from, to ) == 0 && layout.compare( expected.getKey(), from ) == 0) ||
                        (layout.compare( expected.getKey(), from ) >= 0 && layout.compare( expected.getKey(), to ) < 0);
                if ( matchesRange )
                {
                    assertTrue( "Expected hit not found in tree " + expected, seeker.next() );
                    Assert.assertEquals( "Unexpected order of result, expected " + expected + ", but got " + seeker.get().key(), 0,
                            layout.compare( expected.getKey(), seeker.get().key() ) );
                    assertValueEquals( expected.getKey(), expected.getValue(), seeker.get().value() );
                }
            }
            assertFalse( seeker.next() );
        }
    }

    private void assertValueEquals( KEY key, VALUE expectedValue, VALUE actualValue )
    {
        Assert.assertEquals( "Unexpected value for key, " + key + ", expected " + expectedValue + " got " + actualValue, 0,
                layout.compareValue( expectedValue, actualValue ) );
    }

    private Object[] allKeys()
    {
        return data.keySet().stream().toArray();
    }

    boolean isEmpty()
    {
        return data.isEmpty();
    }

    int size()
    {
        return data.size();
    }

    GBPTreeData<KEY,VALUE> copy()
    {
        return new GBPTreeData<>( layout, new TreeMap<>( data ) );
    }
}
