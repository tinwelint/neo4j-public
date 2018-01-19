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
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.PageCacheRule.PageCacheConfig;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.neo4j.test.rule.PageCacheRule.config;

public abstract class SimplePropertyStoreAbstractionTestBase
{
    interface Creator
    {
        SimplePropertyStoreAbstraction create( PageCache pageCache, FileSystemAbstraction fs, File dir )
                throws IOException;
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        List<Object[]> data = new ArrayList<>();
        data.add( new Object[] {new Creator()
        {
            @Override
            public SimplePropertyStoreAbstraction create( PageCache pageCache, FileSystemAbstraction fs, File dir )
            {
                return new CurrentFormat( pageCache, fs, dir );
            }

            @Override
            public String toString()
            {
                return "Current";
            }
        }} );
        data.add( new Object[] {new Creator()
        {
            @Override
            public SimplePropertyStoreAbstraction create( PageCache pageCache, FileSystemAbstraction fs, File dir )
                    throws IOException
            {
                return new ProposedFormat( pageCache, dir );
            }

            @Override
            public String toString()
            {
                return "New";
            }
        }} );
        return data;
    }

    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule( pageCacheConfig() );
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( getClass() );
    @Rule
    public final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final RepeatRule repeater = new RepeatRule();

    private final Creator creator;
    protected SimplePropertyStoreAbstraction store;

    public SimplePropertyStoreAbstractionTestBase( Creator creator )
    {
        this.creator = creator;
    }

    protected PageCacheConfig pageCacheConfig()
    {
        return config().withInconsistentReads( false );
    }

    @Before
    public void before() throws IOException
    {
        this.store = creator.create( pageCacheRule.getPageCache( fs ), fs, directory.directory() );
    }

    @After
    public void after() throws IOException
    {
        this.store.close();
    }
}
