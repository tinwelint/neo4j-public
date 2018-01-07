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

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

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

    public final @Rule PageCacheRule pageCacheRule = new PageCacheRule( config().withInconsistentReads( false ) );
    public final @Rule TestDirectory directory = TestDirectory.testDirectory( getClass() );
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final Creator creator;
    protected SimplePropertyStoreAbstraction store;

    public SimplePropertyStoreAbstractionTestBase( Creator creator )
    {
        this.creator = creator;
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
