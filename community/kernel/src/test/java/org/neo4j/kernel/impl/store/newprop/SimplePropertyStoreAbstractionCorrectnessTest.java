package org.neo4j.kernel.impl.store.newprop;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.test.rule.PageCacheRule.config;
import static org.neo4j.values.storable.Values.intValue;

@RunWith( Parameterized.class )
public class SimplePropertyStoreAbstractionCorrectnessTest
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
    private SimplePropertyStoreAbstraction store;

    public SimplePropertyStoreAbstractionCorrectnessTest( Creator creator )
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

    @Test
    public void shouldSetOneProperty() throws Exception
    {
        // GIVEN
        int key = 0;

        // WHEN
        long id = store.set( -1, key, intValue( 10 ) );

        // THEN
        assertTrue( store.has( id, key ) );
        assertTrue( store.getAlthoughNotReally( id, key ) );
    }

    @Test
    public void shouldSetTwoProperties() throws Exception
    {
        // GIVEN
        int key1 = 0, key2 = 1;

        // WHEN
        long id = store.set( -1, key1, intValue( 10 ) );
        id = store.set( id, key2, intValue( 1_000_000_000 ) );

         // THEN
        assertTrue( store.has( id, key1 ) );
        assertTrue( store.getAlthoughNotReally( id, key1 ) );

        assertTrue( store.has( id, key2 ) );
        assertTrue( store.getAlthoughNotReally( id, key2 ) );
    }

    @Test
    public void shouldSetManyProperties() throws Exception
    {
        // WHEN
        long id = -1;
        for ( int i = 0; i < 100; i++ )
        {
            id = store.set( id, i, intValue( i ) );
        }

        // THEN
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( store.has( id, i ) );
        }
    }

    @Test
    public void shouldRemoveProperty() throws Exception
    {
        // GIVEN
        long id = -1;
        for ( int i = 0; i < 10; i++ )
        {
            id = store.set( id, i, intValue( i ) );
        }

        // WHEN
        store.remove( id, 1 );

        // THEN
        for ( int i = 0; i < 10; i++ )
        {
            assertEquals( i != 1, store.has( id, i ) );
        }
    }
}
