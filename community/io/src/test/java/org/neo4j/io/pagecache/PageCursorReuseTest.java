package org.neo4j.io.pagecache;

import org.junit.Rule;
import org.junit.Test;

import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCursor;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.RandomRule.Seed;

import static org.junit.Assert.assertTrue;

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.test.rule.PageCacheRule.config;

public class PageCursorReuseTest
{
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule( config().withInconsistentReads( false ) );
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( getClass() );
    @Rule
    public final RandomRule random = new RandomRule();

    @Seed( 1484559557414L )
    @Test
    public void shouldNotReuseSameWriteCursor() throws Exception
    {
        PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() );
        PagedFile pf = pageCache.map( directory.file( "file" ), 1024, StandardOpenOption.CREATE );

        List<PageCursor> openCursors = new ArrayList<>();
        Map<PageCursor/*base*/,PageCursor/*link*/> links = new HashMap<>();
        for ( int i = 0; i < 1_000; i++ )
        {
            boolean did = false;
            int action = random.nextInt( 10 );
            if ( action < 3 )
            {   // open cursor
                PageCursor cursor = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK );
                openCursors.add( cursor );
                System.out.println( "open " + name( cursor ) );
                did = true;
            }
            else if ( action == 7 )
            {   // open linked cursor
                if ( !openCursors.isEmpty() )
                {
                    PageCursor cursor = openCursors.get( random.nextInt( openCursors.size() ) );
                    PageCursor linkedCursor = cursor.openLinkedCursor( 0 );
                    openCursors.add( linkedCursor );
                    PageCursor prev = links.put( cursor, linkedCursor );
                    System.out.println( "open linked " + name( cursor ) + " --> " + name( linkedCursor ) );
                    if ( prev != null )
                    {
                        updateCloseStateRecursively( openCursors, links, prev );
                    }
                    did = true;
                }
            }
            else
            {   // close cursor
                if ( !openCursors.isEmpty() )
                {
                    PageCursor cursor = openCursors.remove( random.nextInt( openCursors.size() ) );
                    cursor.close();
                    System.out.println( "close " + name( cursor ) );
                    updateCloseStateRecursively( openCursors, links, cursor );
                    did = true;
                }
            }

            if ( did )
            {
                assertAllOpen( openCursors );
                print( openCursors, links );
            }
        }
        pf.close();
    }

    private void print( List<PageCursor> openCursors, Map<PageCursor,PageCursor> links )
    {
        System.out.println( "  === NOW AT" );
        for ( PageCursor cursor : openCursors )
        {
            String indent = "  ";
            System.out.println( indent + name( cursor ) );
            while ( links.containsKey( cursor ) )
            {
                indent += "  ";
                cursor = links.get( cursor );
                System.out.println( indent + name( cursor ) );
            }
        }
    }

    private String name( PageCursor cursor )
    {
        return "" + cursor.hashCode();// + " " + cursor;
    }

    private void updateCloseStateRecursively( List<PageCursor> openCursors, Map<PageCursor,PageCursor> links,
            PageCursor cursor )
    {
        String indent = "";
        while ( links.containsKey( cursor ) )
        {
            indent = indent + "  ";
            cursor = links.remove( cursor );
            assertTrue( name( cursor ) + " refers to closed cursor", openCursors.remove( cursor ) );
            System.out.println( indent + "close linked " + name( cursor ) );
        }
    }

    @Test
    public void shouldReproduceIt() throws Exception
    {
        PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() );
        PagedFile pf = pageCache.map( directory.file( "file" ), 1024, StandardOpenOption.CREATE );

        /*
         *
open 315138752
open linked 315138752 --> 2114874018
open 911312317
open 415186196
open 1337344609
close 315138752
  close linked 2114874018
open linked 911312317 --> 2114874018
open 315138752
open linked 315138752 --> 1113619023
close 2114874018
close 1337344609
open 1337344609
close 415186196
open 415186196
close 911312317

         */

        PageCursor _315138752  = pf.io( 0, PF_SHARED_WRITE_LOCK );
        PageCursor _2114874018 = _315138752.openLinkedCursor( 0 );
        PageCursor _911312317  = pf.io( 0, PF_SHARED_WRITE_LOCK );
        PageCursor _415186196  = pf.io( 0, PF_SHARED_WRITE_LOCK );
        PageCursor _1337344609 = pf.io( 0, PF_SHARED_WRITE_LOCK );
        _315138752.close();
        PageCursor _2114874018_2 = _911312317.openLinkedCursor( 0 );
        PageCursor _315138752_2 = pf.io( 0, PF_SHARED_WRITE_LOCK );
        PageCursor _1113619023 = _315138752_2.openLinkedCursor( 0 );
        _2114874018_2.close();
        _1337344609.close();
        PageCursor _1337344609_2 = pf.io( 0, PF_SHARED_WRITE_LOCK );
        _415186196.close();
        PageCursor _415186196_2 = pf.io( 0, PF_SHARED_WRITE_LOCK );
        _911312317.close();

        pf.close();
    }

    private void assertAllOpen( List<PageCursor> openCursors )
    {
        openCursors.forEach( cursor -> ((MuninnPageCursor)cursor).assertOpen() );
    }
}
