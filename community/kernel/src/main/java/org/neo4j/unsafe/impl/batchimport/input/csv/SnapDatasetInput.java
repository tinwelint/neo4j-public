package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputGroupsDeserializer.DeserializerFactory;

import static java.nio.charset.Charset.defaultCharset;

import static org.neo4j.csv.reader.Readables.files;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;

public class SnapDatasetInput implements Input
{
    private final IdType idType = IdType.INTEGER;
    private final File theInputFile;
    private final Configuration config = Configuration.TABS;
    private final int maxProcessors;
    private final Extractors extractors = new Extractors( config.arrayDelimiter() );
    private final Groups groups = new Groups();
    private final Collector collector = new NoCollector();
    private final Header.Factory relationshipheader = new Header.Factory()
    {
        @Override
        public Header create( CharSeeker dataSeeker, Configuration configuration, IdType idType )
        {
            return new Header(
                    // Start node
                    new Header.Entry( null, Type.START_ID, null, extractors.long_() ),
                    // End node
                    new Header.Entry( null, Type.END_ID, null, extractors.long_() ) );
        }
    };
    private final String relationshipType;

    public SnapDatasetInput( File theInputFile, int maxProcessors, String relationshipType )
    {
        this.theInputFile = theInputFile;
        this.maxProcessors = maxProcessors;
        this.relationshipType = relationshipType;
    }

    @Override
    public InputIterable<InputNode> nodes()
    {
        return new InputIterable<InputNode>()
        {
            @Override
            public InputIterator<InputNode> iterator()
            {
//                return new InputIterator.Adapter<InputNode>()
//                {
//                    // This is given that relationships are sorted by start node
//                    private final InputIterator<InputRelationship> relationships = relationships().iterator();
//                    private long currentNodeId;
//                    private long highestNodeId;
//
//                    @Override
//                    protected InputNode fetchNextOrNull()
//                    {
//                        if ( currentNodeId <= highestNodeId )
//                        {
//                            return node( currentNodeId++ );
//                        }
//
//                        while ( relationships.hasNext() && currentNodeId >= highestNodeId )
//                        {
//                            InputRelationship nextRelationship = relationships.next();
//                            highestNodeId = max( highestNodeId,
//                                    max( (Long) nextRelationship.startNode(), (Long) nextRelationship.endNode() ) );
//                        }
//                        return currentNodeId <= highestNodeId ? node( currentNodeId++ ) : null;
//                    }
//
//                    private InputNode node( long nodeId )
//                    {
//                        return new InputNode( "", relationships.lineNumber(), relationships.position(),
//                                nodeId, NO_PROPERTIES, null, NO_LABELS, null );
//                    }
//
//                    @Override
//                    public int processors( int delta )
//                    {
//                        return relationships.processors( delta );
//                    }
//
//                    @Override
//                    public String sourceDescription()
//                    {
//                        return relationships.sourceDescription();
//                    }
//
//                    @Override
//                    public long lineNumber()
//                    {
//                        return relationships.lineNumber();
//                    }
//
//                    @Override
//                    public long position()
//                    {
//                        return relationships.position();
//                    }
//                };

                return new SimpleIdRangeInputNodeIterator( 0, 125_000_000 );
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    @Override
    public InputIterable<InputRelationship> relationships()
    {
        return new InputIterable<InputRelationship>()
        {
            @Override
            public InputIterator<InputRelationship> iterator()
            {
                DeserializerFactory<InputRelationship> factory = (dataHeader, dataStream, decorator, validator) ->
                        new InputEntityDeserializer<>( dataHeader, dataStream, config.delimiter(),
                                new InputRelationshipDeserialization( dataHeader, dataStream, groups ),
                                decorator, validator, collector );
                Data<InputRelationship> data = data( defaultRelationshipType( relationshipType ),
                        fileWithFirstCommentLinesRemoved( theInputFile ) ).create( config );
                InputIterator<InputRelationship> bla = new ParallelInputEntityDeserializer<>( data, relationshipheader,
                        config, idType, maxProcessors, factory, new InputRelationshipValidator(),
                        InputRelationship.class  );
                return new InputIterator.Delegate<InputRelationship>( bla )
                {
                    int count = 0;

                    @Override
                    protected InputRelationship fetchNextOrNull()
                    {
                        InputRelationship a = super.fetchNextOrNull();
//                        System.out.println( a );
                        if ( a != null )
                        {
                            count++;
                        }
                        return a;
                    }

                    @Override
                    public void close()
                    {
                        System.out.println( count );
                        super.close();
                    }
                };
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    protected Supplier<CharReadable> fileWithFirstCommentLinesRemoved( File file )
    {
        return () ->
        {
            try
            {
                int commentLength;
                char[] chunk = new char[100_000];
                try ( CharReadable readable = files( defaultCharset(), file ) )
                {
                    readable.read( chunk, 0, chunk.length );
                    commentLength = charactersWithComments( chunk );
                }

                CharReadable readable = files( defaultCharset(), file );
                readable.read( chunk, 0, commentLength );
                // The readable is now primed for reading
                return readable;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private int charactersWithComments( char[] chunk )
    {
        // TODO: check actual read length as well
        int i = 0;
        while ( chunk[i] == '#' )
        {
            // Read until line break
            char ch;
            while ( (ch = chunk[++i]) != '\n' && ch != '\r' );
            while ( (ch = chunk[++i]) == '\n' || ch == '\r' );
        }
        return i;
    }

    @Override
    public IdMapper idMapper()
    {
        return idType.idMapper();
    }

    @Override
    public IdGenerator idGenerator()
    {
        return idType.idGenerator();
    }

    @Override
    public Collector badCollector()
    {
        return collector;
    }

    private final class NoCollector implements Collector
    {
        @Override
        public PrimitiveLongIterator leftOverDuplicateNodesIds()
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        @Override
        public void collectExtraColumns( String source, long row, String value )
        {
            System.out.println( "collectExtraColumns" );
        }

        @Override
        public void collectDuplicateNode( Object id, long actualId, String group, String firstSource, String otherSource )
        {
            System.out.println( "collectDuplicateNode" );
        }

        @Override
        public void collectBadRelationship( InputRelationship relationship, Object specificValue )
        {
            System.out.println( "collectBadRelationship" );
        }

        @Override
        public void close()
        {
        }

        @Override
        public int badEntries()
        {
            return 0;
        }
    }
}
