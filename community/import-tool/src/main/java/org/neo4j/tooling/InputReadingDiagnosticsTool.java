/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.tooling;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.function.Function;

import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.csv.reader.ProcessingSource;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Args.Option;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.Version;
import org.neo4j.unsafe.impl.batchimport.Configuration.Overridden;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.InputIteratorBatcherStep;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.MissingRelationshipDataException;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.Data;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactory;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static java.lang.System.currentTimeMillis;
import static java.nio.charset.Charset.defaultCharset;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.helpers.Strings.TAB;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_NODE_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;

public class InputReadingDiagnosticsTool
{
    enum Options
    {
        NODE_DATA( "nodes", null,
                "[:Label1:Label2] \"<file1>" + MULTI_FILE_DELIMITER + "<file2>" + MULTI_FILE_DELIMITER + "...\"",
                "Node CSV header and data. Multiple files will be logically seen as one big file "
                        + "from the perspective of the importer. "
                        + "The first line must contain the header. "
                        + "Multiple data sources like these can be specified in one import, "
                        + "where each data source has its own header. "
                        + "Note that file groups must be enclosed in quotation marks.",
                        true, true ),
        RELATIONSHIP_DATA( "relationships", null,
                "[:RELATIONSHIP_TYPE] \"<file1>" + MULTI_FILE_DELIMITER + "<file2>" +
                MULTI_FILE_DELIMITER + "...\"",
                "Relationship CSV header and data. Multiple files will be logically seen as one big file "
                        + "from the perspective of the importer. "
                        + "The first line must contain the header. "
                        + "Multiple data sources like these can be specified in one import, "
                        + "where each data source has its own header. "
                        + "Note that file groups must be enclosed in quotation marks.",
                        true, true ),
        DELIMITER( "delimiter", null,
                "<delimiter-character>",
                "Delimiter character, or 'TAB', between values in CSV data. The default option is `" + COMMAS.delimiter() + "`." ),
        ARRAY_DELIMITER( "array-delimiter", null,
                "<array-delimiter-character>",
                "Delimiter character, or 'TAB', between array elements within a value in CSV data. The default option is `" + COMMAS.arrayDelimiter() + "`." ),
        QUOTE( "quote", null,
                "<quotation-character>",
                "Character to treat as quotation character for values in CSV data. "
                        + "The default option is `" + COMMAS.quotationCharacter() + "`. "
                        + "Quotes inside quotes escaped like `\"\"\"Go away\"\", he said.\"` and "
                        + "`\"\\\"Go away\\\", he said.\"` are supported. "
                        + "If you have set \"`'`\" to be used as the quotation character, "
                        + "you could write the previous example like this instead: " + "`'\"Go away\", he said.'`" ),
        MULTILINE_FIELDS( "multiline-fields", org.neo4j.csv.reader.Configuration.DEFAULT.multilineFields(),
                "<true/false>",
                "Whether or not fields from input source can span multiple lines, i.e. contain newline characters." ),
        PROCESSORS( "processors", null,
                "<max processor count>",
                "(advanced) Max number of processors used by the importer. Defaults to the number of "
                        + "available processors reported by the JVM"
                        + availableProcessorsHint()
                        + ". There is a certain amount of minimum threads needed so for that reason there "
                        + "is no lower bound for this value. For optimal performance this value shouldn't be "
                        + "greater than the number of available processors." ),
        TRIM_STRINGS( "trim-strings", org.neo4j.csv.reader.Configuration.DEFAULT.trimStrings(),
                "<true/false>",
                "Whether or not strings should be trimmed for whitespaces."),

        INPUT_ENCODING( "input-encoding", null,
                "<character set>",
                "Character set that input data is encoded in. Provided value must be one out of the available "
                        + "character sets in the JVM, as provided by Charset#availableCharsets(). "
                        + "If no input encoding is provided, the default character set of the JVM will be used.",
                true ),
        IGNORE_EMPTY_STRINGS( "ignore-empty-strings", org.neo4j.csv.reader.Configuration.DEFAULT.emptyQuotedStringsAsNull(),
                "<true/false>",
                "Whether or not empty string fields, i.e. \"\" from input source are ignored, i.e. treated as null." ),
        STACKTRACE( "stacktrace", false,
                "<true/false>",
                "Enable printing of error stack traces." ),
        LEGACY_STYLE_QUOTING( "legacy-style-quoting", Configuration.DEFAULT_LEGACY_STYLE_QUOTING,
                "<true/false>",
                "Whether or not backslash-escaped quote e.g. \\\" is interpreted as inner quote." );

        private final String key;
        private final Object defaultValue;
        private final String usage;
        private final String description;
        private final boolean keyAndUsageGoTogether;
        private final boolean supported;

        Options( String key, Object defaultValue, String usage, String description )
        {
            this( key, defaultValue, usage, description, false, false );
        }

        Options( String key, Object defaultValue, String usage, String description, boolean supported )
        {
            this( key, defaultValue, usage, description, supported, false );
        }

        Options( String key, Object defaultValue, String usage, String description, boolean supported, boolean
                keyAndUsageGoTogether
                )
        {
            this.key = key;
            this.defaultValue = defaultValue;
            this.usage = usage;
            this.description = description;
            this.supported = supported;
            this.keyAndUsageGoTogether = keyAndUsageGoTogether;
        }

        String key()
        {
            return key;
        }

        String argument()
        {
            return "--" + key();
        }

        void printUsage( PrintStream out )
        {
            out.println( argument() + spaceInBetweenArgumentAndUsage() + usage );
            for ( String line : Args.splitLongLine( descriptionWithDefaultValue().replace( "`", "" ), 80 ) )
            {
                out.println( "\t" + line );
            }
        }

        private String spaceInBetweenArgumentAndUsage()
        {
            return keyAndUsageGoTogether ? "" : " ";
        }

        String descriptionWithDefaultValue()
        {
            String result = description;
            if ( defaultValue != null )
            {
                if ( !result.endsWith( "." ) )
                {
                    result += ".";
                }
                result += " Default value: " + defaultValue;
            }
            return result;
        }

        String manPageEntry()
        {
            String filteredDescription = descriptionWithDefaultValue().replace( availableProcessorsHint(), "" );
            String usageString = (usage.length() > 0) ? spaceInBetweenArgumentAndUsage() + usage : "";
            return "*" + argument() + usageString + "*::\n" + filteredDescription + "\n\n";
        }

        String manualEntry()
        {
            return "[[import-tool-option-" + key() + "]]\n" + manPageEntry() + "//^\n\n";
        }

        Object defaultValue()
        {
            return defaultValue;
        }

        private static String availableProcessorsHint()
        {
            return " (in your case " + Runtime.getRuntime().availableProcessors() + ")";
        }

        public boolean isSupportedOption()
        {
            return this.supported;
        }
    }

    /**
     * Delimiter used between files in an input group.
     */
    static final String MULTI_FILE_DELIMITER = ",";

    /**
     * Runs the import tool given the supplied arguments.
     *
     * @param incomingArguments arguments for specifying input and configuration for the import.
     * @throws IOException
     */
    public static void main( String[] incomingArguments ) throws IOException
    {
        main( incomingArguments, false );
    }

    /**
     * Runs the import tool given the supplied arguments.
     *
     * @param incomingArguments arguments for specifying input and configuration for the import.
     * @param defaultSettingsSuitableForTests default configuration geared towards unit/integration
     * test environments, for example lower default buffer sizes.
     * @throws IOException
     */
    public static void main( String[] incomingArguments, boolean defaultSettingsSuitableForTests ) throws IOException
    {
        PrintStream out = System.out;
        PrintStream err = System.err;
        Args args = Args.parse( incomingArguments );

        if ( ArrayUtil.isEmpty( incomingArguments ) || asksForUsage( args ) )
        {
            printUsage( out );
            return;
        }

        Collection<Option<File[]>> nodesFiles, relationshipsFiles;
        Input input = null;
        Charset inputEncoding;
        IdType idType = IdType.ACTUAL;
        Number processors = null;

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        try
        {
            nodesFiles = extractInputFiles( args, Options.NODE_DATA.key(), err );
            relationshipsFiles = extractInputFiles( args, Options.RELATIONSHIP_DATA.key(), err );
            processors = args.getNumber( Options.PROCESSORS.key(), null );
            validateInputFiles( nodesFiles, relationshipsFiles );
            inputEncoding = Charset.forName( args.get( Options.INPUT_ENCODING.key(), defaultCharset().name() ) );
            Number finalProcessors = processors;
            Overridden config = new org.neo4j.unsafe.impl.batchimport.Configuration.Overridden(
                    org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT )
            {
                @Override
                public int batchSize()
                {
                    return 100_000;
                }

                @Override
                public int maxNumberOfProcessors()
                {
                    return finalProcessors != null ? finalProcessors.intValue() : super.maxNumberOfProcessors();
                }
            };
            Iterable<DataFactory<InputNode>> nodeData = nodeData( inputEncoding, nodesFiles );
            Iterable<DataFactory<InputRelationship>> relationshipData = relationshipData( inputEncoding, relationshipsFiles );
            input = new CsvInput( nodeData, defaultFormatNodeFileHeader(),
                    relationshipData, defaultFormatRelationshipFileHeader(),
                    idType, csvConfiguration( args, defaultSettingsSuitableForTests ), Collector.NO_COLLECTOR,
                    config.maxNumberOfProcessors() );
            String types = args.get( "types", "123" );

            if ( types.contains( "1" ) )
            {
                System.out.println( "=== Plowing through chunks only ===" );
                viaChunksOnly( relationshipData, config );
            }

            if ( types.contains( "2" ) )
            {
                System.out.println( "=== Using ticketed processing ===" );
                viaProcessing( input, config );
            }

            if ( types.contains( "3" ) )
            {
                System.out.println( "=== Using staging ===" );
                viaStaging( input );
            }
        }
        catch ( IllegalArgumentException e )
        {
            throw andPrintError( "Input error", e, false, err );
        }
    }

    private static void viaProcessing( Input input, org.neo4j.unsafe.impl.batchimport.Configuration config )
    {
        try ( InputIterator<InputRelationship> iterator = input.relationships().iterator() )
        {
            iterator.hasNext();
            iterator.processors( config.maxNumberOfProcessors() - iterator.processors( 0 ) );
            long millis = currentTimeMillis();
            long count = Iterators.count( iterator );
            millis = currentTimeMillis() - millis;
            System.out.println( duration( millis ) + " " + count );
        }
    }

    private static void viaChunksOnly( Iterable<DataFactory<InputRelationship>> relationshipData,
            org.neo4j.unsafe.impl.batchimport.Configuration config ) throws IOException
    {
        long total = 0;
        long millis = currentTimeMillis();
        for ( DataFactory<InputRelationship> factory : relationshipData )
        {
            Data<InputRelationship> data = factory.create( COMMAS );
            try ( ProcessingSource source = new ProcessingSource( data.stream(), (int) mebiBytes( 4 ),
                    config.maxNumberOfProcessors() ) )
            {
                Chunk chunk;
                while ( (chunk = source.nextChunk()).length() > 0 )
                {
                    total += chunk.length();
                    chunk.close();
                }
            }
        }
        millis = currentTimeMillis() - millis;
        System.out.println( bytes( total ) + " in " + duration( millis ) + ", "
                + bytes( (long) ((total / (millis / 1000D))) ) + "/s" );
    }

    private static void viaStaging( Input input )
    {
        ExecutionSupervisors.superviseDynamicExecution( ExecutionMonitors.defaultVisible(),
                new PloughThroughInputStage( org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT, input ) );
    }

    public static class PloughThroughInputStage extends Stage
    {
        public PloughThroughInputStage( org.neo4j.unsafe.impl.batchimport.Configuration config, Input input )
        {
            super( "GOGO", config );
            add( new InputIteratorBatcherStep<>( control(), config,
                    input.relationships().iterator(), InputRelationship.class, t -> true ) );
            add( new DeadEndStep( control(), config ) );
        }
    }

    public static class DeadEndStep extends ProcessorStep<Object>
    {
        public DeadEndStep( StageControl control, org.neo4j.unsafe.impl.batchimport.Configuration config )
        {
            super( control, "END", config, 0 );
        }

        @Override
        protected void process( Object batch, BatchSender sender ) throws Throwable
        {
        }
    }

    public static Collection<Option<File[]>> extractInputFiles( Args args, String key, PrintStream err )
    {
        return args
                .interpretOptionsWithMetadata( key, Converters.<File[]>optional(),
                        Converters.toFiles( MULTI_FILE_DELIMITER, Converters.regexFiles( true ) ), filesExist(
                                err ),
                        Validators.<File>atLeast( "--" + key, 1 ) );
    }

    private static Validator<File[]> filesExist( PrintStream err )
    {
        return files ->
        {
            for ( File file : files )
            {
                if ( file.getName().startsWith( ":" ) )
                {
                    err.println( "It looks like you're trying to specify default label or relationship type (" +
                                      file.getName() + "). Please put such directly on the key, f.ex. " +
                                      Options.NODE_DATA.argument() + ":MyLabel" );
                }
                Validators.REGEX_FILE_EXISTS.validate( file );
            }
        };
    }

    public static void validateInputFiles( Collection<Option<File[]>> nodesFiles,
            Collection<Option<File[]>> relationshipsFiles )
    {
        if ( nodesFiles.isEmpty() )
        {
            if ( relationshipsFiles.isEmpty() )
            {
                throw new IllegalArgumentException( "No input specified, nothing to import" );
            }
            throw new IllegalArgumentException( "No node input specified, cannot import relationships without nodes" );
        }
    }

    private static String manualReference( ManualPage page, Anchor anchor )
    {
        // Docs are versioned major.minor-suffix, so drop the patch version.
        String[] versionParts = Version.getNeo4jVersion().split("-");
        versionParts[0] = versionParts[0].substring(0, 3);
        String docsVersion = String.join("-", versionParts);

        return " https://neo4j.com/docs/operations-manual/" + docsVersion + "/" +
               page.getReference( anchor );
    }

    /**
     * Method name looks strange, but look at how it's used and you'll see why it's named like that.
     * @param stackTrace whether or not to also print the stack trace of the error.
     * @param err error output
     */
    private static RuntimeException andPrintError( String typeOfError, Exception e, boolean stackTrace,
            PrintStream err )
    {
        // List of common errors that can be explained to the user
        if ( DuplicateInputIdException.class.equals( e.getClass() ) )
        {
            printErrorMessage( "Duplicate input ids that would otherwise clash can be put into separate id space, " +
                               "read more about how to use id spaces in the manual:" +
                               manualReference( ManualPage.IMPORT_TOOL_FORMAT, Anchor.ID_SPACES ), e, stackTrace,
                    err );
        }
        else if ( MissingRelationshipDataException.class.equals( e.getClass() ) )
        {
            printErrorMessage( "Relationship missing mandatory field '" +
                               ((MissingRelationshipDataException) e).getFieldType() + "', read more about " +
                               "relationship format in the manual: " +
                               manualReference( ManualPage.IMPORT_TOOL_FORMAT, Anchor.RELATIONSHIP ), e, stackTrace,
                    err );
        }
        // This type of exception is wrapped since our input code throws InputException consistently,
        // and so IllegalMultilineFieldException comes from the csv component, which has no access to InputException
        // therefore it's wrapped.
        else if ( Exceptions.contains( e, IllegalMultilineFieldException.class ) )
        {
            printErrorMessage( "Detected field which spanned multiple lines for an import where " +
                               Options.MULTILINE_FIELDS.argument() + "=false. If you know that your input data " +
                               "include fields containing new-line characters then import with this option set to " +
                               "true.", e, stackTrace, err );
        }
        else if ( Exceptions.contains( e, InputException.class ) )
        {
            printErrorMessage( "Error in input data", e, stackTrace, err );
        }
        // Fallback to printing generic error and stack trace
        else
        {
            printErrorMessage( typeOfError + ": " + e.getMessage(), e, true, err );
        }
        err.println();

        // Mute the stack trace that the default exception handler would have liked to print.
        // Calling System.exit( 1 ) or similar would be convenient on one hand since we can set
        // a specific exit code. On the other hand It's very inconvenient to have any System.exit
        // call in code that is tested.
        Thread.currentThread().setUncaughtExceptionHandler( ( t, e1 ) ->
        {
            /* Shhhh */
        } );
        return launderedException( e ); // throw in order to have process exit with !0
    }

    private static void printErrorMessage( String string, Exception e, boolean stackTrace, PrintStream err )
    {
        err.println( string );
        err.println( "Caused by:" + e.getMessage() );
        if ( stackTrace )
        {
            e.printStackTrace( err );
        }
    }

    public static Iterable<DataFactory<InputRelationship>>
            relationshipData( final Charset encoding, Collection<Option<File[]>> relationshipsFiles )
    {
        return new IterableWrapper<DataFactory<InputRelationship>,Option<File[]>>( relationshipsFiles )
        {
            @Override
            protected DataFactory<InputRelationship> underlyingObjectToObject( Option<File[]> group )
            {
                return data( defaultRelationshipType( group.metadata() ), encoding, group.value() );
            }
        };
    }

    public static Iterable<DataFactory<InputNode>> nodeData( final Charset encoding,
            Collection<Option<File[]>> nodesFiles )
    {
        return new IterableWrapper<DataFactory<InputNode>,Option<File[]>>( nodesFiles )
        {
            @Override
            protected DataFactory<InputNode> underlyingObjectToObject( Option<File[]> input )
            {
                Decorator<InputNode> decorator = input.metadata() != null
                        ? additiveLabels( input.metadata().split( ":" ) )
                        : NO_NODE_DECORATOR;
                return data( decorator, encoding, input.value() );
            }
        };
    }

    private static void printUsage( PrintStream out )
    {
        out.println( "Neo4j Import Tool" );
        for ( String line : Args.splitLongLine( "neo4j-import is used to create a new Neo4j database "
                                                + "from data in CSV files. "
                                                +
                                                "See the chapter \"Import Tool\" in the Neo4j Manual for details on the CSV file format "
                                                + "- a special kind of header is required.", 80 ) )
        {
            out.println( "\t" + line );
        }
        out.println( "Usage:" );
        for ( Options option : Options.values() )
        {
            option.printUsage( out );
        }

        out.println( "Example:");
        out.print( Strings.joinAsLines(
                TAB + "bin/neo4j-import --into retail.db --id-type string --nodes:Customer customers.csv ",
                TAB + "--nodes products.csv --nodes orders_header.csv,orders1.csv,orders2.csv ",
                TAB + "--relationships:CONTAINS order_details.csv ",
                TAB + "--relationships:ORDERED customer_orders_header.csv,orders1.csv,orders2.csv" ) );
    }

    private static boolean asksForUsage( Args args )
    {
        for ( String orphan : args.orphans() )
        {
            if ( isHelpKey( orphan ) )
            {
                return true;
            }
        }

        for ( Entry<String,String> option : args.asMap().entrySet() )
        {
            if ( isHelpKey( option.getKey() ) )
            {
                return true;
            }
        }
        return false;
    }

    private static boolean isHelpKey( String key )
    {
        return key.equals( "?" ) || key.equals( "help" );
    }

    public static Configuration csvConfiguration( Args args, final boolean defaultSettingsSuitableForTests )
    {
        final Configuration defaultConfiguration = COMMAS;
        final Character specificDelimiter = args.interpretOption( Options.DELIMITER.key(),
                Converters.<Character>optional(), CHARACTER_CONVERTER );
        final Character specificArrayDelimiter = args.interpretOption( Options.ARRAY_DELIMITER.key(),
                Converters.<Character>optional(), CHARACTER_CONVERTER );
        final Character specificQuote = args.interpretOption( Options.QUOTE.key(), Converters.<Character>optional(),
                CHARACTER_CONVERTER );
        final Boolean multiLineFields = args.getBoolean( Options.MULTILINE_FIELDS.key(), null );
        final Boolean emptyStringsAsNull = args.getBoolean( Options.IGNORE_EMPTY_STRINGS.key(), null );
        final Boolean trimStrings = args.getBoolean( Options.TRIM_STRINGS.key(), null);
        final Boolean legacyStyleQuoting = args.getBoolean( Options.LEGACY_STYLE_QUOTING.key(), null );
        return new Configuration.Default()
        {
            @Override
            public char delimiter()
            {
                return specificDelimiter != null
                        ? specificDelimiter.charValue()
                        : defaultConfiguration.delimiter();
            }

            @Override
            public char arrayDelimiter()
            {
                return specificArrayDelimiter != null
                        ? specificArrayDelimiter.charValue()
                        : defaultConfiguration.arrayDelimiter();
            }

            @Override
            public char quotationCharacter()
            {
                return specificQuote != null
                        ? specificQuote.charValue()
                        : defaultConfiguration.quotationCharacter();
            }

            @Override
            public boolean multilineFields()
            {
                return multiLineFields != null
                        ? multiLineFields.booleanValue()
                        : defaultConfiguration.multilineFields();
            }

            @Override
            public boolean emptyQuotedStringsAsNull()
            {
                return emptyStringsAsNull != null
                        ? emptyStringsAsNull.booleanValue()
                        : defaultConfiguration.emptyQuotedStringsAsNull();
            }

            @Override
            public int bufferSize()
            {
                return defaultSettingsSuitableForTests ? 10_000 : super.bufferSize();
            }

            @Override
            public boolean trimStrings()
            {
                return trimStrings != null
                       ? trimStrings.booleanValue()
                       : defaultConfiguration.trimStrings();
            }

            @Override
            public boolean legacyStyleQuoting()
            {
                return legacyStyleQuoting != null
                        ? legacyStyleQuoting.booleanValue()
                        : defaultConfiguration.legacyStyleQuoting();
            }
        };
    }

    private static final Function<String,Character> CHARACTER_CONVERTER = new CharacterConverter();

    private enum ManualPage
    {
        IMPORT_TOOL_FORMAT( "tools/import/file-header-format/" );

        private final String page;

        ManualPage( String page )
        {
            this.page = page;
        }

        public String getReference( Anchor anchor )
        {
            // As long as the the operations manual is single-page we only use the anchor.
            return page + "#" + anchor.anchor;
        }
    }

    private enum Anchor
    {
        ID_SPACES( "import-tool-id-spaces" ),
        RELATIONSHIP( "import-tool-header-format-rels" );

        private final String anchor;

        Anchor( String anchor )
        {
            this.anchor = anchor;
        }
    }
}
