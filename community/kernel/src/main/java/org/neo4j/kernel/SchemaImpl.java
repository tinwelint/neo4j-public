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
package org.neo4j.kernel;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.single;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.IndexNotFoundKernelException;
import org.neo4j.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.KeyHolder;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class SchemaImpl implements Schema
{
    private final ThreadToStatementContextBridge ctxProvider;
    private final KeyHolder<PropertyIndex> propertyKeyManager;

    public SchemaImpl( ThreadToStatementContextBridge ctxProvider, KeyHolder<PropertyIndex> propertyKeyManager )
    {
        this.ctxProvider = ctxProvider;
        this.propertyKeyManager = propertyKeyManager;
    }

    @Override
    public IndexCreator indexCreator( Label label )
    {
        return new IndexCreatorImpl( ctxProvider, propertyKeyManager, label );
    }

    @Override
    public Iterable<IndexDefinition> getIndexes( final Label label )
    {
        StatementContext context = ctxProvider.getCtxForReading();
        try
        {
            Iterable<IndexRule> indexRules = context.getIndexRules( context.getLabelId( label.name() ) );
            return map( new Function<IndexRule, IndexDefinition>()
            {
                @Override
                public IndexDefinition apply( IndexRule rule )
                {
                    return new IndexDefinitionImpl( ctxProvider, label,
                            propertyKeyManager.getKeyByIdOrNull( (int) rule.getPropertyKey() ).getKey() );
                }
            }, indexRules );
        }
        catch ( LabelNotFoundKernelException e )
        {
            return emptyList();
        }
    }

    @Override
    public IndexState getIndexState( IndexDefinition index )
    {
        StatementContext context = ctxProvider.getCtxForReading();
        String propertyKey = single( index.getPropertyKeys() );
        try
        {
            long labelId = context.getLabelId( index.getLabel().name() );
            long propertyKeyId = context.getPropertyKeyId( propertyKey );
            InternalIndexState indexState =
                    context.getIndexState( context.getIndexRule( labelId, propertyKeyId ) );
            switch ( indexState )
            {
                case POPULATING:
                    return IndexState.POPULATING;
                case ONLINE:
                    return IndexState.ONLINE;
                case FAILED:
                    return IndexState.FAILED;
                case NON_EXISTENT:
                    throw new NotFoundException( format( "No index for label %s on property %s",
                            index.getLabel().name(), propertyKey ) );
                default:
                    throw new IllegalArgumentException( String.format( "Illegal index state %s", indexState ) );
            }
        }
        catch ( LabelNotFoundKernelException e )
        {
            throw new NotFoundException( format( "Label %s not found", index.getLabel().name() ) );
        }
        catch ( PropertyKeyNotFoundException e )
        {
            throw new NotFoundException( format( "Property key %s not found", propertyKey ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                    index.getLabel().name(), propertyKey ) );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                    index.getLabel().name(), propertyKey ), e );
        }
    }
}
