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
package org.neo4j.kernel.impl.store;

import org.neo4j.kernel.Constants;
import org.neo4j.helpers.collection.Iterables;


public enum StoreFile
{
    // all store files in Neo4j
    NODE_STORE( Constants.StoreFactory.NODE_STORE_NAME ),

    NODE_LABEL_STORE( Constants.StoreFactory.NODE_LABELS_STORE_NAME ),

    PROPERTY_STORE( Constants.StoreFactory.PROPERTY_STORE_NAME ),

    PROPERTY_ARRAY_STORE( Constants.StoreFactory.PROPERTY_ARRAYS_STORE_NAME ),

    PROPERTY_STRING_STORE( Constants.StoreFactory.PROPERTY_STRINGS_STORE_NAME ),

    PROPERTY_KEY_TOKEN_STORE( Constants.StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME ),

    PROPERTY_KEY_TOKEN_NAMES_STORE( Constants.StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),

    RELATIONSHIP_STORE( Constants.StoreFactory.RELATIONSHIP_STORE_NAME ),

    RELATIONSHIP_GROUP_STORE( Constants.StoreFactory.RELATIONSHIP_GROUP_STORE_NAME ),

    RELATIONSHIP_TYPE_TOKEN_STORE( Constants.StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME ),

    RELATIONSHIP_TYPE_TOKEN_NAMES_STORE( Constants.StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME ),

    LABEL_TOKEN_STORE( Constants.StoreFactory.LABEL_TOKEN_STORE_NAME ),

    LABEL_TOKEN_NAMES_STORE( Constants.StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME ),

    SCHEMA_STORE( Constants.StoreFactory.SCHEMA_STORE_NAME ),

    COUNTS_STORE_LEFT( Constants.StoreFactory.COUNTS_STORE + Constants.CountsTracker.LEFT, false )
            {
                @Override
                public boolean isOptional()
                {
                    return true;
                }
            },
    COUNTS_STORE_RIGHT( Constants.StoreFactory.COUNTS_STORE + Constants.CountsTracker.RIGHT, false )
            {
                @Override
                public boolean isOptional()
                {
                    return true;
                }
            },

    NEO_STORE( "" );

    private final String storeFileNamePart;
    private final boolean recordStore;

    StoreFile( String storeFileNamePart )
    {
        this( storeFileNamePart, true );
    }

    StoreFile( String storeFileNamePart, boolean recordStore )
    {
        this.storeFileNamePart = storeFileNamePart;
        this.recordStore = recordStore;
    }

    public String fileNamePart()
    {
        return storeFileNamePart;
    }

    public boolean isRecordStore()
    {
        return recordStore;
    }

    public static Iterable<StoreFile> currentStoreFiles()
    {
        return Iterables.iterable( values() );
    }

    public boolean isOptional()
    {
        return false;
    }
}

