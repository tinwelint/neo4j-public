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
package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;

class SillyPropertyCursor implements PropertyCursor
{
    private Iterator<Entry<Integer,PropertyData>> properties;
    private Entry<Integer,PropertyData> current;

    @Override
    public boolean next()
    {
        if ( !properties.hasNext() )
        {
            current = null;
            return false;
        }

        current = properties.next();
        return true;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        properties = null;
    }

    @Override
    public boolean isClosed()
    {
        return properties == null;
    }

    @Override
    public int propertyKey()
    {
        return current.getKey();
    }

    @Override
    public ValueGroup propertyType()
    {
        return current.getValue().value().valueGroup();
    }

    @Override
    public Value propertyValue()
    {
        return current.getValue().value();
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean booleanValue()
    {
        return ((BooleanValue)current.getValue().value()).booleanValue();
    }

    @Override
    public String stringValue()
    {
        return ((TextValue)current.getValue().value()).stringValue();
    }

    @Override
    public long longValue()
    {
        return ((NumberValue)current.getValue().value()).longValue();
    }

    @Override
    public double doubleValue()
    {
        return ((NumberValue)current.getValue().value()).doubleValue();
    }

    @Override
    public boolean valueEqualTo( long value )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueEqualTo( double value )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueEqualTo( String value )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueMatches( Pattern regex )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueGreaterThan( long number )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueGreaterThan( double number )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueLessThan( long number )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueLessThan( double number )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueGreaterThanOrEqualTo( long number )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueGreaterThanOrEqualTo( double number )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueLessThanOrEqualTo( long number )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean valueLessThanOrEqualTo( double number )
    {
        throw new UnsupportedOperationException();
    }

    void init( ConcurrentMap<Integer,PropertyData> properties )
    {
        this.properties = properties.entrySet().iterator();
    }
}
