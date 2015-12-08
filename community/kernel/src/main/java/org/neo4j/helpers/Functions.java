/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.helpers;

import java.util.Map;

/**
 * @deprecated use {@link org.neo4j.function.Functions} instead.
 */
@Deprecated
public final class Functions
{
    @Deprecated
    public static <From, To> Function<From, To> map( final Map<From, To> map )
    {
        return new Function<From, To>()
        {
            @Override
            public To apply( From from )
            {
                return map.get( from );
            }
        };
    }

    @Deprecated
    public static <From, To> Function<From, To> withDefaults( final org.neo4j.function.Function<From, To> defaults, final org.neo4j.function.Function<From,
            To> f )
    {
        return new Function<From, To>()
        {
            @Override
            public To apply( From from )
            {
                To to = f.apply( from );

                if ( to == null )
                {
                    return defaults.apply( from );
                }
                else
                {
                    return to;
                }
            }
        };
    }

    @Deprecated
    public static <From, To> Function<From, To> nullFunction()
    {
        return new Function<From, To>()
        {
            @Override
            public To apply( From from )
            {
                return null; // Always return null
            }
        };
    }

    @Deprecated
    public static <From, To> Function<From, To> constant( final To value )
    {
        return new Function<From, To>()
        {
            @Override
            public To apply( From from )
            {
                return value;
            }
        };
    }

    @Deprecated
    public static <T> Function<T, T> identity()
    {
        return new Function<T, T>()
        {
            @Override
            public T apply( T from )
            {
                return from;
            }
        };
    }

    @Deprecated
    public static Function<Object, String> TO_STRING = new Function<Object, String>()
    {
        @Override
        public String apply( Object from )
        {
            if (from != null)
            {
                return from.toString();
            }
            else
            {
                return "";
            }
        }
    };

    @Deprecated
    public static <FROM, TO> Function<FROM, TO> cast( final Class<TO> to )
    {
        return new Function<FROM, TO>()
        {
            @Override
            public TO apply( FROM from )
            {
                return to.cast( from );
            }

            @Override
            public String toString()
            {
                return "cast(to=" + to.getName() + ")";
            }
        };
    }
}
