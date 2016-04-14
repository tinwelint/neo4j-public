/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Encoding and decoding for int..long values, 3..7 bytes. Encoding information lives in a separate
 * {@code long} header so that values are cleanly read and written. The header is an {@code int} and so
 * it can only contain encoding information about a limited number of values.
 *
 * To encode a value into a {@link PageCursor} first the type of encoding needs to be chosen,
 * this is one of the different set of methods, e.g. 2bit, 1bitSigned, a.s.o. Calling the {@code length} method
 * is optional, but calling {@code setHeader} method is required before calling {@code encode} method. Example:
 *
 * <pre>
 * long value = 12_345_678_900L;
 * int header = 0;
 * int placeInHeader = 4; // i.e. [0000,0000][0000,0000][0000,0000][00XX,0000]
 *                                                                    ^^
 * header = _2bitSetHeader( value, -1, header, placeInHeader );
 * _2bitEncode( pageCursor, value, header, placeInHeader );
 * </pre>
 *
 * The encoding is determined in {@code setHeader} call and reused in {@code encode} call.
 */
public class SimpleNumberEncoding
{
    private static final int _3BYTE_LENGTH = 3;
    private static final int _4BYTE_LENGTH = 4;
    private static final int _5BYTE_LENGTH = 5;
    private static final int _7BYTE_LENGTH = 7;

    private static final long _3BYTE_MASK = byteMask( _3BYTE_LENGTH );
    private static final long _4BYTE_MASK = byteMask( _4BYTE_LENGTH );
    private static final long _5BYTE_MASK = byteMask( _5BYTE_LENGTH );
    private static final long _7BYTE_MASK = byteMask( _7BYTE_LENGTH );

    // ┌──────────────────────────────────────────────────────┐
    // │ 2-bit encoding                                       │
    // ├──────────────────────────────────────────────────────┤
    // │ 00: NULL                                             │
    // │ 01: 3 byte value                                     │
    // │ 10: 5 byte value                                     │
    // │ 11: 7 byte value                                     │
    // └──────────────────────────────────────────────────────┘
    public static int _2bitLength( long value, long nullValue )
    {
        if ( value == nullValue )
        {
            return 0;
        }
        else if ( (value & _3BYTE_MASK) == 0 )
        {
            return _3BYTE_LENGTH;
        }
        else if ( (value & _5BYTE_MASK) == 0 )
        {
            return _5BYTE_LENGTH;
        }
        else if ( (value & _7BYTE_MASK) == 0 )
        {
            return _7BYTE_LENGTH;
        }
        throw new IllegalArgumentException( value + " too big" );
    }

    public static long _2bitSetHeader( long value, long nullValue, long header, int headerShift )
    {
        if ( nullValue == value )
        {
            return header;
        }
        else if ( (value & _3BYTE_MASK) == 0 )
        {
            return header | (1 << headerShift);
        }
        else if ( (value & _5BYTE_MASK) == 0 )
        {
            return header | (2 << headerShift);
        }
        else if ( (value & _7BYTE_MASK) == 0 )
        {
            return header | (3 << headerShift);
        }
        throw new IllegalArgumentException( value + " too big" );
    }

    public static void _2bitEncode( PageCursor cursor, long value, long header, int headerShift )
    {
        int encoding = (int)((header >>> headerShift) & 0x3);
        switch ( encoding )
        {
        case 0: return;
        case 1:
            write3ByteValue( cursor, value );
            return;
        case 2:
            write5ByteValue( cursor, value );
            return;
        case 3:
            write7ByteValue( cursor, value );
            return;
        default:
            throw new IllegalArgumentException( "" + encoding );
        }
    }

    public static long _2bitDecode( PageCursor cursor, long nullValue, long header, int headerShift )
    {
        switch ( (int)((header >>> headerShift) & 0x3) )
        {
        case 0:
            return nullValue;
        case 1:
            return read3ByteValue( cursor );
        case 2:
            return read5ByteValue( cursor );
        case 3:
            return read7ByteValue( cursor );
        default:
            throw new IllegalArgumentException();
        }
    }

    // ┌──────────────────────────────────────────────────────┐
    // │ 2-bit signed encoding, i.e. 3 bits used              │
    // ├──────────────────────────────────────────────────────┤
    // │ 000: NULL                                            │
    // │ 100: UNUSED                                          │
    // │ 001: 3 byte positive value                           │
    // │ 010: 5 byte positive value                           │
    // │ 011: 7 byte positive value                           │
    // │ 101: 3 byte negative value                           │
    // │ 110: 5 byte negative value                           │
    // │ 111: 7 byte negative value                           │
    // └──────────────────────────────────────────────────────┘
    public static long _2bitSignedLength( long value, long nullValue )
    {
        throw new UnsupportedOperationException();
    }

    public static long _2bitSignedSetHeader( long value, long nullValue, long header, int headerShift )
    {
        throw new UnsupportedOperationException();
    }

    public static long _2bitSignedEncode( PageCursor cursor, long value, long header, int headerShift )
    {
        throw new UnsupportedOperationException();
    }

    public static long _2bitSignedDecode( PageCursor cursor, long nullValue, long header, int headerShift )
    {
        throw new UnsupportedOperationException();
    }

    // ┌──────────────────────────────────────────────────────┐
    // │ 1-bit signed encoding, i.e. 2 bits used              │
    // ├──────────────────────────────────────────────────────┤
    // │ 00: 3 byte positive value                            │
    // │ 01: 7 byte positive value                            │
    // │ 10: 3 byte negative value                            │
    // │ 11: 7 byte negative value                            │
    // └──────────────────────────────────────────────────────┘
    public static int _1bitSignedLength( long value )
    {
        // checking with >= 0 seems to be the fastest way of telling
        boolean positive = value >= 0;
        long absoluteValue = positive ? value : ~value;

        if ( (absoluteValue & _3BYTE_MASK) == 0 )
        {
            return _3BYTE_LENGTH; // bytes
        }
        else if ( (absoluteValue & _7BYTE_MASK) == 0 )
        {
            return _7BYTE_LENGTH; // bytes
        }
        throw new IllegalArgumentException( value + " too big" );
    }

    public static long _1bitSignedSetHeader( long value, long header, int headerShift )
    {
        // checking with >= 0 seems to be the fastest way of telling
        boolean positive = value >= 0;
        long absoluteValue = positive ? value : ~value;

        if ( (absoluteValue & _3BYTE_MASK) == 0 )
        {
            return header | (positive ? 0 : (2 << headerShift));
        }
        else if ( (absoluteValue & _7BYTE_MASK) == 0 )
        {
            return header | (1 << headerShift) | (positive ? 0 : (2 << headerShift));
        }
        else
        {
            throw new IllegalArgumentException( value + " too big" );
        }
    }

    public static void _1bitSignedEncode( PageCursor cursor, long value, long header, int headerShift )
    {
        if ( value < 0 )
        {
            value = ~value;
        }
        if ( (header & (1 << headerShift)) == 0 )
        {
            write3ByteValue( cursor, value );
        }
        else
        {
            write7ByteValue( cursor, value );
        }
    }

    public static long _1bitSignedDecode( PageCursor cursor, long header, int headerShift )
    {
        long absoluteValue;
        if ( (header & (1 << headerShift)) == 0 )
        {
            absoluteValue = read3ByteValue( cursor );
        }
        else
        {
            absoluteValue = read7ByteValue( cursor );
        }
        return (header & (2 << headerShift)) == 0 ? absoluteValue : ~absoluteValue;
    }

    // ┌──────────────────────────────────────────────────────┐
    // │ 1-bit encoding                                       │
    // ├──────────────────────────────────────────────────────┤
    // │ 0: 4 byte positive value                             │
    // │ 1: 7 byte positive value                             │
    // └──────────────────────────────────────────────────────┘
    public static int _1bitLength( long value )
    {
        if ( (value & _4BYTE_MASK) == 0 )
        {
            return _4BYTE_LENGTH;
        }
        else if ( (value & _7BYTE_MASK) == 0 )
        {
            return _7BYTE_LENGTH;
        }
        throw new IllegalArgumentException( value + " too big" );
    }

    public static long _1bitSetHeader( long value, long header, int headerShift )
    {
        if ( (value & _4BYTE_MASK) == 0 )
        {
            return header;
        }
        else if ( (value & _7BYTE_MASK) == 0 )
        {
            return header | (1 << headerShift);
        }
        throw new IllegalArgumentException( value + " too big" );
    }

    public static void _1bitEncode( PageCursor cursor, long value, long header, int headerShift )
    {
        if ( (header & (1 << headerShift)) == 0 )
        {
            cursor.putInt( (int) value );
        }
        else
        {
            write7ByteValue( cursor, value );
        }
    }

    public static long _1bitDecode( PageCursor cursor, long header, int headerShift )
    {
        return (header & (1 << headerShift)) == 0 ? cursor.getInt() & 0xFFFFFFFFL : read7ByteValue( cursor );
    }

    // ┌──────────────────────────────────────────────────────┐
    // │ READ/WRITE values of various sizes                   │
    // └──────────────────────────────────────────────────────┘
    public static int read3ByteValue( PageCursor cursor )
    {
        int lowShort = cursor.getShort() & 0xFFFF;
        int highByte = cursor.getByte() & 0xFF;
        return lowShort | (highByte << Short.SIZE);
    }

    public static long read5ByteValue( PageCursor cursor )
    {
        long lowInt = cursor.getInt() & 0xFFFFFFFFL;
        long highByte = cursor.getByte() & 0xFF;
        return lowInt | (highByte << Integer.SIZE);
    }

    public static long read7ByteValue( PageCursor cursor )
    {
        long lowInt = cursor.getInt() & 0xFFFFFFFFL;
        long highShort = cursor.getShort() & 0xFFFF;
        long highByte = cursor.getByte() & 0xFF;
        return lowInt | (highShort << Integer.SIZE) | (highByte << (Integer.SIZE + Short.SIZE));
    }

    public static void write3ByteValue( PageCursor cursor, long value )
    {
        cursor.putShort( (short) value );
        cursor.putByte( (byte) (value >>> Short.SIZE) );
    }

    public static void write5ByteValue( PageCursor cursor, long value )
    {
        cursor.putInt( (int) value );
        cursor.putByte( (byte) (value >>> Integer.SIZE) );
    }

    public static void write7ByteValue( PageCursor cursor, long value )
    {
        cursor.putInt( (int) value );
        cursor.putShort( (short) (value >> Integer.SIZE) );
        cursor.putByte( (byte) (value >> (Integer.SIZE + Short.SIZE)) );
    }

    private static long byteMask( int bytes )
    {
        long mask = 0;
        for ( int i = 0; i < bytes; i++ )
        {
            mask = (mask << Byte.SIZE) | 0xFF;
        }
        return ~mask;
    }
}
