/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;

/**
 * {@link #encode(long, PageCursor) Encoding} and {@link #decode(PageCursor) decoding} of {@code long}
 * references, max 58-bit, into an as compact format as possible. Format is close to how utf-8 does similar encoding.
 *
 * Basically one or more header bits are used to note the number of bytes required to represent a
 * particular {@code long} value followed by the value itself. Number of bytes used for any long ranges from
 * 3 up to the full 8 bytes. The header bits sits in the most significant bit(s) of the most significant byte,
 * so for that the bytes that make up a value is written (and of course read) in big-endian order.
 *
 * Negative values are also supported, in order to handle relative references.
 *
 * @author Mattias Persson
 */
enum Reference
{
    // bit masks below contain one bit for 's' (sign) so actual address space is one bit less than advertised

    // 3-byte, 23-bit addr space: 0sxx xxxx xxxx xxxx xxxx xxxx
    BYTE_3( 3, 1 ),

    // 4-byte, 30-bit addr space: 10sx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
    BYTE_4( 4, 2 ),

    // 5-byte, 37-bit addr space: 110s xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
    BYTE_5( 5, 3 ),

    // 6-byte, 44-bit addr space: 1110 sxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
    BYTE_6( 6, 4 ),

    // 7-byte, 51-bit addr space: 1111 0sxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
    BYTE_7( 7, 5 ),

    // 8-byte, 59-bit addr space: 1111 1sxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
    BYTE_8( 8, 5 );

    // Take one copy here since Enum#values() does an unnecessary defensive copy every time.
    private static final Reference[] ENCODINGS = Reference.values();

    static final int MAX_BITS = 58;

    private final int numberOfBytes;
    private final int headerShift;
    private final long valueOverflowMask;

    Reference( int numberOfBytes, int headerBits )
    {
        this.numberOfBytes = numberOfBytes;
        this.headerShift = Byte.SIZE - headerBits;
        this.valueOverflowMask = ~valueMask( numberOfBytes, headerShift - 1 /*sign bit uses one bit*/ );
    }

    private long valueMask( int numberOfBytes, int headerShift )
    {
        long mask = ( 1L << headerShift ) - 1;
        for ( int i = 0; i < numberOfBytes - 1; i++ )
        {
            mask <<= 8;
            mask |= 0xFF;
        }
        return mask;
    }

    private boolean canEncode( long absoluteReference )
    {
        return (absoluteReference & valueOverflowMask) == 0;
    }

    private int maxBitsSupported()
    {
        return Long.SIZE - Long.numberOfLeadingZeros( ~valueOverflowMask );
    }

    public static void encode( long reference, PageCursor target )
    {
        // checking with < 0 seems to be the fastest way of telling
        boolean positive = reference >= 0;
        long absoluteReference = positive ? reference : ~reference;

        if ( (absoluteReference & ~0x3FFFFF ) == 0 ) // 3
        {
            byte high = (byte) (absoluteReference >>> Short.SIZE);
            target.putByte( (byte) (high | (positive ? 0 : 0x40)) );
            target.putShort( (short) absoluteReference );
        }
        else if ( (absoluteReference & ~0x1FFFFFFF ) == 0 ) // 4
        {
            byte high = (byte) (absoluteReference >>> (Short.SIZE + Byte.SIZE));
            target.putByte( (byte) (high | 0x80 | (positive ? 0 : 0x20)) );
            target.putShort( (short) absoluteReference );
            target.putByte( (byte) (absoluteReference >>> Short.SIZE) );
        }
        else if ( (absoluteReference & ~0xFFFFFFFFFL ) == 0 ) // 5
        {
            byte high = (byte) (absoluteReference >>> Integer.SIZE);
            target.putByte( (byte) (high | 0xC0 | (positive ? 0 : 0x10)) );
            target.putInt( (int) absoluteReference );
        }
        else if ( (absoluteReference & ~0x7FFFFFFFFFFL ) == 0 ) // 6
        {
            byte highByte = (byte) (absoluteReference >>> (Integer.SIZE + Byte.SIZE));
            target.putByte( (byte) (highByte | 0xE0 | (positive ? 0 : 0x8)) );
            target.putInt( (int) absoluteReference );
            target.putByte( (byte) (absoluteReference >>> Integer.SIZE) );
        }
        else if ( (absoluteReference & ~0x3FFFFFFFFFFFFL ) == 0 ) // 7
        {
            byte highByte = (byte) (absoluteReference >>> (Integer.SIZE + Short.SIZE));
            target.putByte( (byte) (highByte | 0xF0 | (positive ? 0 : 0x4)) );
            target.putInt( (int) absoluteReference );
            target.putShort( (short) (absoluteReference >>> Integer.SIZE) );
        }
        else if ( (absoluteReference & ~0x3FFFFFFFFFFFFFFL ) == 0 ) // 8
        {
            byte highByte = (byte) (absoluteReference >>> (Integer.SIZE + Short.SIZE + Byte.SIZE));
            highByte |= 0xF8 | (positive ? 0 : 0x4);
            target.putByte( highByte );
            target.putInt( (int) absoluteReference );
            target.putShort( (short) (absoluteReference >>> Integer.SIZE) );
            target.putByte( (byte) (absoluteReference >>> (Integer.SIZE + Short.SIZE) ) );
        }
        else
        {
            throw unsupportedOperationDueToTooBigReference( reference );
        }
    }

    private static UnsupportedOperationException unsupportedOperationDueToTooBigReference( long reference )
    {
        return new UnsupportedOperationException( format( "Reference %d uses too many bits to be encoded by "
                + "current compression scheme, max %d bits allowed", reference, maxBits() ) );
    }

    public static int length( long reference )
    {
        boolean positive = reference >= 0;
        long absoluteReference = positive ? reference : ~reference;

        for ( Reference encoding : ENCODINGS )
        {
            if ( encoding.canEncode( absoluteReference ) )
            {
                return encoding.numberOfBytes;
            }
        }
        throw unsupportedOperationDueToTooBigReference( reference );
    }

    private static int maxBits()
    {
        int max = 0;
        for ( Reference encoding : ENCODINGS )
        {
            max = Math.max( max, encoding.maxBitsSupported() );
        }
        return max;
    }

    public static long decode( PageCursor source )
    {
        long absoluteReference;
        long unsignedHighByte = (short) (source.getByte() & 0xFF);
        if ( (unsignedHighByte & 0x80) == 0 )
        {
            int lowShort = source.getShort() & 0xFFFF;
            absoluteReference = lowShort | ((unsignedHighByte & 0x3F) << Short.SIZE);
            return (unsignedHighByte & 0x40) == 0 ? absoluteReference : ~absoluteReference;
        }
        else if ( (unsignedHighByte & 0xC0) == 0x80 )
        {
            int lowShort = source.getShort() & 0xFFFF;
            int thirdByte = source.getByte() & 0xFF;
            absoluteReference = lowShort | (thirdByte << Short.SIZE) | ((unsignedHighByte & 0x1F) << (Short.SIZE + Byte.SIZE));
            return (unsignedHighByte & 0x20) == 0 ? absoluteReference : ~absoluteReference;
        }
        else if ( (unsignedHighByte & 0xE0) == 0xC0 )
        {
            long lowInt = source.getInt() & 0xFFFFFFFFL;
            absoluteReference = lowInt | ((unsignedHighByte & 0xF) << Integer.SIZE);
            return (unsignedHighByte & 0x10) == 0 ? absoluteReference : ~absoluteReference;
        }
        else if ( (unsignedHighByte & 0xF0) == 0xE0 )
        {
            long lowInt = source.getInt() & 0xFFFFFFFFL;
            long fifthByte = source.getByte() & 0xFF;
            absoluteReference = lowInt | (fifthByte << Integer.SIZE) | ((unsignedHighByte & 0x7) << (Integer.SIZE + Byte.SIZE));
            return (unsignedHighByte & 0x8) == 0 ? absoluteReference : ~absoluteReference;
        }
        else if ( (unsignedHighByte & 0xF8) == 0xF0 )
        {
            long lowInt = source.getInt() & 0xFFFFFFFFL;
            long theShort = source.getShort() & 0xFFFF;
            absoluteReference = lowInt | (theShort << Integer.SIZE) | ((unsignedHighByte & 0x3) << (Integer.SIZE + Short.SIZE));
            return (unsignedHighByte & 0x4) == 0 ? absoluteReference : ~absoluteReference;
        }
        else if ( (unsignedHighByte & 0xF8) == 0xF8 )
        {
            long lowInt = source.getInt() & 0xFFFFFFFFL;
            long theShort = source.getShort() & 0xFFFF;
            long theByte = source.getByte() & 0xFF;
            absoluteReference = lowInt | (theShort << Integer.SIZE) | (theByte << (Integer.SIZE + Short.SIZE)) | ((unsignedHighByte & 0x3) << (Integer.SIZE + Short.SIZE + Byte.SIZE));
            return (unsignedHighByte & 0x4) == 0 ? absoluteReference : ~absoluteReference;
        }
        else
        {
            throw new IllegalArgumentException( "Unknown encoding read from high byte " + unsignedHighByte );
        }
    }

    /**
     * Convert provided reference to be relative to basisReference
     * @param reference reference that will be converter to relative
     * @param basisReference conversion basis
     * @return reference relative to basisReference
     */
    public static long toRelative( long reference, long basisReference )
    {
        return Math.subtractExact( reference , basisReference );
    }

    /**
     * Convert provided relative to basis reference into absolute
     * @param relativeReference relative reference to convert
     * @param basisReference basis reference
     * @return absolute reference
     */
    public static long toAbsolute( long relativeReference, long basisReference )
    {
        return Math.addExact( relativeReference, basisReference );
    }
}
