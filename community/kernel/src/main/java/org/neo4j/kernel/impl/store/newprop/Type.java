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
package org.neo4j.kernel.impl.store.newprop;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.BooleanArray;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.CharArray;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.UTF8StringValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;

import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.charArray;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatArray;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortArray;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.Values.stringArray;

enum Type
{
    TRUE( true, 0 )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        { // No need, the type is the value
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            return booleanValue( true );
        }
    },
    FALSE( true, 0 )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        { // No need, the type is the value
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            return booleanValue( false );
        }
    },
    INT8( true, Byte.BYTES )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            cursor.putByte( (byte) ((IntegralValue)value).longValue() );
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            return byteValue( cursor.getByte() );
        }
    },
    INT16( true, Short.BYTES )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            cursor.putShort( (short) ((IntegralValue)value).longValue() );
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            return shortValue( cursor.getShort() );
        }
    },
    // INT24?
    INT32( true, Integer.BYTES )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            cursor.putInt( (int) ((IntegralValue)value).longValue() );
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            return intValue( cursor.getInt() );
        }
    },
    // INT40?
    // INT48?
    // INT56?
    INT64( true, Long.BYTES )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            cursor.putLong( ((IntegralValue)value).longValue() );
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            return longValue( cursor.getLong() );
        }
    },
    FLOAT( true, Float.BYTES )
    {
        @Override
        public void putValue( PageCursor cursor, Object preparedValue, int valueLength )
        {
            putValue( cursor, ((FloatValue)preparedValue).value() );
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            return floatValue( Float.intBitsToFloat( cursor.getInt() ) );
        }

        void putValue( PageCursor cursor, float value )
        {
            cursor.putInt( Float.floatToIntBits( value ) );
        }
    },
    DOUBLE( true, Double.BYTES )
    {
        @Override
        public void putValue( PageCursor cursor, Object preparedValue, int valueLength )
        {
            cursor.putLong( Double.doubleToLongBits( ((DoubleValue)preparedValue).value() ) );
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            return doubleValue( Double.longBitsToDouble( cursor.getLong() ) );
        }
    },
    UTF8_STRING( false, -1 )
    {
        // TODO introduce a way to stream the value
        @Override
        public Object prepare( Value value )
        {
            if ( value instanceof UTF8StringValue )
            {
                return ((UTF8StringValue)value).bytes();
            }
            return UTF8.encode( ((TextValue)value).stringValue() );
        }

        @Override
        public int valueLength( Object preparedValue )
        {
            return ((byte[])preparedValue).length;
        }

        @Override
        public void putValue( PageCursor cursor, Object preparedValue, int valueLength )
        {
            // TODO this implementation is only suitable for the first iteration of this property store where
            // there's a limit that all property data for an entity must fit in one page, one page being 8192 - 64 max size
            // Therefore this format doesn't add any sort of chunking or value header saying how many bytes are in this
            // page and potentially a link to next page or something like that.

            cursor.putBytes( (byte[]) preparedValue );
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            byte[] bytes = new byte[valueLength];
            cursor.getBytes( bytes );
            return Values.utf8Value( bytes );
        }
    },
    // FORMAT:
    // - 1 byte to say how many values are used in the last byte
    // - 1 bit per boolean
    BOOLEAN_ARRAY( false, -1 )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            BooleanArray array = (BooleanArray) value;
            int length = array.length();
            byte rest = (byte) (length % Byte.SIZE);
            cursor.putByte( rest == 0 ? 8 : rest );
            for ( int offset = 0; offset < length; )
            {
                byte currentByte = 0;
                for ( int bit = 0; bit < Byte.SIZE && offset < length; bit++, offset++ )
                {
                    if ( array.booleanValue( offset ) )
                    {
                        currentByte |= 1 << bit;
                    }
                }
                cursor.putByte( currentByte );
            }
        }

        @Override
        public boolean getValueStructure( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            byte valuesInLastByte = cursor.getByte();
            structure.integralValue( (valueLength - 2 /*the first header byte we just read*/) * Byte.SIZE + valuesInLastByte );
            return true;
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            int length = toIntExact( structure.integralValue() );
            boolean[] array = new boolean[length];
            for ( int offset = 0; offset < length; )
            {
                byte currentByte = cursor.getByte();
                for ( int bit = 0; bit < Byte.SIZE && offset < length; bit++, offset++ )
                {
                    if ( (currentByte & (1 << bit)) != 0 )
                    {
                        array[offset] = true;
                    }
                }
            }
            return booleanArray( array );
        }

        @Override
        public int valueLength( Object value )
        {
            BooleanArray array = (BooleanArray) value;
            return 1 +                                   // the byte saying how many bits are used in the last byte
                   (array.length() - 1) / Byte.SIZE + 1; // all the boolean bits
        }
    },
    BYTE_ARRAY( false, -1 )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            ByteArray array = (ByteArray) value;
            int length = toIntExact( array.length() );
            for ( int offset = 0; offset < length; offset++ )
            {
                cursor.putByte( (byte) array.longValue( offset ) );
            }
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            byte[] array = new byte[valueLength];
            cursor.getBytes( array );
            return byteArray( array );
        }

        @Override
        public int valueLength( Object value )
        {
            return toIntExact( ((ByteArray) value).length() );
        }
    },
    // TODO Perhaps use some famous number array compression library out there instead?
    SHORT_ARRAY( false, -1 )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            putIntegralArray( cursor, (IntegralArray) value, INT16.ordinal() );
        }

        @Override
        public boolean getValueStructure( PageCursor cursor, int valueLength, ValueStructure structure ) throws IOException
        {
            structure.integralValue( cursor.getByte() );
            return true;
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            int minimalType = toIntExact( structure.integralValue() );
            int length = integralArrayLength( valueLength, minimalType );
            short[] array = new short[length];
            for ( int offset = 0; offset < length; offset++ )
            {
                array[offset] = (short) getMinimalIntegralValue( cursor, minimalType );
            }
            return shortArray( array );
        }

        @Override
        public int valueLength( Object value )
        {
            return integralArrayValueLength( (IntegralArray) value, INT16.ordinal() );
        }
    },
    INT_ARRAY( false, -1 )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            putIntegralArray( cursor, (IntegralArray) value, INT32.ordinal() );
        }

        @Override
        public boolean getValueStructure( PageCursor cursor, int valueLength, ValueStructure structure ) throws IOException
        {
            structure.integralValue( cursor.getByte() );
            return true;
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            int minimalType = toIntExact( structure.integralValue() );
            int length = integralArrayLength( valueLength, minimalType );
            int[] array = new int[length];
            for ( int offset = 0; offset < length; offset++ )
            {
                array[offset] = (int) getMinimalIntegralValue( cursor, minimalType );
            }
            return intArray( array );
        }

        @Override
        public int valueLength( Object value )
        {
            return integralArrayValueLength( (IntegralArray) value, INT32.ordinal() );
        }
    },
    LONG_ARRAY( false, -1 )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            putIntegralArray( cursor, (IntegralArray) value, INT64.ordinal() );
        }

        @Override
        public boolean getValueStructure( PageCursor cursor, int valueLength, ValueStructure structure ) throws IOException
        {
            structure.integralValue( cursor.getByte() );
            return true;
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            int minimalType = toIntExact( structure.integralValue() );
            int length = integralArrayLength( valueLength, minimalType );
            long[] array = new long[length];
            for ( int offset = 0; offset < length; offset++ )
            {
                array[offset] = getMinimalIntegralValue( cursor, minimalType );
            }
            return longArray( array );
        }

        @Override
        public int valueLength( Object value )
        {
            return integralArrayValueLength( (IntegralArray) value, INT64.ordinal() );
        }
    },
    FLOAT_ARRAY( false, -1 )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            FloatingPointArray array = (FloatingPointArray) value;
            int length = array.length();
            for ( int offset = 0; offset < length; offset++ )
            {
                cursor.putInt( Float.floatToIntBits( (float) array.doubleValue( offset ) ) );
            }
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            int length = valueLength / Float.BYTES;
            float[] array = new float[length];
            for ( int offset = 0; offset < length; offset++ )
            {
                array[offset] = Float.intBitsToFloat( cursor.getInt() );
            }
            return floatArray( array );
        }

        @Override
        public int valueLength( Object preparedValue )
        {
            return ((FloatingPointArray)preparedValue).length() * Float.BYTES;
        }
    },
    DOUBLE_ARRAY( false, -1 )
    {
        @Override
        public void putValue( PageCursor cursor, Object value, int valueLength )
        {
            FloatingPointArray array = (FloatingPointArray) value;
            int length = array.length();
            for ( int offset = 0; offset < length; offset++ )
            {
                cursor.putLong( Double.doubleToLongBits( array.doubleValue( offset ) ) );
            }
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            int length = valueLength / Double.BYTES;
            double[] array = new double[length];
            for ( int offset = 0; offset < length; offset++ )
            {
                array[offset] = Double.longBitsToDouble( cursor.getLong() );
            }
            return doubleArray( array );
        }

        @Override
        public int valueLength( Object preparedValue )
        {
            return ((FloatingPointArray)preparedValue).length() * Double.BYTES;
        }

    },
    CHAR_ARRAY( false, -1 )
    {
        @Override
        public void putValue( PageCursor cursor, Object preparedValue, int valueLength )
        {
            CharArray array = (CharArray) preparedValue;
            int length = array.length();
            for ( int i = 0; i < length; i++ )
            {
                cursor.putShort( (short) array.charValue( i ) );
            }
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            char[] chars = new char[valueLength / Character.BYTES];
            for ( int i = 0; i < chars.length; i++ )
            {
                chars[i] = (char) cursor.getShort();
            }
            return charArray( chars );
        }

        @Override
        public int valueLength( Object value )
        {
            return ((CharArray)value).length() * Character.BYTES;
        }
    },
    UTF8_STRING_ARRAY( false, -1 )
    {
        @Override
        public void putValue( PageCursor cursor, Object preparedValue, int valueLength )
        {
            byte[][] bytes = (byte[][]) preparedValue;
            putLengthEfficiently( cursor, bytes.length );
            for ( byte[] stringBytes : bytes )
            {
                cursor.putInt( stringBytes.length );
            }
            for ( byte[] stringBytes : bytes )
            {
                cursor.putBytes( stringBytes );
            }
        }

        private void putLengthEfficiently( PageCursor cursor, int length )
        {
            // TODO instead of fixed 4 bytes then have some custom field size, run-length encoding or something
            cursor.putInt( length );
        }

        private int getLengthEfficiently( PageCursor cursor )
        {
            // TODO instead of fixed 4 bytes then have some custom field size, run-length encoding or something
            return cursor.getInt();
        }

        @Override
        public boolean getValueStructure( PageCursor cursor, int valueLength, ValueStructure structure ) throws IOException
        {
            int arrayLength = getLengthEfficiently( cursor );
            if ( cursor.shouldRetry() )
            {
                return false;
            }

            int[] stringLengths = new int[arrayLength];
            for ( int i = 0; i < arrayLength; i++ )
            {
                stringLengths[i] = cursor.getInt();
            }
            structure.value( stringLengths );
            return true;
        }

        @Override
        public Value getValue( PageCursor cursor, int valueLength, ValueStructure structure )
        {
            // TODO if there were a TextArray w/ utf8... that would be juuust great
            int[] stringLengths = (int[]) structure.value();
            int arrayLength = stringLengths.length;
            String[] strings = new String[arrayLength];
            byte[] bytes = null;
            for ( int i = 0; i < arrayLength; i++ )
            {
                int length = stringLengths[i];
                if ( bytes == null || bytes.length < length )
                {
                    bytes = new byte[length];
                }
                cursor.getBytes( bytes, 0, length );
                strings[i] = UTF8.decode( bytes, 0, length );
            }
            return stringArray( strings );
        }

        @Override
        public Object prepare( Value value )
        {
            TextArray array = (TextArray) value;
            int length = array.length();
            byte[][] bytes = new byte[length][];
            for ( int offset = 0; offset < length; offset++ )
            {
                bytes[offset] = UTF8.encode( array.stringValue( offset ) );
            }
            return bytes;
        }

        @Override
        public int valueLength( Object preparedValue )
        {
            // TODO instead of 4 bytes for individual string length here then be able to have custom field size per string
            int length = Integer.BYTES;
            byte[][] bytes = (byte[][]) preparedValue;
            for ( byte[] stringBytes : bytes )
            {
                length += Integer.BYTES + stringBytes.length;
            }
            return length;
        }
    }
    ;
    // ...TODO more types here, like point/geo?

    public static Type[] ALL = values(); // to avoid clone() every time

    private final boolean fixedSize;
    private final int valueLength;

    public static Type fromHeader( long headerEntryUnsignedInt, PageCursor cursor )
    {
        int ordinal = (int) ((headerEntryUnsignedInt & 0xFF000000) >>> 24);

        // If this is a read cursor then communicate exceptions via CursorException instead of actual exception right here
        if ( !cursor.isWriteLocked() )
        {
            if ( ordinal < 0 || ordinal >= ALL.length )
            {
                cursor.setCursorException( "Invalid value type " + ordinal );
                // A bit hacky to just return _a_ type? We're guaranteed to fail in the end and so if we return _something_
                // then the rest of the operation can progress, obviously going willy-nilly and out of bounds and everything,
                // but that's all right.
                return FALSE;
            }
        }
        return ALL[ordinal];
    }

    public Object prepare( Value value )
    {
        return value;
    }

    public abstract void putValue( PageCursor cursor, Object preparedValue, int valueLength );

    /**
     * {@link #getValue(PageCursor, int, ValueStructure)} has a pre-phase of {@link #getValueStructure(PageCursor, int)} where stuff like
     * array lengths and what-not is read. This information affects how other data is read from the {@link PageCursor} and so
     * must be read in a should-retry loop. The result of this call will be passed into {@link #getValue(PageCursor, int, ValueStructure)}.
     *
     * @param cursor {@link PageCursor} to read structure from.
     * @param valueLength as gotten from {@link #valueLength(PageCursor)}
     * @param structure value structure object to fill and then to pass on to {@link #getValue(PageCursor, int, ValueStructure)}.
     * @return {@code true} if the read was consistent, otherwise {@code false} where a full retry will have to be made.
     * @throws IOException on PageCursor read error.
     */
    public boolean getValueStructure( PageCursor cursor, int valueLength, ValueStructure structure ) throws IOException
    {
        return true;
    }

    public abstract Value getValue( PageCursor cursor, int valueLength, ValueStructure structure );

    public int numberOfHeaderEntries()
    {
        return fixedSize ? 1 : 2;
    }

    public static Type fromValue( Value value )
    {
        if ( value == null )
        {
            throw new IllegalArgumentException( "null value" );
        }
        if ( value instanceof BooleanValue )
        {
            return ((BooleanValue)value).booleanValue() ? Type.TRUE : Type.FALSE;
        }
        if ( value instanceof IntegralValue )
        {
            // TODO don't let negative values use bigger INTXX than the type actually is
            long longValue = ((IntegralValue)value).longValue();
            if ( (longValue & ~0x7F) == 0 )
            {
                return INT8;
            }
            if ( (longValue & ~0x7FFF) == 0 )
            {
                return INT16;
            }
            if ( (longValue & ~0x7FFFFFFFL) == 0 )
            {
                return INT32;
            }
            return INT64;
        }
        if ( value instanceof FloatValue )
        {
            return FLOAT;
        }
        if ( value instanceof DoubleValue )
        {
            return DOUBLE;
        }
        if ( value instanceof TextValue )
        {
            // TODO match string with more compressed encodings before falling back to UTF-8
            return UTF8_STRING;
        }
        if ( value instanceof BooleanArray )
        {
            return BOOLEAN_ARRAY;
        }
        if ( value instanceof ByteArray )
        {
            return BYTE_ARRAY;
        }
        if ( value instanceof ShortArray )
        {
            return SHORT_ARRAY;
        }
        if ( value instanceof IntArray )
        {
            return INT_ARRAY;
        }
        if ( value instanceof LongArray )
        {
            return LONG_ARRAY;
        }
        if ( value instanceof FloatArray )
        {
            return FLOAT_ARRAY;
        }
        if ( value instanceof DoubleArray )
        {
            return DOUBLE_ARRAY;
        }
        if ( value instanceof TextArray )
        {
            return UTF8_STRING_ARRAY;
        }
        throw new UnsupportedOperationException( "Unfortunately values like " + value + " which is of type "
                + value.getClass() + " aren't supported a.t.m." );
    }

    private Type( boolean fixedSize, int valueLength )
    {
        this.fixedSize = fixedSize;
        this.valueLength = valueLength;
    }

    public int valueLength( PageCursor cursor )
    {
        return fixedSize ? valueLength : cursor.getInt();
    }

    public int valueLength( Object preparedValue )
    {
        return valueLength;
    }

    public void putHeader( PageCursor cursor, int key, int valueOffset, Object preparedValue )
    {
        int keyAndType = ordinal() << 24 | key;
        cursor.putInt( keyAndType );

        if ( !fixedSize )
        {
            cursor.putInt( valueLength( preparedValue ) );
        }
    }

    public int keyOf( long headerEntryUnsignedInt )
    {
        return (int) (headerEntryUnsignedInt & 0xFFFFFF);
    }

    int minimalIntegralArrayType( IntegralArray array, int maxType )
    {
        int length = array.length();
        int minimalType = 0;
        for ( int offset = 0; offset < length && minimalType < maxType; offset++ )
        {
            long candidate = array.longValue( offset );
            if ( minimalType < INT8.ordinal() && candidate != 0 )
            {
                minimalType = INT8.ordinal();
            }
            if ( minimalType < INT16.ordinal() && (candidate & ~0xFF) != 0 )
            {
                minimalType = INT16.ordinal();
            }
            if ( minimalType < INT32.ordinal() && (candidate & ~0xFFFF) != 0 )
            {
                minimalType = INT32.ordinal();
            }
            if ( minimalType < INT64.ordinal() && (candidate & ~0xFFFFFFFFL) != 0 )
            {
                minimalType = INT64.ordinal();
            }
        }
        return minimalType;
    }

    void putIntegralArray( PageCursor cursor, IntegralArray array, int maxType )
    {
        int length = toIntExact( array.length() );
        int minimalType = minimalIntegralArrayType( array, maxType );
        cursor.putByte( (byte) minimalType );
        for ( int offset = 0; offset < length; offset++ )
        {
            long candidate = array.longValue( offset );
            if ( minimalType == -1 )
            {
                cursor.putByte( (byte) candidate );
            }
            else if ( minimalType == INT8.ordinal() )
            {
                cursor.putByte( (byte) candidate );
            }
            else if ( minimalType == INT16.ordinal() )
            {
                cursor.putShort( (short) candidate );
            }
            else if ( minimalType == INT32.ordinal() )
            {
                cursor.putInt( (int) candidate );
            }
            else if ( minimalType == INT64.ordinal() )
            {
                cursor.putLong( candidate );
            }
        }
    }

    int integralArrayValueLength( IntegralArray array, int maxType )
    {
        int minimalType = minimalIntegralArrayType( array, maxType );
        int itemSize = ALL[minimalType].valueLength( null );

        return 1 + /*header byte saying which minimal type the values are stored in*/
               toIntExact( array.length() ) * itemSize;
    }

    long getMinimalIntegralValue( PageCursor cursor, int minimalType )
    {
        if ( minimalType == 0 )
        {
            return 0;
        }
        else if ( minimalType == INT8.ordinal() )
        {
            return cursor.getByte() & 0xFF;
        }
        else if ( minimalType == INT16.ordinal() )
        {
            return cursor.getShort() & 0xFFFF;
        }
        else if ( minimalType == INT32.ordinal() )
        {
            return cursor.getInt() & 0xFFFFFFFFL;
        }
        else if ( minimalType == INT64.ordinal() )
        {
            return cursor.getLong();
        }
        else
        {
            throw new IllegalArgumentException( String.valueOf( minimalType ) );
        }
    }

    int integralArrayLength( int valueLength, int minimalType )
    {
        int itemSize = ALL[minimalType].valueLength( null );
        return (valueLength - 1) / itemSize;
    }
}
