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
package org.neo4j.helpers;

import java.nio.ByteBuffer;

public class Enc128
{

    public static final int BITMASK = 0b0111_1111;
    public static final int NEXT_BLOCK_FLAG = 0b1000_0000;
    public static final int SHIFT_COUNT = 7;

    /**
     * 
     * @param target
     * @param value
     * @return number of bytes used for the encoded value
     */
    public int encode( ByteBuffer target, long value )
    {
        assert value >= 0 : "Invalid value " + value;
        
        int startPosition = target.position();
        while ( true )
        {
            if ( value <= BITMASK)
            {
                target.put( (byte) value );
                break;
            }
            else
            {
                byte thisByte = (byte) ( NEXT_BLOCK_FLAG | (byte) (value& BITMASK) );
                target.put( thisByte );
                value >>>= SHIFT_COUNT;
            }
        }
        return target.position() - startPosition;
    }
    
    public long decode( ByteBuffer source )
    {
        long result = 0;
        int shiftCount = 0;
        while ( true )
        {
            long thisByte = source.get();
            if ( (thisByte & NEXT_BLOCK_FLAG) == 0 )
            {
                result |= (thisByte << shiftCount);
                return result;
            }
            else
            {
                result |= ((thisByte& BITMASK) << shiftCount);
                shiftCount += SHIFT_COUNT;
            }
        }
    }
}
