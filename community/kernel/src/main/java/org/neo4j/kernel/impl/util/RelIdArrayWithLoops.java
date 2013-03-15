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
package org.neo4j.kernel.impl.util;

import java.nio.ByteBuffer;

public class RelIdArrayWithLoops extends RelIdArray
{
    private ByteBuffer lastLoopBlock;
    
    public RelIdArrayWithLoops( int type )
    {
        super( type );
    }
    
    @Override
    public int size()
    {
        return super.size() + sizeOfBlockWithReference( lastLoopBlock );
    }
    
    protected RelIdArrayWithLoops( RelIdArray from )
    {
        super( from );
        lastLoopBlock = from.getLastLoopBlock();
    }
    
    protected RelIdArrayWithLoops( int type, ByteBuffer out, ByteBuffer in, ByteBuffer loop )
    {
        super( type, out, in );
        this.lastLoopBlock = loop;
    }

    @Override
    protected ByteBuffer getLastLoopBlock()
    {
        return this.lastLoopBlock;
    }

    @Override
    protected void setLastLoopBlock( ByteBuffer block )
    {
        this.lastLoopBlock = block;
    }
    
    @Override
    public RelIdArray upgradeIfNeeded( RelIdArray capabilitiesToMatch )
    {
        return this;
    }
    
    @Override
    public RelIdArray downgradeIfPossible()
    {
        return lastLoopBlock == null ? new RelIdArray( this ) : this;
    }
    
    @Override
    public RelIdArray addAll( RelIdArray source )
    {
        if ( source == null )
        {
            return this;
        }
        append( source, DirectionWrapper.OUTGOING );
        append( source, DirectionWrapper.INCOMING );
        append( source, DirectionWrapper.BOTH );
        return this;
    }

    @Override
    public RelIdArray newSimilarInstance()
    {
        return new RelIdArrayWithLoops( getType() );
    }
    
    @Override
    public boolean couldBeNeedingUpdate()
    {
        return true;
    }
    
    @Override
    public RelIdArray shrink()
    {
        ByteBuffer outBuffer = DirectionWrapper.OUTGOING.getLastBlock( this );
        ByteBuffer shrunkOut = shrink( outBuffer );
        ByteBuffer inBuffer = DirectionWrapper.INCOMING.getLastBlock( this );
        ByteBuffer shrunkIn = shrink( inBuffer );
        ByteBuffer shrunkLoop = shrink( lastLoopBlock );
        return shrunkOut == outBuffer && shrunkIn == inBuffer && shrunkLoop == lastLoopBlock ? this : 
                new RelIdArrayWithLoops( getType(), shrunkOut, shrunkIn, shrunkLoop );
    }
}
