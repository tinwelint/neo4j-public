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

import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;
import static org.neo4j.kernel.impl.cache.SizeOfs.withReference;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Enc128;
import org.neo4j.kernel.impl.cache.SizeOf;

public class RelIdArray implements SizeOf
{
    private static final Enc128 enc128 = new Enc128();
    
    private static final DirectionWrapper[] DIRECTIONS_FOR_OUTGOING =
            new DirectionWrapper[] { DirectionWrapper.OUTGOING, DirectionWrapper.BOTH };
    private static final DirectionWrapper[] DIRECTIONS_FOR_INCOMING =
            new DirectionWrapper[] { DirectionWrapper.INCOMING, DirectionWrapper.BOTH };
    private static final DirectionWrapper[] DIRECTIONS_FOR_BOTH =
            new DirectionWrapper[] { DirectionWrapper.OUTGOING, DirectionWrapper.INCOMING, DirectionWrapper.BOTH };
    
    public static class EmptyRelIdArray extends RelIdArray
    {
        private static final DirectionWrapper[] EMPTY_DIRECTION_ARRAY = new DirectionWrapper[0];
        private final RelIdIterator EMPTY_ITERATOR = new RelIdIteratorImpl( this, EMPTY_DIRECTION_ARRAY )
        {
            @Override
            public boolean hasNext()
            {
                return false;
            }

            @Override
            protected boolean nextBlock()
            {
                return false;
            }
            
            @Override
            public void doAnotherRound()
            {
            }
            
            @Override
            public RelIdIterator updateSource( RelIdArray newSource, DirectionWrapper direction )
            {
                return direction.iterator( newSource );
            }
        };
        
        private EmptyRelIdArray( int type )
        {
            super( type );
        }

        @Override
        public RelIdIterator iterator( final DirectionWrapper direction )
        {
            return EMPTY_ITERATOR;
        }
    };
    
    public static RelIdArray empty( int type )
    {
        return new EmptyRelIdArray( type );
    }
    
    public static final RelIdArray EMPTY = new EmptyRelIdArray( -1 );
    
    private final int type;
    private ByteBuffer outIds;
    private ByteBuffer inIds;
//    private IdBlock lastOutBlock;
//    private IdBlock lastInBlock;
    
    public RelIdArray( int type )
    {
        this.type = type;
    }
    
    @Override
    public int size()
    {
        return withObjectOverhead( 8 /*type (padded)*/ + sizeOfBlockWithReference( outIds ) + sizeOfBlockWithReference( inIds ) ); 
    }
    
    static int sizeOfBlockWithReference( ByteBuffer ids )
    {
        return withReference( ids != null ? withObjectOverhead( ids.capacity() ) : 0 );
    }

    public int getType()
    {
        return type;
    }
    
    protected RelIdArray( RelIdArray from )
    {
        this( from.type );
        this.outIds = from.outIds;
        this.inIds = from.inIds;
    }
    
    protected RelIdArray( int type, ByteBuffer out, ByteBuffer in )
    {
        this( type );
        this.outIds = out;
        this.inIds = in;
    }
    
    /*
     * Adding an id with direction BOTH means that it's a loop
     */
    public void add( long id, DirectionWrapper direction )
    {
        ByteBuffer lastBlock = direction.getLastBlock( this );
        if ( lastBlock == null )
        {
            ByteBuffer newLastBlock = null;
            newLastBlock = ByteBuffer.allocate( 20 );
            direction.setLastBlock( this, newLastBlock );
            lastBlock = newLastBlock;
        }
        add( lastBlock, id );
    }
    
    private void add( ByteBuffer buffer, long id )
    {
        enc128.encode( buffer, id );
    }
    
    private void addAll( ByteBuffer target, ByteBuffer source )
    {
        // TODO source position?
        target.put( source );
    }

    public RelIdArray addAll( RelIdArray source )
    {
        if ( source == null )
        {
            return this;
        }
        
        if ( source.getLastLoopBlock() != null )
        {
            return upgradeIfNeeded( source ).addAll( source );
        }
        
        append( source, DirectionWrapper.OUTGOING );
        append( source, DirectionWrapper.INCOMING );
        append( source, DirectionWrapper.BOTH );
        return this;
    }
    
    protected ByteBuffer getLastLoopBlock()
    {
        return null;
    }
    
    public RelIdArray shrink()
    {
        ByteBuffer shrunkOut = shrink( outIds );
        ByteBuffer shrunkIn = shrink( inIds );
        return shrunkOut == outIds && shrunkIn == inIds ? this : 
                new RelIdArray( type, shrunkOut, shrunkIn );
    }
    
    private ByteBuffer shrink( ByteBuffer buffer )
    {
        if ( buffer.position() == buffer.capacity() )
            return buffer;
        
        return copy( buffer );
    }

    private ByteBuffer copy( ByteBuffer buffer )
    {
        ByteBuffer newBuffer = ByteBuffer.allocate( buffer.position() );
        buffer.position( 0 );
        newBuffer.put( buffer );
        return newBuffer;
    }
    
    protected void setLastLoopBlock( ByteBuffer block )
    {
        throw new UnsupportedOperationException( "Should've upgraded to RelIdArrayWithLoops before this" );
    }
    
    public RelIdArray upgradeIfNeeded( RelIdArray capabilitiesToMatch )
    {
        return capabilitiesToMatch.getLastLoopBlock() != null ? new RelIdArrayWithLoops( this ) : this;
    }
    
    public RelIdArray downgradeIfPossible()
    {
        return this;
    }
    
    protected void append( RelIdArray source, DirectionWrapper direction )
    {
        ByteBuffer toBlock = direction.getLastBlock( this );
        ByteBuffer fromBlock = direction.getLastBlock( source );
        if ( fromBlock != null )
        {
            if ( toBlock == null )
            {
                direction.setLastBlock( this, copy( fromBlock ) );
            }
            else
            {
                addAll( toBlock, fromBlock );
            }
        }
    }
    
    public boolean isEmpty()
    {
        return outIds == null && inIds == null && getLastLoopBlock() == null;
    }
    
    public RelIdIterator iterator( DirectionWrapper direction )
    {
        return direction.iterator( this );
    }
    
    public RelIdArray newSimilarInstance()
    {
        return new RelIdArray( type );
    }
    
    public static enum DirectionWrapper
    {
        OUTGOING( Direction.OUTGOING )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new RelIdIteratorImpl( ids, DIRECTIONS_FOR_OUTGOING );
            }

            @Override
            ByteBuffer getLastBlock( RelIdArray ids )
            {
                return ids.outIds;
            }

            @Override
            void setLastBlock( RelIdArray ids, ByteBuffer buffer )
            {
                ids.outIds = buffer;
            }
        },
        INCOMING( Direction.INCOMING )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new RelIdIteratorImpl( ids, DIRECTIONS_FOR_INCOMING );
            }

            @Override
            ByteBuffer getLastBlock( RelIdArray ids )
            {
                return ids.inIds;
            }

            @Override
            void setLastBlock( RelIdArray ids, ByteBuffer buffer )
            {
                ids.inIds = buffer;
            }
        },
        BOTH( Direction.BOTH )
        {
            @Override
            RelIdIterator iterator( RelIdArray ids )
            {
                return new RelIdIteratorImpl( ids, DIRECTIONS_FOR_BOTH );
            }

            @Override
            ByteBuffer getLastBlock( RelIdArray ids )
            {
                return ids.getLastLoopBlock();
            }

            @Override
            void setLastBlock( RelIdArray ids, ByteBuffer block )
            {
                ids.setLastLoopBlock( block );
            }
        };
        
        private final Direction direction;

        private DirectionWrapper( Direction direction )
        {
            this.direction = direction;
        }
        
        abstract RelIdIterator iterator( RelIdArray ids );
        
        /*
         * Only used during add
         */
        abstract ByteBuffer getLastBlock( RelIdArray ids );
        
        /*
         * Only used during add
         */
        abstract void setLastBlock( RelIdArray ids, ByteBuffer buffer );
        
        public Direction direction()
        {
            return this.direction;
        }
    }
    
    public static DirectionWrapper wrap( Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING: return DirectionWrapper.OUTGOING;
        case INCOMING: return DirectionWrapper.INCOMING;
        case BOTH: return DirectionWrapper.BOTH;
        default: throw new IllegalArgumentException( "" + direction );
        }
    }
    
//    public static abstract class IdBlock implements SizeOf
//    {
//        // First element is the actual length w/o the slack
//        private int[] ids = new int[3];
//        
//        /**
//         * @return a copy of itself. The copy is also shrunk so that there's no
//         * slack in the id array.
//         */
//        IdBlock copy()
//        {
//            IdBlock copy = copyInstance();
//            int length = length();
//            copy.ids = new int[length+1];
//            System.arraycopy( ids, 0, copy.ids, 0, length+1 );
//            return copy;
//        }
//        
//        public int size()
//        {
//            return withObjectOverhead( withReference( withArrayOverhead( 4*ids.length ) ) );
//        }
//        
//        /**
//         * @return a shrunk version of itself. It returns itself if there is
//         * no need to shrink it or a {@link #copy()} if there is slack in the array.
//         */
//        IdBlock shrink()
//        {
//            return length() == ids.length-1 ? this : copy();
//        }
//        
//        /**
//         * Upgrades to a {@link HighIdBlock} if this is a {@link LowIdBlock}.
//         */
//        abstract IdBlock upgradeIfNeeded();
//        
//        int length()
//        {
//            return ids[0];
//        }
//
//        IdBlock getPrev()
//        {
//            return null;
//        }
//        
//        abstract void setPrev( IdBlock prev );
//        
//        protected abstract IdBlock copyInstance();
//
//        // Assume id has same high bits
//        void add( int id )
//        {
//            int length = ensureSpace( 1 );
//            ids[length+1] = id;
//            ids[0] = length+1;
//        }
//        
//        int ensureSpace( int delta )
//        {
//            int length = length();
//            int newLength = length+delta;
//            if ( newLength >= ids.length-1 )
//            {
//                int calculatedLength = ids.length*2;
//                if ( newLength > calculatedLength )
//                {
//                    calculatedLength = newLength*2;
//                }
//                int[] newIds = new int[calculatedLength];
//                System.arraycopy( ids, 0, newIds, 0, length+1 );
//                ids = newIds;
//            }
//            return length;
//        }
//        
//        void addAll( IdBlock block )
//        {
//            int otherBlockLength = block.length();
//            int length = ensureSpace( otherBlockLength+1 );
//            System.arraycopy( block.ids, 1, ids, length+1, otherBlockLength );
//            ids[0] = otherBlockLength+length;
//        }
//        
//        long get( int index )
//        {
//            assert index >= 0 && index < length();
//            return transform( ids[index+1] );
//        }
//        
//        abstract long transform( int id );
//        
//        void set( long id, int index )
//        {
//            // Assume same high bits
//            ids[index+1] = (int) id;
//        }
//        
//        abstract long getHighBits();
//    }
//    
//    private static class LowIdBlock extends IdBlock
//    {
//        @Override
//        void setPrev( IdBlock prev )
//        {
//            throw new UnsupportedOperationException();
//        }
//        
//        @Override
//        IdBlock upgradeIfNeeded()
//        {
//            IdBlock highBlock = new HighIdBlock( 0 );
//            highBlock.ids = ((IdBlock)this).ids;
//            return highBlock;
//        }
//
//        @Override
//        long transform( int id )
//        {
//            return (long)(id&0xFFFFFFFFL);
//        }
//        
//        @Override
//        protected IdBlock copyInstance()
//        {
//            return new LowIdBlock();
//        }
//        
//        @Override
//        long getHighBits()
//        {
//            return 0;
//        }
//    }
//    
//    private static class HighIdBlock extends IdBlock
//    {
//        private final long highBits;
//        private IdBlock prev;
//
//        HighIdBlock( long highBits )
//        {
//            this.highBits = highBits;
//        }
//        
//        public int size()
//        {
//            int size = super.size() + 8 + SizeOfs.REFERENCE_SIZE;
//            if ( prev != null )
//            {
//                size += prev.size();
//            }
//            return size;
//        }
//        
//        @Override
//        IdBlock upgradeIfNeeded()
//        {
//            return this;
//        }
//        
//        @Override
//        IdBlock copy()
//        {
//            IdBlock copy = super.copy();
//            if ( prev != null )
//            {
//                copy.setPrev( prev.copy() );
//            }
//            return copy;
//        }
//
//        @Override
//        IdBlock getPrev()
//        {
//            return prev;
//        }
//
//        @Override
//        void setPrev( IdBlock prev )
//        {
//            this.prev = prev;
//        }
//
//        @Override
//        long transform( int id )
//        {
//            return (((long)(id&0xFFFFFFFFL))|(highBits));
//        }
//        
//        @Override
//        protected IdBlock copyInstance()
//        {
//            return new HighIdBlock( highBits );
//        }
//        
//        @Override
//        long getHighBits()
//        {
//            return highBits;
//        }
//    }
    
    private static class IteratorState
    {
        private ByteBuffer block;
        
        public IteratorState( ByteBuffer block, int relativePosition )
        {
            this.block = block;
            // TODO use relativePosition?
        }
        
        boolean hasNext()
        {
            return block.hasRemaining();
        }
        
        /*
         * Only called if hasNext returns true
         */
        long next()
        {
            return enc128.decode( block );
        }

        public void update( ByteBuffer lastBlock )
        {
            this.block = lastBlock;
        }
    }
    
    public static class RelIdIteratorImpl implements RelIdIterator
    {
        private final DirectionWrapper[] directions;
        private int directionPosition = -1;
        private DirectionWrapper currentDirection;
        private IteratorState currentState;
        private final IteratorState[] states;
        
        private long nextElement;
        private boolean nextElementDetermined;
        private RelIdArray ids;
        
        RelIdIteratorImpl( RelIdArray ids, DirectionWrapper[] directions )
        {
            this.ids = ids;
            this.directions = directions;
            this.states = new IteratorState[directions.length];
            
            // Find the initial block which isn't null. There can be directions
            // which have a null block currently, but could potentially be set
            // after the next getMoreRelationships.
            ByteBuffer block = null;
            while ( block == null && directionPosition+1 < directions.length )
            {
                currentDirection = directions[++directionPosition];
                block = currentDirection.getLastBlock( ids );
            }
            
            if ( block != null )
            {
                currentState = new IteratorState( block, 0 );
                states[directionPosition] = currentState;
            }
        }
        
        @Override
        public int getType()
        {
            return ids.getType();
        }
        
        @Override
        public RelIdArray getIds()
        {
            return ids;
        }
        
        @Override
        public RelIdIterator updateSource( RelIdArray newSource, DirectionWrapper direction )
        {
            if ( ids != newSource || newSource.couldBeNeedingUpdate() )
            {
                ids = newSource;
                
                // Blocks may have gotten upgraded to support a linked list
                // of blocks, so reestablish those references.
                for ( int i = 0; i < states.length; i++ )
                {
                    if ( states[i] != null )
                    {
                        states[i].update( directions[i].getLastBlock( ids ) );
                    }
                }
            }
            return this;
        }
        
        @Override
        public boolean hasNext()
        {
            if ( nextElementDetermined )
            {
                return nextElement != -1;
            }
            
            while ( true )
            {
                if ( currentState != null && currentState.hasNext() )
                {
                    nextElement = currentState.next();
                    nextElementDetermined = true;
                    return true;
                }
                else
                {
                    if ( !nextBlock() )
                    {
                        break;
                    }
                }
            }
            
            // Keep this false since the next call could come after we've loaded
            // some more relationships
            nextElementDetermined = false;
            nextElement = -1;
            return false;
        }

        protected boolean nextBlock()
        {
            // Try next block in the chain
            if ( currentState != null && currentState.nextBlock() )
            {
                return true;
            }
            
            // It's ok to return null here... which will result in hasNext
            // returning false. IntArrayIterator will try to get more relationships
            // and call hasNext again.
            return findNextBlock();
        }
        
        /* (non-Javadoc)
         * @see org.neo4j.kernel.impl.util.RelIdIterator#doAnotherRound()
         */
        @Override
        public void doAnotherRound()
        {
            directionPosition = -1;
            findNextBlock();
        }

        protected boolean findNextBlock()
        {
            while ( directionPosition+1 < directions.length )
            {
                currentDirection = directions[++directionPosition];
                IteratorState nextState = states[directionPosition];
                if ( nextState != null )
                {
                    currentState = nextState;
                    return true;
                }
                IdBlock block = currentDirection.getLastBlock( ids );
                if ( block != null )
                {
                    currentState = new IteratorState( block, 0 );
                    states[directionPosition] = currentState;
                    return true;
                }
            }
            return false;
        }
        
        /* (non-Javadoc)
         * @see org.neo4j.kernel.impl.util.RelIdIterator#next()
         */
        @Override
        public long next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException();
            }
            nextElementDetermined = false;
            return nextElement;
        }
    }
    
    public static RelIdArray from( RelIdArray src, RelIdArray add, Collection<Long> remove )
    {
        if ( remove == null )
        {
            if ( src == null )
            {
                return add.downgradeIfPossible();
            }
            if ( add != null )
            {
                src = src.addAll( add );
                return src.downgradeIfPossible();
            }
            return src;
        }
        else
        {
            if ( src == null && add == null )
            {
                return null;
            }
            RelIdArray newArray = null;
            if ( src != null )
            {
                newArray = src.newSimilarInstance();
                newArray.addAll( src );
                evictExcluded( newArray, remove );
            }
            else
            {
                newArray = add.newSimilarInstance();
            }
            if ( add != null )
            {
                newArray = newArray.upgradeIfNeeded( add );
                for ( RelIdIteratorImpl fromIterator = (RelIdIteratorImpl) add.iterator( DirectionWrapper.BOTH ); fromIterator.hasNext();)
                {
                    long value = fromIterator.next();
                    if ( !remove.contains( value ) )
                    {
                        newArray.add( value, fromIterator.currentDirection );
                    }
                }
            }
            return newArray;
        }
    }

    private static void evictExcluded( RelIdArray ids, Collection<Long> excluded )
    {
        for ( RelIdIteratorImpl iterator = (RelIdIteratorImpl) DirectionWrapper.BOTH.iterator( ids ); iterator.hasNext(); )
        {
            long value = iterator.next();
            if ( excluded.contains( value ) )
            {
                boolean swapSuccessful = false;
                IteratorState state = iterator.currentState;
                IdBlock block = state.block;
                for ( int j = block.length() - 1; j >= state.relativePosition; j--)
                {
                    long backValue = block.get( j );
                    block.ids[0] = block.ids[0]-1;
                    if ( !excluded.contains( backValue) )
                    {
                        block.set( backValue, state.relativePosition-1 );
                        swapSuccessful = true;
                        break;
                    }
                }
                if ( !swapSuccessful ) // all elements from pos in remove
                {
                    block.ids[0] = block.ids[0]-1;
                }
            }
        }
    }

    /**
     * Optimization in the lazy loading of relationships for a node.
     * {@link RelIdIterator#updateSource(RelIdArray)} is only called if
     * this returns true, i.e if a {@link RelIdArray} or {@link IdBlock} might have
     * gotten upgraded to handle f.ex loops or high id ranges so that the
     * {@link RelIdIterator} gets updated accordingly.
     */
    public boolean couldBeNeedingUpdate()
    {
        return (lastOutBlock != null && lastOutBlock.getPrev() != null) ||
                (lastInBlock != null && lastInBlock.getPrev() != null);
    }
}
