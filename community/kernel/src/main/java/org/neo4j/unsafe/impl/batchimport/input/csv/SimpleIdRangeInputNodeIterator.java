package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;

import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_LABELS;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;

public class SimpleIdRangeInputNodeIterator extends InputIterator.Adapter<InputNode>
{
    private long from;
    private final long to;

    public SimpleIdRangeInputNodeIterator( long from, long to )
    {
        this.from = from;
        this.to = to;
    }

    @Override
    protected InputNode fetchNextOrNull()
    {
        if ( from >= to )
        {
            return null;
        }

        return new InputNode( "source", from, from, from++, NO_PROPERTIES, null, NO_LABELS, null );
    }
}
