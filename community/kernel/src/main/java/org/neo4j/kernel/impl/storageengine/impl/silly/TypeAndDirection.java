package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.function.IntPredicate;

import org.neo4j.storageengine.api.Direction;

class TypeAndDirection
{
    boolean isDirection( Direction direction )
    {
        return false;
    }

    boolean isType( IntPredicate typeIds )
    {
        return false;
    }
}
