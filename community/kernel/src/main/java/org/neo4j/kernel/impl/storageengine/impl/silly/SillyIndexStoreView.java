package org.neo4j.kernel.impl.storageengine.impl.silly;

import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.NodeUpdates;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.values.storable.Value;

public class SillyIndexStoreView implements IndexStoreView
{
    @Override
    public Value getPropertyValue( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        return null;
    }

    @Override
    public void loadProperties( long nodeId, PrimitiveIntSet propertyIds, PropertyLoadSink sink )
    {
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds, IntPredicate propertyKeyIdFilter,
            Visitor<NodeUpdates,FAILURE> propertyUpdateVisitor, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
            boolean forceStoreScan )
    {
        return null;
    }

    @Override
    public NodeUpdates nodeAsUpdates( long nodeId )
    {
        return null;
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( long indexId, DoubleLongRegister output )
    {
        return null;
    }

    @Override
    public DoubleLongRegister indexSample( long indexId, DoubleLongRegister output )
    {
        return null;
    }

    @Override
    public void replaceIndexCounts( long indexId, long uniqueElements, long maxUniqueElements, long indexSize )
    {
    }

    @Override
    public void incrementIndexUpdates( long indexId, long updatesDelta )
    {
    }
}
