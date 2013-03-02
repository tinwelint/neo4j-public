package org.neo4j.kernel.impl.api.index;

import static org.neo4j.helpers.collection.Iterables.flatMap;
import static org.neo4j.helpers.collection.Iterables.map;

import java.util.Iterator;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Pair;

public class LabelUpdateToPropertyUpdate
{

    private final IndexingService.IndexStoreView storeView;
    private final IndexRuleRepository indexRules;

    private Function<NodeLabelUpdate,Iterator<NodePropertyUpdate>> labelUpdateToPropertyUpdate = new Function<NodeLabelUpdate, Iterator<NodePropertyUpdate>>()
    {
        @Override
        public Iterator<NodePropertyUpdate> apply( final NodeLabelUpdate labelUpdate )
        {
            return map( new Function<Pair<Integer, Object>, NodePropertyUpdate>()
            {
                @Override
                public NodePropertyUpdate apply( Pair<Integer, Object> entry )
                {
                    switch ( labelUpdate.getMode() )
                    {
                        case ADD:
                            return new NodePropertyUpdate( labelUpdate.getNodeId(), entry.first(), null, entry.other());
                        case REMOVE:
                            return new NodePropertyUpdate( labelUpdate.getNodeId(), entry.first(), entry.other(), null);
                        default:
                            throw new IllegalStateException( "Unknown update mode: " + labelUpdate.getMode() );
                    }
                }
            }, storeView.getNodeProperties( labelUpdate.getNodeId(), indexRules.getIndexedProperties( labelUpdate
                    .getLabelId() ) ) );
        }
    };

    public LabelUpdateToPropertyUpdate(IndexingService.IndexStoreView storeView, IndexRuleRepository indexRules)
    {
        this.storeView = storeView;
        this.indexRules = indexRules;
    }

    public Iterator<NodePropertyUpdate> apply( Iterator<NodeLabelUpdate> labelUpdates )
    {
        return flatMap( labelUpdateToPropertyUpdate, labelUpdates );
    }
}
