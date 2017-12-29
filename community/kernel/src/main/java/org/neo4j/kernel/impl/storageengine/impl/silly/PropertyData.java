package org.neo4j.kernel.impl.storageengine.impl.silly;

import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.values.storable.Value;

class PropertyData implements PropertyItem
{
    private final Value value;
    private final int key;

    PropertyData( int key, Value value )
    {
        this.key = key;
        this.value = value;
    }

    @Override
    public Value value()
    {
        return value;
    }

    @Override
    public int propertyKeyId()
    {
        return key;
    }
}
