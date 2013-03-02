package org.neo4j.kernel.impl.api.index;

public class NodeLabelUpdate
{
    public enum Mode
    {
        ADD,
        REMOVE;
    }

    private final long nodeId;
    private final Mode mode;
    private final long labelId;

    public NodeLabelUpdate( long nodeId, Mode mode, long labelId )
    {
        this.nodeId = nodeId;
        this.mode = mode;
        this.labelId = labelId;
    }

    public long getLabelId()
    {
        return labelId;
    }
    public Mode getMode()
    {
        return mode;
    }

    public long getNodeId()
    {
        return nodeId;
    }

    @Override
    public String toString()
    {
        return String.format( "NodeLabelUpdate[%s label %d to %d]", mode, labelId, nodeId );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        NodeLabelUpdate that = (NodeLabelUpdate) o;

        if ( labelId != that.labelId )
        {
            return false;
        }
        if ( nodeId != that.nodeId )
        {
            return false;
        }
        if ( mode != that.mode )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (nodeId ^ (nodeId >>> 32));
        result = 31 * result + mode.hashCode();
        result = 31 * result + (int) (labelId ^ (labelId >>> 32));
        return result;
    }
}
