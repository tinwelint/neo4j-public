package org.neo4j.kernel.api.index;

public class IndexProviderDescriptor
{
    private final String key;
    private final String version;

    public IndexProviderDescriptor( String key, String version )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "null provider key prohibited" );
        }
        if ( key.length() == 0 )
        {
            throw new IllegalArgumentException( "empty provider key prohibited" );
        }
        if ( version == null )
        {
            throw new IllegalArgumentException( "null provider version prohibited" );
        }

        this.key = key;
        this.version = version;
    }

    public String getKey()
    {
        return key;
    }

    public String getVersion()
    {
        return version;
    }

    @Override
    public int hashCode()
    {
        return ( 23 + key.hashCode() ) ^ version.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj != null && obj instanceof IndexProviderDescriptor )
        {
            IndexProviderDescriptor otherDescriptor = (IndexProviderDescriptor) obj;
            return key.equals( otherDescriptor.getKey() ) && version.equals( otherDescriptor.getVersion() );
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "{key=" + key + ", version=" + version + "}";
    }
}
