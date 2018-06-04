package org.neo4j.kernel;

public class Constants {
	
	public static class StoreFactory
	{
		public static final String LABELS_PART = ".labels";
	    public static final String NAMES_PART = ".names";
	    public static final String INDEX_PART = ".index";
	    public static final String KEYS_PART = ".keys";
	    public static final String ARRAYS_PART = ".arrays";
	    public static final String STRINGS_PART = ".strings";
	    public static final String NODE_STORE_NAME = ".nodestore.db";
	    public static final String NODE_LABELS_STORE_NAME = NODE_STORE_NAME + LABELS_PART;
	    public static final String PROPERTY_STORE_NAME = ".propertystore.db";
	    public static final String PROPERTY_KEY_TOKEN_STORE_NAME = PROPERTY_STORE_NAME + INDEX_PART;
	    public static final String PROPERTY_KEY_TOKEN_NAMES_STORE_NAME = PROPERTY_STORE_NAME + INDEX_PART + KEYS_PART;
	    public static final String PROPERTY_STRINGS_STORE_NAME = PROPERTY_STORE_NAME + STRINGS_PART;
	    public static final String PROPERTY_ARRAYS_STORE_NAME = PROPERTY_STORE_NAME + ARRAYS_PART;
	    public static final String RELATIONSHIP_STORE_NAME = ".relationshipstore.db";
	    public static final String RELATIONSHIP_TYPE_TOKEN_STORE_NAME = ".relationshiptypestore.db";
	    public static final String RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME = RELATIONSHIP_TYPE_TOKEN_STORE_NAME +
	                                                                          NAMES_PART;
	    public static final String LABEL_TOKEN_STORE_NAME = ".labeltokenstore.db";
	    public static final String LABEL_TOKEN_NAMES_STORE_NAME = LABEL_TOKEN_STORE_NAME + NAMES_PART;
	    public static final String SCHEMA_STORE_NAME = ".schemastore.db";
	    public static final String RELATIONSHIP_GROUP_STORE_NAME = ".relationshipgroupstore.db";
	    public static final String COUNTS_STORE = ".counts.db";
	}
	public static class CountsTracker
	{
		private static final byte[] FORMAT = {'N', 'e', 'o', 'C', 'o', 'u', 'n', 't',
                'S', 't', 'o', 'r', 'e', /**/0, 2, 'V'};
		public static final String LEFT = ".a";
		public static final String RIGHT = ".b";
		public static final String TYPE_DESCRIPTOR = "CountsStore";
	}
	
	public static class MetaDataStore
	{
	    public static final String TYPE_DESCRIPTOR = "NeoStore";
	    // This value means the field has not been refreshed from the store. Normally, this should happen only once
	    public static final long FIELD_NOT_INITIALIZED = Long.MIN_VALUE;
	    /*
	     *  9 longs in header (long + in use), time | random | version | txid | store version | graph next prop | latest
	     *  constraint tx | upgrade time | upgrade id
	     */
	    public static final String DEFAULT_NAME = "neostore";
	}
}
