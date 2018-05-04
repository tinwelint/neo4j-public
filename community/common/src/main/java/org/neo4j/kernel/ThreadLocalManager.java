package org.neo4j.kernel;

public class ThreadLocalManager {

	public static int numPartitions = 0;
	public static final MyThreadLocal<Integer> graphDBID = new MyThreadLocal<Integer>(0);
	
	public static String processDBID(String uname)
	{
		int databaseIndex = 0;
	    if (uname.contains("."))
	    {
	        	String[] tmp = uname.split("\\.");
	        	uname = tmp[0];
	        	databaseIndex = Integer.parseInt(tmp[1]);        	
	    }
	    if (databaseIndex < numPartitions)
    			ThreadLocalManager.graphDBID.set( databaseIndex );
	    else
    			ThreadLocalManager.graphDBID.set( 0 );
           
        return uname;
	}
}