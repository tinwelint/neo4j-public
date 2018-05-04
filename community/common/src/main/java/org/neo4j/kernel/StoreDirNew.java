package org.neo4j.kernel;

import java.io.File;

public class StoreDirNew {

	File path;
	public StoreDirNew(File path){
		this.path = path;
	}
	
	public File getAsFile()
	{
		return path;
	}

}
