package indi.sasaki.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class File2Join {
	
	public static Map<String, String[]> readAsMap(File dCache) throws IOException{
		BufferedReader reader = null;
	    Map<String, String[]> fileMap = new HashMap<String, String[]>();
		try {
	    	reader = new BufferedReader(new FileReader(dCache));
	        String currentLine = null;
	        while ((currentLine = reader.readLine()) != null) {
	        	String[] lineFields = currentLine.split(",");
	        	String keyCp = lineFields[3] + "-" + lineFields[4];
	        	String[] valueArray = {lineFields[7], lineFields[8]};
	        	fileMap.put(keyCp, valueArray);
	        }
	    } finally {
	    	try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	    }
		return fileMap;
	}
}
