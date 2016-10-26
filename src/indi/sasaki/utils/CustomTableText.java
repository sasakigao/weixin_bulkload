package indi.sasaki.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sasaki
 * Read the HBase table text into a list, including the columns and families(as well as those joined).
 * File's supposed to use "," as a separator. For example,
 * co1,cf1
 * co2,cf1
 * co3,cf2  
 */
public class CustomTableText {

	public static List<List<String>> read(File file) {
	    BufferedReader reader = null;
	    List<String> columns = new ArrayList<String>();
	    List<String> columnFamilies = new ArrayList<String>();
	    List<List<String>> tableProfile = new ArrayList<List<String>>();
	    try {
	    	reader = new BufferedReader(new FileReader(file));
	        String currentLine = null;
	        while ((currentLine = reader.readLine()) != null) {
	        	String[] lineFields = currentLine.split(","); 
	        	columns.add(lineFields[0]);
	        	columnFamilies.add(lineFields[1]);
	        }
	    } catch (IOException e) {    //IOex is always something bad happened
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	    }
	    tableProfile.add(columns);
	    tableProfile.add(columnFamilies);
	    return tableProfile;
	}

}
