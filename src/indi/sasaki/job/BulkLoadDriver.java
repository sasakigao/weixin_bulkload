package indi.sasaki.job;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import indi.sasaki.process.HFileCreater;
import indi.sasaki.process.PreSplitTable;
import indi.sasaki.utils.CustomTableText;

/**
 * @author sasaki
 * A driver maintains table creation, HFile gen and HFile load.
 */
public class BulkLoadDriver {
	public static String inputPath = null;
	public static String outputPath = null;
	public static String tableName = null;
	public static List<String> columns = null;
	public static List<String> columnFamilies = null;
	public static int regionNum = 80;
	public static String startKey = "0|13002600006|13708|1063|1388332863000";   //def start and end key
	public static String endKey = "80|18698589998|55058|32311|1391356863000";
	
	/**
	 * @param args ==> input output tableName tableFile seperator regionNum
	 */
	public static void main(String[] args) {
	
		try {
			paramsInit(args);
			if (PreSplitTable.createTable(startKey, endKey)) {
				long beginTime = System.currentTimeMillis();
				HFileCreater.jobExcu();
				long endTime = System.currentTimeMillis();
				System.out.println("Bulk load costs: " + (endTime - beginTime) / (1.0 * 60 * 1000) + " min");
			} else {
				///// LOG HERE! table created failed
			}
		} catch (IOException e) {    //IOex is always something bad happened
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
	}
	
	public static void paramsInit(String[] params) throws IOException{
		inputPath = params[0];
		outputPath = params[1];
		tableName = params[2];
		regionNum = Integer.parseInt(params[4]);
		List<List<String>> columnList = CustomTableText.read(new File(params[3]));   //potentially throw IOEx
		columns = columnList.get(0);
		columnFamilies = columnList.get(1);
	}
}
