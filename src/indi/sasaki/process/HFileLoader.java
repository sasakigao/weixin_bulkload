package indi.sasaki.process;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

import indi.sasaki.job.BulkLoadDriver;

/**
 * @author sasaki
 * This part loads the HFiles into a table.
 */
public class HFileLoader {

	public static void load() throws Exception {		
		Connection conn = ConnectionFactory.createConnection(ConfigInit.configHadoop);
		TableName tableName = TableName.valueOf(BulkLoadDriver.tableName);
		Table table = conn.getTable(tableName);
		
		try {
			LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(ConfigInit.configHadoop);
			HTable hTable = new HTable(ConfigInit.configHadoop, tableName);
			loadFfiles.doBulkLoad(new Path(BulkLoadDriver.outputPath), hTable);
		} finally {
			table.close();
			conn.close();
		}
	}

}
