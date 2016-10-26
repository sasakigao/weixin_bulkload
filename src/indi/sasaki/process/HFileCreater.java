package indi.sasaki.process;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;

import indi.sasaki.job.BulkLoadDriver;
import indi.sasaki.utils.CustomTableText;
import indi.sasaki.utils.File2Join;
import indi.sasaki.utils.RowkeyAssembler;

public class HFileCreater extends 
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	
	private static final String cacheJoinFile = "hdfs:///bulkload/cache/goodbase-hz-id.csv#symcache";   //exist on hdfs yet
	private static final String cacheColumnFile = "hdfs:///bulkload/cache/column#column";
	
	private static String seperator = null;
	private static List<String> columns = null;
	private static List<String> columnFamilies = null;
	private static int regionNum;
	private static Map<String, String[]> mapSideJoin = new HashMap<String, String[]>();
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 * Do a map-side-join work in mapper
	 */
	public void map(LongWritable lineNum, Text rawRecord, Context context)
			throws IOException, InterruptedException {
		String[] oneRecord = rawRecord.toString().split(seperator); serious
		
		
		if (oneRecord.length == 27 && oneRecord[1].length() <= 5 
				&& oneRecord[2].length() <= 5) {
			RowkeyAssembler assembler = new RowkeyAssembler(oneRecord[0], oneRecord[1], 
					oneRecord[2], oneRecord[5], regionNum);
			byte[] rkBytes;
			try {
				rkBytes = assembler.saltedKey();
				ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rkBytes);
				Put put = new Put(rkBytes);
				
				String baseInWeixin = oneRecord[1] + "-" + oneRecord[2];
				String[] joinedValue = mapSideJoin.get(baseInWeixin);
				if (joinedValue != null) {
					int i;
					int[] usedFields = {3, 4, 5, 27, 28};
					for (i = 0; i < usedFields.length - 2; i++) {
						put.addColumn(Bytes.toBytes(columnFamilies.get(usedFields[i])), 
								Bytes.toBytes(columns.get(usedFields[i])), 
								Bytes.toBytes(oneRecord[usedFields[i]]));
					}
					put.addColumn(Bytes.toBytes(columnFamilies.get(usedFields[i])),         //add the extra joined fields
							Bytes.toBytes(columns.get(usedFields[i])), Bytes.toBytes(joinedValue[0]));
					put.addColumn(Bytes.toBytes(columnFamilies.get(usedFields[i + 1])), 
							Bytes.toBytes(columns.get(usedFields[i + 1])), Bytes.toBytes(joinedValue[1]));
					context.write(rowKey, put);
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 * Run before map
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException {
		File cacheColumn = new File("./column"); 
		List<List<String>> columnList = CustomTableText.read(cacheColumn);   //potentially throw IOEx
		columns = columnList.get(0);
		columnFamilies = columnList.get(1);	
		regionNum = 80;
		File cacheJoin = new File("./symcache");             //read by simlink #symcache
		mapSideJoin.putAll(File2Join.readAsMap(cacheJoin));	
		seperator = "\\|";
	}
	
	public static void jobExcu() throws Exception {
		Connection conn = ConnectionFactory.createConnection(ConfigInit.configHadoop);
		try {
			Job job = Job.getInstance(ConfigInit.configHadoop, "HFile Generator");
			job.setJarByClass(HFileCreater.class);                  // class that contains mapper
			job.setMapperClass(HFileCreater.class);
//			job.setReducerClass(PutSortReducer.class);           //source auto decides the reducer, below the same
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
//	        job.setInputFormatClass(TextInputFormat.class);  
//      	job.setOutputFormatClass(HFileOutputFormat2.class);
			job.addCacheFile(new URI(cacheJoinFile));      //cache the small file on hdfs for join. "some" is a symbol link.
			job.addCacheFile(new URI(cacheColumnFile));
			FileInputFormat.addInputPath(job, new Path(BulkLoadDriver.inputPath));
			FileOutputFormat.setOutputPath(job, new Path(BulkLoadDriver.outputPath));
			
			TableName name = TableName.valueOf(BulkLoadDriver.tableName);
			HFileOutputFormat2.configureIncrementalLoad(job, 
					conn.getTable(name), conn.getRegionLocator(name));
			
			job.waitForCompletion(true);
			
			/*if (job.isSuccessful()) {
				HFileLoader.load();
			}*/
		} finally {
			conn.close();
		}
	}
}
