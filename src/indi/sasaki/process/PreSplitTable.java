package indi.sasaki.process;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import indi.sasaki.job.BulkLoadDriver;

public class PreSplitTable {

    public static boolean createTable(String startKey, String endKey) throws IOException{
    	Admin admin = null;
    	Connection conn = ConnectionFactory.createConnection(ConfigInit.configHadoop);
        try {
            admin = conn.getAdmin();
            TableName tableName = TableName.valueOf(BulkLoadDriver.tableName);
            if (admin.tableExists(tableName)) {  
                return false;  
            }
            HTableDescriptor desc = new HTableDescriptor(tableName);
            Set<String> cfSet = new HashSet<String>(BulkLoadDriver.columnFamilies);    //cf must be different
            for(String cf : cfSet){
            	HColumnDescriptor coDesc = new HColumnDescriptor(cf);
            	coDesc.setMaxVersions(1);
                desc.addFamily(coDesc);
            }
            admin.createTable(desc, Bytes.toBytes(startKey), Bytes.toBytes(endKey), BulkLoadDriver.regionNum);
            return true;                     ///// LOG HERE!
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            return false;
        } catch (ZooKeeperConnectionException e) {  
            e.printStackTrace();
            return false;
        }  finally {
        	conn.close();
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    }
  
}
