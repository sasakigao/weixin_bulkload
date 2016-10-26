package indi.sasaki.process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class ConfigInit {
	public static Configuration configHadoop = new Configuration(); 
    static {
    	configHadoop.set("mapreduce.output.textoutputformat.separator", "\\|");
    	configHadoop.set("hbase.zookeeper.property.clientPort", "2181");
    	configHadoop.set("hbase.zookeeper.quorum", "sparkA11");
    	HBaseConfiguration.addHbaseResources(configHadoop);    //load the core-site.xml into hadoop configuration
    }
}
