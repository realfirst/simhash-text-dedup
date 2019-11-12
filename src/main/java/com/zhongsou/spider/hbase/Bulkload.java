package com.zhongsou.spider.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;

public class Bulkload {

	
	public static void main(String args[])
	{
		Configuration conf=HBaseConfiguration.create();
		try {
			SchemaMetrics.configureGlobally(conf);
			LoadIncrementalHFiles load=new LoadIncrementalHFiles(conf);
			System.out.println("length="+args.length);
			load.run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
