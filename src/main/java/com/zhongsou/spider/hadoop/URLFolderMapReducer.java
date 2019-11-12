package com.zhongsou.spider.hadoop;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class URLFolderMapReducer {
	 static class Uploader 
	  extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		 Character c;
		 HashMap map;

	    private long checkpoint = 100;
	    private long count = 0;
	    
	    @Override
	    public void map(LongWritable key, Text line, Context context)
	    throws IOException {
	      
	      // Input is a CSV file
	      // Each map() is a single line, where the key is the line number
	      // Each line is comma-delimited; row,family,qualifier,value
	            
	      // Split CSV line
	      String [] values = line.toString().split(",");
	      if(values.length != 4) {
	        return;
	      }
	      
	      // Extract each value
	      byte [] row = Bytes.toBytes(values[0]);
	      byte [] family = Bytes.toBytes(values[1]);
	      byte [] qualifier = Bytes.toBytes(values[2]);
	      byte [] value = Bytes.toBytes(values[3]);
	      
	      // Create Put
	      Put put = new Put(row);
	      put.add(family, qualifier, value);
	      
	      // Uncomment below to disable WAL. This will improve performance but means
	      // you will experience data loss in the case of a RegionServer crash.
	      // put.setWriteToWAL(false);
	      
	      try {
	        context.write(new ImmutableBytesWritable(row), put);
	      } catch (InterruptedException e) {
	        e.printStackTrace();
	      }
	      
	      // Set status every checkpoint lines
	      if(++count % checkpoint == 0) {
	        context.setStatus("Emitting Put " + count);
	      }
	    }
	  }
}
