package com.zhongsou.spider.hadoop;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class DoubleTextPartition implements Partitioner<PipeText, Text> {
	public int getPartition(PipeText key, Text value, int numPartitions) {
		byte s[] = key.getText().getBytes();
		return Math.abs(Bytes.toString(s, 0, 8).hashCode() * 127) % numPartitions;
	}

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		return ;
	}

}
