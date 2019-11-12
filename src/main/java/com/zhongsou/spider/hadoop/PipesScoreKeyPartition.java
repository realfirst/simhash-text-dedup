package com.zhongsou.spider.hadoop;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class PipesScoreKeyPartition implements Partitioner<PipeScoreKey, Text> {

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub

	}

	@Override
	public int getPartition(PipeScoreKey key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		byte s[] = key.getBuffer();
		return Math.abs(Bytes.toString(s, 0, 8).hashCode() * 127) % numPartitions;
	}

}