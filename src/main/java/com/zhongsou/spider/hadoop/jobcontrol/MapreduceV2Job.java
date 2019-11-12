package com.zhongsou.spider.hadoop.jobcontrol;

import org.apache.hadoop.mapreduce.Job;

public interface MapreduceV2Job {
	public Job createRunnableJob(String args[]);
}
