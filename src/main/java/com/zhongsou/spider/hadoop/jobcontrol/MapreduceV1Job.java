package com.zhongsou.spider.hadoop.jobcontrol;

import org.apache.hadoop.mapred.RunningJob;

public interface MapreduceV1Job {

	public RunningJob createRunnableJob(String args[]);

}
