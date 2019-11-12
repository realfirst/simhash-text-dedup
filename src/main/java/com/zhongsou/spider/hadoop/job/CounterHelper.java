package com.zhongsou.spider.hadoop.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;

public class CounterHelper {

	public static long getCounterValue(RunningJob job, Enum<?> c) {
		try {
			Counters s = job.getCounters();
			return s.getCounter(c);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	public static long getCounterValue(RunningJob job, String groupName, String counterName) {
		try {
			Counters s = job.getCounters();
			Group group = s.getGroup(groupName);
			return group.getCounter(counterName);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	public static int counterMap(Job job, HashMap<String, String> tmap) {
		try {
			org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
			Iterator<CounterGroup> it = counters.iterator();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

}
