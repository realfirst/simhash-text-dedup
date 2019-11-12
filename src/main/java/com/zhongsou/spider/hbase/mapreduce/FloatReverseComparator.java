package com.zhongsou.spider.hbase.mapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparator;

public class FloatReverseComparator extends WritableComparator {
	public FloatReverseComparator() {
		super(FloatWritable.class);
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		float thisValue = readFloat(b1, s1);
		float thatValue = readFloat(b2, s2);
		return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
	}

	@Override
	public int compare(Object a, Object b) {
		// TODO Auto-generated method stub
		return -1 * super.compare(a, b);
	}
	
}