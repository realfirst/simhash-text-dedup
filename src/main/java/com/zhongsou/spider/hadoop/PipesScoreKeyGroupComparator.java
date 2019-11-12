package com.zhongsou.spider.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PipesScoreKeyGroupComparator extends WritableComparator {

	protected PipesScoreKeyGroupComparator() {
		super(PipeScoreKey.class, true);
		// TODO Auto-generated constructor stub
	}

	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		PipeScoreKey c = (PipeScoreKey) a;
		PipeScoreKey d = (PipeScoreKey) b;
		byte e[] = c.getBuffer();
		byte f[] = d.getBuffer();

		return compareBytes(e, 0, 8, f, 0, 8);
	}
}