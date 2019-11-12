package com.zhongsou.spider.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DoubleTextGroupComparator extends WritableComparator {

	protected DoubleTextGroupComparator() {
		super(PipeText.class, true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		PipeText c = (PipeText) a;
		PipeText d = (PipeText) b;
		byte e[] = c.getText().getBytes();
		byte f[] = d.getText().getBytes();

		return compareBytes(e, 0, 8, f, 0, 8);
	}

	/*
	 * public int compare(WritableComparable w1, WritableComparable w2) {
	 * PipeText ip1 = (PipeText) w1; PipeText ip2 = (PipeText) w2; String s =
	 * ip1.getText().toString(); String s2 = ip2.getText().toString(); if (a ==
	 * false) { System.out.println("group use comparator "); a = true; } int cmp
	 * = s.substring(0, 8).compareTo(s2.substring(0, 8)); return cmp; }
	 */
}
