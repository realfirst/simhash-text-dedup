package com.zhongsou.spider.hadoop;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class DoubleTextComparator extends WritableComparator {
	public DoubleTextComparator() {
		super(PipeText.class, true);
	}


	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		int n1 = WritableUtils.decodeVIntSize(b1[s1]);
		int n2 = WritableUtils.decodeVIntSize(b2[s2]);
		int i = compareBytes(b1, s1 + n1, 8, b2, s2 + n2, 8);
		if (i != 0)
			return i;
		else
			return -compareBytes(b1, s1 + n1 + 8, l1 - n1 - 8, b2, s2 + n2 + 8, l2 - n2 - 8);
	}

	/*
	 * public int compare(WritableComparable w1, WritableComparable w2) {
	 * PipeText ip1 = (PipeText) w1; PipeText ip2 = (PipeText) w2; String s =
	 * ip1.getText().toString(); String s2 = ip2.getText().toString(); int cmp =
	 * s.substring(0, 8).compareTo(s2.substring(0, 8)); if (cmp != 0) { return
	 * cmp; } return s.substring(8).compareTo(s2.substring(8)); }
	 */

}
