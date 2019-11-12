package com.zhongsou.spider.hadoop;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import com.zhongsou.spider.common.util.NumberUtil;

public class PipesScoreComparator extends WritableComparator {
	public PipesScoreComparator() {
		super(PipeScoreKey.class, true);
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// if (l1 < 22 || l2 < 22) {
		// System.out.println("length error" + l1 + "\t" + l2);
		// }
		int n1 = WritableUtils.decodeVIntSize(b1[s1]);
		int n2 = WritableUtils.decodeVIntSize(b2[s2]);
		// 先按主分组排序，比如folder,host,domain
		int i = compareBytes(b1, s1 + n1, 8, b2, s2 + n2, 8);
		if (i != 0)
			return i;
		else {
			// 倒序排列,从而保证文件夹的的得分在这个文件夹之下的URL前面
			i = compareBytes(b1, s1 + n1 + 8, 1, b2, s2 + n2 + 8, 1);
			if (i != 0)
				return -i;
			else {
				// 读取出评分,倒序
				float f1 = Math.abs(NumberUtil.readFloat(b1, s1 + n1 + 17));
				float f2 = Math.abs(NumberUtil.readFloat(b2, s2 + n2 + 17));
//				System.out.println("binary compare f1=" + f1 + "\t" + f2);
				return f1 > f2 ? -1 : ((f1 < f2) ? 1 : 0);
			}
		}
	}
}
