package com.zhongsou.spider.common.util;

public class BitsUtil {

	static int setBit(int ele, int index) {
		ele = ele | (1 << index);
		return ele;
	}

	static int clearBit(int ele, int index) {
		ele = ele & ~(1 << index);
		return ele;
	}

}
