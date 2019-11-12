package com.zhongsou.spider.hbase;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;

import com.zhongsou.spider.common.util.NumberUtil;

public class DataParser {
	private final byte[][] families;
	private final byte[][] qualifiers;
	Map<String, Integer> columnMap;
	private boolean isMapReduceOutput = true;

	public DataParser(String column, String separator) {
		columnMap = new HashMap<String, Integer>();
		String t[] = column.split(separator);
		if (t != null && t.length > 0) {
			int i = 0;
			families = new byte[t.length][];
			qualifiers = new byte[t.length][];
			for (String m : t) {
				columnMap.put(m, i);
				String[] parts = m.split(":", 2);
				if (parts.length == 1) {
					families[i] = m.getBytes();
					qualifiers[i] = HConstants.EMPTY_BYTE_ARRAY;
				} else {
					families[i] = parts[0].getBytes();
					qualifiers[i] = parts[1].getBytes();
				}
				i++;
			}
		} else {
			families = new byte[0][];
			qualifiers = new byte[0][];
		}
	}

	public DataParser(String column, String separator, boolean isMapReduceOutput) {
		this(column, separator);
		this.isMapReduceOutput = isMapReduceOutput;
	}

	public byte[] getFamily(int idx) {
		return families[idx];
	}

	public byte[] getQualifier(int idx) {
		return qualifiers[idx];
	}

	public static class PosAndOffset {
		int start;
		int len;

		public PosAndOffset(int start, int len) {
			this.start = start;
			this.len = len;
		}

		public int getStart() {
			return start;
		}

		public void setStart(int start) {
			this.start = start;
		}

		public int getLen() {
			return len;
		}

		public void setLen(int len) {
			this.len = len;
		}

	}

	public List<PosAndOffset> parse(byte[] lineBytes, int length) {
		// Enumerate separator offsets
		List<PosAndOffset> tlist = new LinkedList<PosAndOffset>();
		int len;
		int index = 0;
		int packets=NumberUtil.readInt(lineBytes, 0);
		for (int offset = 4; offset + 4 < length&&index<packets;) {
			len = NumberUtil.readInt(lineBytes, offset);
			offset += 4;
			tlist.add(new PosAndOffset(offset, len));
			offset += len;
			index++;
		}
		return tlist;
	}

}
