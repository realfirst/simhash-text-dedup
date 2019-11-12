package com.zhongsou.spider.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.NumberUtil;

public class HostInfo implements WritableComparable<HostInfo>, Cloneable {
	byte domain[];
	byte host[];

	public HostInfo() {
		this.domain = new byte[8];

	}

	public byte[] getDomain() {
		return domain;
	}

	public void setDomain(byte[] domain) {
		this.domain = domain;
	}

	public byte[] getHost() {
		return host;
	}

	public void setHost(byte[] host) {
		this.host = host;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.write(domain);
		if (host != null && host.length > 0) {
			WritableUtils.writeVInt(out, host.length);
			out.write(host);
		} else {
			WritableUtils.writeVInt(out, 0);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		in.readFully(this.domain);
		int length = WritableUtils.readVInt(in);
		this.host = new byte[length];
		in.readFully(this.host);
	}

	static class HostInfoComparator extends WritableComparator {
		public HostInfoComparator() {
			super(HostInfo.class, true);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int i = compareBytes(b1, s1, 8, b2, s2, 8);
			if (i != 0)
				return i;
			else {
				int n3 = WritableUtils.decodeVIntSize(b1[s1 + 8]);
				int n4 = WritableUtils.decodeVIntSize(b2[s2 + 8]);
				i = compareBytes(b1, s1 + 8 + n3, l1 - 8 - n3, b2, s2 + 8 + n4, l2 - 8 - n4);
				return i;
			}
		}

		/*
		 * public int compare(WritableComparable w1, WritableComparable w2) {
		 * PipeText ip1 = (PipeText) w1; PipeText ip2 = (PipeText) w2; String s
		 * = ip1.getText().toString(); String s2 = ip2.getText().toString(); int
		 * cmp = s.substring(0, 8).compareTo(s2.substring(0, 8)); if (cmp != 0)
		 * { return cmp; } return s.substring(8).compareTo(s2.substring(8)); }
		 */

	}

	@Override
	public String toString() {

		StringBuffer buffer = new StringBuffer();
		buffer.append(NumberUtil.getHexString(this.domain) + "\t" + new String(this.host));
		return buffer.toString();
	}

	static { // register this comparator
		WritableComparator.define(HostInfo.class, new HostInfoComparator());
	}

	@Override
	public int compareTo(HostInfo o) {
		// TODO Auto-generated method stub
		int cmp = WritableComparator.compareBytes(domain, 0, 8, o.domain, 0, 8);
		if (cmp != 0) {
			return cmp;
		}
		return WritableComparator.compareBytes(host, 0, host.length, o.getHost(), 0, o.getHost().length);
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}
}