package com.zhongsou.incload;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.StringUtils;

import com.zhongsou.spider.common.util.NumberUtil;

/**
 * Describe class <code>PageNode</code> here.
 * 
 * @author <a href="mailto:dingje@zhongsou.com">Ding Jingen</a>
 * @version 1.0
 */
public class PageNode implements WritableComparable<PageNode> {
	private byte[] urlid;
	private byte[] finger;
	private byte[] pr;
	private byte[] lf;
	private byte[] nf;

	public PageNode() {
		set(new byte[8], new byte[8], new byte[4], new byte[1], new byte[1]);
	}

	public PageNode(PageNode pn) {
		set(pn);
	}

	public PageNode(byte[] urlid, byte[] finger, byte[] pr, byte[] lf, byte[] nf) {
		set(urlid, finger, pr, lf, nf);
	}

	public void set(byte[] urlid, byte[] finger, byte[] pr, byte[] lf, byte[] nf) {
		this.urlid = urlid;
		this.finger = finger;
		this.pr = pr;
		this.lf = lf;
		this.nf = nf;
	}

	public void set(PageNode o) {
		set(o.getUrlid(), o.getFinger(), o.getPr(), o.getLf(), o.getNf());
	}

	public void setUrlid(byte[] urlid) {
		this.urlid = urlid;
	}

	public void setFinger(byte[] finger) {
		this.finger = finger;
	}

	public void setPr(byte[] pr) {
		this.pr = pr;
	}

	public void setLf(byte[] lf) {
		this.lf = lf;
	}

	public void setNf(byte[] nf) {
		this.nf = nf;
	}

	public byte[] getUrlid() {
		return urlid;
	}

	public byte[] getFinger() {
		return finger;
	}

	public byte[] getPr() {
		return pr;
	}

	public byte[] getLf() {
		return lf;
	}

	public byte[] getNf() {
		return nf;
	}

	public void write(DataOutput out) throws IOException {
		out.write(urlid, 0, 8);
		out.write(finger, 0, 8);
		out.write(pr, 0, 4);
		out.write(lf, 0, 1);
		out.write(nf, 0, 1);
	}

	public void readFields(DataInput in) throws IOException {
		in.readFully(urlid, 0, 8);
		in.readFully(finger, 0, 8);
		in.readFully(pr, 0, 4);
		in.readFully(lf, 0, 1);
		in.readFully(nf, 0, 1);
	}

	private Float getScore() {
		return Bytes.toFloat(this.getPr());
	}

	@Override
	public int compareTo(PageNode other) {
		return this.getScore().compareTo(other.getScore());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((urlid == null) ? 0 : Arrays.hashCode(urlid));
		result = prime * result + ((finger == null) ? 0 : Arrays.hashCode(finger));
		result = prime * result + ((pr == null) ? 0 : Arrays.hashCode(pr));
		result = prime * result + ((lf == null) ? 0 : Arrays.hashCode(lf));
		result = prime * result + ((nf == null) ? 0 : Arrays.hashCode(nf));

		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof PageNode)) {
			return false;
		}

		PageNode other = (PageNode) o;

		return Arrays.equals(urlid, other.urlid) && Arrays.equals(finger, other.finger) && Arrays.equals(pr, other.pr) && Arrays.equals(lf, other.lf) && Arrays.equals(nf, other.nf);
	}

	@Override
	public String toString() {
		return "urlid:" + NumberUtil.getHexString(urlid) + "\t" + "finger:" + NumberUtil.getHexString(finger) + "\t" + "pr:" + String.valueOf(Bytes.toFloat(pr)) + "\t" +
		// "pr:" + Bytes.toFloat(pr) + "\t" +
				"lf:" + NumberUtil.getHexString(lf) + "\t" +
				// "lf:" + Bytes.toInt(lf) + "\t" +
				"nf:" + NumberUtil.getHexString(nf);
		// "nf:" + Bytes.toInt(nf);
	}

	/*
	 * public static class Comparator extends WritableComparator { public
	 * Comparator() { super(PageNode.class); }
	 * 
	 * public int compare(byte[] b1, int s1, int l1, byte [] b2, int s2, int l2)
	 * {
	 * 
	 * } }
	 * 
	 * static { WritableComparator.define(PageNode.class, new Comparator()); }
	 */
}
