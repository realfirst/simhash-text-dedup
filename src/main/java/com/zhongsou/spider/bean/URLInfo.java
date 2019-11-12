package com.zhongsou.spider.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.io.Writable;

public class URLInfo implements Writable {
	byte[] urlmd5;
	byte[] url;
	byte[] srcURLmd5;;
	static Hash hash = Hash.getInstance(1);

	public byte[] getUrlmd5() {
		return urlmd5;
	}

	public void setUrlmd5(byte[] urlmd5) {
		this.urlmd5 = urlmd5;
	}

	public byte[] getUrl() {
		return url;
	}

	public void setUrl(byte[] url) {
		this.url = url;
	}

	public byte[] getSrcURLmd5() {
		return srcURLmd5;
	}

	public void setSrcURLmd5(byte[] srcURLmd5) {
		this.srcURLmd5 = srcURLmd5;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		if (url != null) {
			out.writeInt(url.length);
			out.write(url);
		} else
			out.writeInt(0);
		if (urlmd5 != null) {
			out.writeInt(urlmd5.length);
			out.write(urlmd5);
		} else {
			out.writeInt(0);
		}
		if (srcURLmd5 != null) {
			out.writeInt(srcURLmd5.length);
			out.write(srcURLmd5);
		} else {
			out.writeInt(0);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int t = in.readInt();
		if (t > 0) {
			url = new byte[t];
			in.readFully(url);
		} else {
			url = null;
		}
		t = in.readInt();
		if (t > 0) {
			urlmd5 = new byte[t];
			in.readFully(urlmd5);
		} else {
			urlmd5 = null;
		}
		t = in.readInt();
		if (t > 0) {
			srcURLmd5 = new byte[t];
			in.readFully(srcURLmd5);
		} else {
			srcURLmd5 = null;
		}
	}

	public int hashCode() {
		if (urlmd5 != null) {
			int code = hash.hash(urlmd5);
			return code;
		} else {
			return 0;
		}

	}

	public boolean equals(Object o) {
		if (!(o instanceof URLInfo))
			return false;
		else {
			URLInfo info = (URLInfo) o;
			return Arrays.equals(this.urlmd5, info.urlmd5);
		}
	}

}
