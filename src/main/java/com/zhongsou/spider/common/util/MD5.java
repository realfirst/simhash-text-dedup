package com.zhongsou.spider.common.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class MD5 {
	private static ThreadLocal<MessageDigest> DIGESTER_FACTORY = new ThreadLocal<MessageDigest>() {
		protected MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e);
			}
		}
	};

	private byte[] digest;

	public MD5() {
		this.digest = new byte[8];
	}

	public enum MD5Len {
		eight, sixten;
	}

	public MD5(byte t[], MD5Len len, int start) throws Exception {
		if (len == MD5Len.eight) {
			digest = new byte[8];
			if (t.length < start + 8)
				throw new Exception("byte array to short");
			System.arraycopy(t, start, digest, 0, 8);
		} else {
			digest = new byte[16];
			if (t.length < start + 16)
				throw new Exception("byte array to short");
			System.arraycopy(t, start, digest, 0, 16);
		}

	}

	public MD5(MD5Len len) {
		if (len == MD5Len.eight) {
			digest = new byte[8];
		} else {
			digest = new byte[16];
		}

	}

	public byte[] getDigest() {
		return this.digest;
	}

	public String getHexString() {
		StringBuffer buf = new StringBuffer(this.digest.length * 2);
		for (int i = 0; i < this.digest.length; i++) {
			int b = digest[i];
			buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
			buf.append(HEX_DIGITS[b & 0xf]);
		}
		return buf.toString();
	}

	public String getHexString(boolean withFlag) {
		StringBuffer buf = new StringBuffer(this.digest.length * 2);
		for (int i = 0; i < this.digest.length; i++) {
			int b = digest[i];
			if (withFlag)
				buf.append("\\x");
			buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
			buf.append(HEX_DIGITS[b & 0xf]);
		}
		return buf.toString();
	}

	public void set(MD5 other) {
		if (this.digest.length != other.digest.length) {
			this.digest = new byte[other.digest.length];
		}
		System.arraycopy(other.getDigest(), 0, this.digest, 0, other.getDigest().length);
	}

	public static MD5 digest16(byte[] data) {
		return digest16(data, 0, data.length);
	}

	public static MD5 digest8(byte[] data) {
		return digest8(data, 0, data.length);
	}

	public static String digest8(String str) {
		return digest8(str.getBytes()).getHexString();
	}

	public static String digest16(String str) {
		return digest16(str.getBytes()).getHexString();
	}

	public static MD5 digest8(byte[] data, int start, int len) {
		byte[] digest;
		MessageDigest digester = DIGESTER_FACTORY.get();
		digester.update(data, start, len);
		digest = digester.digest();
		try {
			return new MD5(digest, MD5Len.eight, 0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public static MD5 digest16(byte[] data, int start, int len) {
		byte[] digest;
		MessageDigest digester = DIGESTER_FACTORY.get();
		digester.update(data, start, len);
		digest = digester.digest();
		try {
			return new MD5(digest, MD5Len.sixten, 0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	public String toString() {
		StringBuffer buf = new StringBuffer(this.digest.length * 2);
		for (int i = 0; i < this.digest.length; i++) {
			int b = digest[i];
			buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
			buf.append(HEX_DIGITS[b & 0xf]);
		}
		return buf.toString();
	}

	public int hashCode() {
		int t = 0;
		for (int i = 0; i < this.digest.length; i++) {
			t += this.digest[i];
		}
		return t;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (!(obj instanceof MD5))
			return false;
		MD5 o = (MD5) obj;
		if (o == null || this.digest == null || o.digest.length != this.digest.length)
			return false;
		else {
			return Arrays.equals(o.digest, this.digest);
		}
	}

	public static void main(String args[]) {
		MD5 md5 = MD5.digest8("http://hxs.ucbrifnocs.lwsrpqupptbiecqv/myfv/njo/lypfgptdza/qpmdo/cszuo/fa/meeea/gxxgwszpx".getBytes());
		System.out.println(md5.getHexString());
	}

}