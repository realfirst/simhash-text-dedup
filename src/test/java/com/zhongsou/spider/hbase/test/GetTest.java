package com.zhongsou.spider.hbase.test;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.NumberUtil;

public class GetTest {
	// HTable table;

	// public GetTest(String tableName) throws IOException {
	// Configuration conf = HBaseConfiguration.create();
	// table = new HTable(conf, tableName);
	// byte[][] startKeys = table.getStartKeys();
	// System.out.println("start key length=" + startKeys.length);
	// }

	// public void getRow(String url) {
	// byte a[] = StringUtils.hexStringToByte("07a05d74387dddd4");
	// byte b[] = StringUtils.hexStringToByte("865d922e1972c3c0");
	// MD5 md = null;
	// MD5 md2 = null;
	// byte[] row = new byte[16];
	// try {
	// md = MD5.digest8(url.getBytes());
	// String host = new URL(url).getHost();
	// md2 = MD5.digest8(host.getBytes());
	// System.arraycopy(md2.getDigest(), 0, row, 0, 8);
	// System.arraycopy(md.getDigest(), 0, row, 8, 8);
	// } catch (Exception e1) {
	// // TODO Auto-generated catch block
	// e1.printStackTrace();
	// }
	// Get get = new Get(row);
	//
	// Get get2 = new Get(md2.getDigest());
	// try {
	// MD5 md3 = new MD5(row, MD5Len.sixten, 0);
	// System.out.println(md3.getHexString(true));
	// } catch (Exception e1) {
	// // TODO Auto-generated catch block
	// e1.printStackTrace();
	// }
	//
	// System.out.println(md.getDigest().length + "\t" + md);
	// // get.addColumn(Bytes.toBytes("F"), Bytes.toBytes("s"));
	// get.addFamily(Bytes.toBytes("H"));
	// get.addFamily(Bytes.toBytes("F"));
	//
	// Result result;
	// try {
	// result = table.get(get);
	// byte[] val = result.getValue(Bytes.toBytes("H"), Bytes.toBytes("s"));
	// System.out.print("value=" + new String(val));
	//
	// // System.out.println("content length=" + val.length);
	// // System.out.println(new String(val, "utf-8"));
	// // System.out.println("Result:" + result);
	// KeyValue kvs[] = result.raw();
	// for (KeyValue kv : kvs) {
	// byte t[] = kv.getKey();
	// byte t2[] = kv.getRow();
	//
	// MD5 md5 = new MD5(t2, MD5Len.eight, 0);
	// System.out.println("what" + t2.length + "\t" + md5);
	// System.out.println(kv.getKeyOffset() + "\t" + kv.getKeyLength() + "\t" +
	// kv.getKeyString() + "\t" + Bytes.toString(kv.getValue()));
	// }
	// // System.out.println("Value: " + Bytes.toString(val));
	// // val=result.getValue(Bytes.toBytes("info"),
	// // Bytes.toBytes("data"));
	// // System.out.println("Value: " + Bytes.toString(val));
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// } // co GetExample-5-DoGet Retrieve row with selected columns from
	// // HBase.
	// catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	//
	// }

	static DateFormat dateFormat = new java.text.SimpleDateFormat(
			"yyyy-MM-dd HH:MM:ss");

	static byte[] F = Bytes.toBytes("F");
	static byte[] H = Bytes.toBytes("H");
	static byte[] S = Bytes.toBytes("S");
	static byte[] s = Bytes.toBytes("s");
	static byte[] sh = Bytes.toBytes("sh");
	static byte[] a = Bytes.toBytes("a");
	static byte[] c = Bytes.toBytes("c");
	static byte[] m = Bytes.toBytes("m");
	static byte[] d = Bytes.toBytes("d");
	static byte[] i = Bytes.toBytes("i");
	static byte[] md = Bytes.toBytes("md");
	static byte[] nd = Bytes.toBytes("nd");

	
	public static void getHostDBRow(String key) {
		byte[] row = MD5.digest8(key.getBytes()).getDigest();
		Configuration conf = HBaseConfiguration.create();
		HTable table = null;
		try {
			table = new HTable(conf, "hostDB");
			Get get = new Get(row);
			get.addFamily(Bytes.toBytes("H"));
			Result result;
			result = table.get(get);
			byte[] urlsum = result.getValue(H, s);

			int url_sum = 0, host_num = 0;
			long accessTime = 0, modifyTime = 0, insertTime = 0;
			StringBuffer buffer = new StringBuffer();
			if (urlsum != null) {
				url_sum = NumberUtil.readInt(urlsum, 0);
			}
			byte[] b_sh = result.getValue(H, sh);
			if (b_sh != null) {
				host_num = NumberUtil.readInt(b_sh, 0);
			}
			buffer.append("url num=" + url_sum + "\thost_num=" + host_num
					+ "\t");
			byte[] b_a = result.getValue(H, a);

			if (b_a != null) {
				accessTime = NumberUtil.readInt(b_a, 0);
				Date d = new Date(accessTime * 1000);
				buffer.append("access time=" + dateFormat.format(d) + "\t");
			}

			byte[] b_m = result.getValue(H, m);
			if (b_m != null) {
				modifyTime = NumberUtil.readInt(b_m, 0);
				Date d = new Date(modifyTime * 1000);
				buffer.append("modifyTime=" + dateFormat.format(d) + "\t");
			}

			byte[] b_i = result.getValue(H, i);
			if (b_i != null) {
				insertTime = NumberUtil.readInt(b_i, 0);
				Date d = new Date(insertTime * 1000);
				buffer.append("insertTime=" + dateFormat.format(d) + "\t");
			}

			byte[] b_md = result.getValue(H, md);
			buffer.append("\nmodify num:");
			if (b_md != null) {
				for (int i = 0; i < b_md.length / 4; i++) {
					int num = NumberUtil.readInt(b_md, i * 4);
					buffer.append(num+"\t");
				}
			}
			buffer.append("\nnew url num:");
			byte[] b_nd = result.getValue(H, nd);

			if (b_nd != null) {
				for (int i = 0; i < b_md.length / 4; i++) {
					int num = NumberUtil.readInt(b_nd, i * 4);
					buffer.append(num+"\t");
				}
			}
			System.out.println(buffer.toString());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	
	
	public static void getSelectHostDBRow(String key) {
		byte[] row = MD5.digest8(key.getBytes()).getDigest();
		Configuration conf = HBaseConfiguration.create();
		HTable table = null;
		try {
			table = new HTable(conf, "selectHostDB");
			Get get = new Get(row);
			get.addFamily(Bytes.toBytes("S"));
			Result result;
			result = table.get(get);
			byte[] urlsum = result.getValue(S, s);

			int url_sum = 0, host_num = 0;
			long accessTime = 0, modifyTime = 0, insertTime = 0;
			StringBuffer buffer = new StringBuffer();
			if (urlsum != null) {
				url_sum = NumberUtil.readInt(urlsum, 0);
			}
			byte[] b_sh = result.getValue(S, sh);
			if (b_sh != null) {
				host_num = NumberUtil.readInt(b_sh, 0);
			}
			buffer.append("url num=" + url_sum + "\thost_num=" + host_num
					+ "\t");
			byte[] b_a = result.getValue(S, a);

			if (b_a != null) {
				accessTime = NumberUtil.readInt(b_a, 0);
				Date d = new Date(accessTime * 1000);
				buffer.append("access time=" + dateFormat.format(d) + "\t");
			}

			byte[] b_m = result.getValue(S, m);
			if (b_m != null) {
				modifyTime = NumberUtil.readInt(b_m, 0);
				Date d = new Date(modifyTime * 1000);
				buffer.append("modifyTime=" + dateFormat.format(d) + "\t");
			}

			byte[] b_i = result.getValue(S, i);
			if (b_i != null) {
				insertTime = NumberUtil.readInt(b_i, 0);
				Date d = new Date(insertTime * 1000);
				buffer.append("insertTime=" + dateFormat.format(d) + "\t");
			}

			byte[] b_md = result.getValue(S, md);
			buffer.append("\nmodify num:");
			if (b_md != null) {
				for (int i = 0; i < b_md.length / 4; i++) {
					int num = NumberUtil.readInt(b_md, i * 4);
					buffer.append(num+"\t");
				}
			}
			buffer.append("\nnew url num:");
			byte[] b_nd = result.getValue(S, nd);

			if (b_nd != null) {
				for (int i = 0; i < b_md.length / 4; i++) {
					int num = NumberUtil.readInt(b_nd, i * 4);
					buffer.append(num+"\t");
				}
			}
			System.out.println(buffer.toString());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	public static void main(String args[]) throws IOException {
		// long a = 0x137c9c3ba25L;
		// Date b = new Date(a);
		// System.out.println(b);
		// GetTest t = new GetTest("selectHostDB");

		// UUID uuid = UUID.randomUUID();
		// System.out.println(uuid.toString());
		//
		// DateFormat format = new
		// java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// String str = "2012-06-20 15:00:00";
		// String str2 = "2012-06-20 16:50:00";
		//
		long a=1355835751;
		Date d=new Date(a*1000);
		System.out.println(d);
//		GetTest.getSelectHostDBRow("mc.163.com.yizhan.baidu.com");
//		http://www.lytynw.cn.yizhan.baidu.com/wstong/
		GetTest.getHostDBRow("mc.163.com.yizhan.baidu.com");
		// try {
		// b = format.parse(str);
		// Date c = format.parse(str2);
		// System.out.println(str + "\t" + b.getTime());
		// System.out.println(str + "\t" + c.getTime());
		// } catch (ParseException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// // t.getRow("http://amuse.nen.com.cn/amuse/46/3734046.shtml");
		// String url =
		// "http://v3r-nbj.bbs.house.sina.com.cn/thread-12821822-172.html";
		// String host = URLUtil.getHost(url);
		// System.out.println(host);
		// try {
		// MD5 md5 = MD5.digest8(host.getBytes(), 0, host.getBytes().length);
		// System.out.println(md5.getHexString(true));
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		// int i=0,j=0,k=0;
		//
		// for(i=0;i<16;i++)
		// {
		// for(j=0;j<16;j+=4)
		// {
		// if((i==0&&j==0)||(i==0xF&&j==0xf))
		// {
		// continue;
		// }
		// System.out.print("\"\\x"+Integer.toHexString(i)+"0\\x"+Integer.toHexString(j)+"0\",");
		// }
		// }

	}

}