package com.zhongsou.spider.hbase.test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.log.Log;

import com.zhongsou.incload.MemTable;
import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.hadoop.HostInfo;

public class ScanTest {

	public static void main(String args[]) throws ParseException, IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = null;
		try {
			table = new HTable(conf, "webDB");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		NavigableMap<HRegionInfo, ServerName> tmap;
		try {
			tmap = table.getRegionLocations();
			String hostName;
			InetAddress address = InetAddress.getLocalHost();
			System.out.println("local hostname" + address.getHostName());
			for (Map.Entry<HRegionInfo, ServerName> entry : tmap.entrySet()) {
				System.out.println("serverName="
						+ entry.getValue().getHostname());
			}
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		System.currentTimeMillis();
		// HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY;
		Scan scan = new Scan();
		// scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("u"));
		// scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("u"));
//		scan.addFamily(Bytes.toBytes(""));
	//	scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("f"));
//		scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("d"));
//		scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("u"));
//		scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("f"));
//		scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("m"));

		 scan.addColumn(Bytes.toBytes("P"), Bytes.toBytes("l"));
		 scan.addColumn(Bytes.toBytes("P"), Bytes.toBytes("la"));
		 scan.addColumn(Bytes.toBytes("P"), Bytes.toBytes("n"));
		 scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("u"));
		// scan.addFamily(Bytes.toBytes("H"));
		// scan.addColumn(Bytes.toBytes("P"), Bytes.toBytes("f"));
		// try {
		// scan.setTimeRange(1340266034200l, 1340266034290l);
		// } catch (IOException e1) {
		// // TODO Auto-generated catch block
		// e1.printStackTrace();
		// }
		// TimeRangeFilter tfilter = new TimeRangeFilter(1340259691771l,
		// 1340259704569l);
		// String ti = "1340270704";
//		ParseFilter parse = new ParseFilter();
//		Filter filter;
//	 try {
//		 filter = parse
//		 .parseFilterString("(SingleColumnValueFilter('F','f',!=,'binary:0',true,true) AND SingleColumnValueFilter('F','u',=,'regexstring:http.*lfsmpj.tmall.com.*',true,true))");
//		 scan.setFilter(filter);
//		 } catch (CharacterCodingException e1) {
//		 // TODO Auto-generated catch block
//		 e1.printStackTrace();
//		 }

//		SingleColumnValueFilter lff = null;
//		
//		String t2=StringUtil.toUTF8String(mm, 0, mm.length);
//		System.out.println(t2);
//		byte a1[];
//		try {
//			a1 = t2.getBytes(HConstants.UTF8_ENCODING);
//			System.out.println("byte equqsl=a1 length="+a1.length+"\tmm length="+mm.length+"\t"+(Bytes.compareTo(a1, mm)==0));
//		} catch (UnsupportedEncodingException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		if(true)
//		{
//			System.exit(0);
//		}
		
		
//		System.out.println("filter="+NumberUtil.getHexString(mm));
//		lff = new SingleColumnValueFilter(Bytes.toBytes("F"),
//				Bytes.toBytes("d"), CompareFilter.CompareOp.EQUAL,
//				"regexstring:.{8,}");
//		lff.setFilterIfMissing(true);
//		lff.setLatestVersionOnly(true);
//		scan.setFilter(lff);
//
//		try {
//			Filter rowFilter = parse
//					.parseFilterString("RowFilter(=,'regexstring:.{8,}')");
//			scan.setFilter(filter);
//		} catch (CharacterCodingException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//	
		DateFormat d = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
		java.util.Date date = d.parse("20130104150656");
		int m = (int) (date.getTime() / 1000);
		//conf.set("urlid_finger_load_file", );
		
		 String uric = "/spider_data/new_url_and_finger/20130104150656";
		MemTable memtable=new MemTable(conf);
		 Map<ImmutableBytesWritable, ImmutableBytesWritable> map = memtable.buildTable(uric);
		Set<ImmutableBytesWritable>keySet = map.keySet();
		System.out.println("m=="+m);
		byte mm[]=Bytes.toBytes(m);
		
//		// int tt = 1353701371;
		 SingleColumnValueFilter filter2 = new
		 SingleColumnValueFilter(Bytes.toBytes("P"), Bytes.toBytes("n"),
		 CompareOp.EQUAL, mm);
		 
		 FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			SingleColumnValueFilter nff = null;
			SingleColumnValueFilter lff = null;
				//	nff.setFilterIfMissing(true);
			list.addFilter(filter2);

//			lff = new SingleColumnValueFilter(Bytes.toBytes("P"), Bytes.toBytes("la"), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("0"));
//			lff.setFilterIfMissing(true);
//			list.addFilter(lff);
		 
		 filter2.setFilterIfMissing(true);
		 filter2.setLatestVersionOnly(true);
		 scan.setFilter(list);
		 byte startkey[]=StringUtils.hexStringToByte("d212461407ac285a");
		 byte endKey[]=StringUtils.hexStringToByte("d21271be6f3e93af");
		 scan.setStartRow(startkey);scan.setStopRow(endKey);
		// // filter2.setFilterIfMissing(true);
		// // filter2.setLatestVersionOnly(true);
		//
		// Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new
		// RegexStringComparator(".{9,}"));
		// System.out.println(rowFilter.toString());
		//
		// // scan.setFilter(rowFilter);
		// // // scan.setMaxVersions(1);
		// // scan.setFilter(filter2);
		// scan.setMaxVersions();
		HashSet<String> urlSet = new HashSet<String>();
		String uri = "/tmp/url.txt";

		File file = new File(uri);
		byte t1[] = StringUtils.hexStringToByte("0427a41cf0255b9f10");

		Log.info("bytes" + NumberUtil.getHexString(t1));
		Path path = new Path(uri);
		try {
			// FileSystem fs = FileSystem.get(URI.create(uri), conf);
			scan.setCaching(500);
			BufferedOutputStream output = new BufferedOutputStream(
					new FileOutputStream(file));
			ResultScanner scanner = table.getScanner(scan);

			byte[] H = Bytes.toBytes("H");
			byte[] h = Bytes.toBytes("h");

			byte[] F = Bytes.toBytes("F");
			byte[] u = Bytes.toBytes("u");
			byte []P=Bytes.toBytes("P");
			byte[] a = Bytes.toBytes("a");
			byte[] i = Bytes.toBytes("i");
			byte[] down = Bytes.toBytes("d");
	
			byte []c=Bytes.toBytes("c");
			byte []n=Bytes.toBytes("n");

			// SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
			// new Path(uri), Text.class, NullWritable.class);
			List<Delete> tlist = new LinkedList<Delete>();
			String fileName = "hdfs://hadoop-master-83:9900/user/kaifa/testdata/dd";

			Path lpath = new Path(fileName);
			Log.info("writer path " + fileName);
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(lpath);
			for (FileStatus s : status) {
				System.out.println(s.isDir() + "\t" + s.getPath());
			}
			SequenceFile.Writer loadWriter = SequenceFile.createWriter(fs,
					conf, lpath, HostInfo.class, Text.class);
			HostInfo info = new HostInfo();

			DateFormat dateformat = new java.text.SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			outer: for (Result result : scanner) {
				// System.out.println(result);
				byte t[] = result.getRow();
				// result.getColumnLatest(family, qualifier)
				/*
				 * byte u[] = result.getValue(Bytes.toBytes("F"),
				 * Bytes.toBytes("u")); byte
				 * i[]=result.getValue(Bytes.toBytes("F"), Bytes.toBytes("i"));
				 * int time=Bytes.toInt(i); System.out.println("time="+time);
				 * String m = Bytes.toString(u);
				 */
				ImmutableBytesWritable row=new ImmutableBytesWritable(t);
				
				if(!keySet.contains(row))
				{
					System.out.println("row key is empty ="+NumberUtil.getHexString(t));
				}
				byte time[]=result.getValue(P, n);
				byte load[]=result.getValue(P, Bytes.toBytes("la"));
				byte url[] = result.getValue(F, u);
				String loadtime="";
				if(load!=null&&load.length==4)
				{
					loadtime=String.valueOf(Bytes.toInt(load));
				}
//				byte insert[] = result.getValue(F, i);
//				// byte access[] = result.getValue(F, a);
//				byte download[] = result.getValue(F, down);
//				// byte modify[] = result.getValue(F, m);
//				
//				byte []c1=result.getValue(P, l);
//				byte []n1=result.getValue(P, n);
//				long dd=Bytes.toInt(download, 0);
////				long in = NumberUtil.readInt(insert, 0);
//				// long ac = NumberUtil.readInt(access, 0);
//				// long mo = NumberUtil.readInt(modify, 0);
//
//				Date ddate=new Date(dd*1000);
////				Date din = new Date(in * 1000);
				// Date dac = new Date(ac * 1000);
				// Date dmo = new Date(mo * 1000);
				System.out.println("rowkey=" + NumberUtil.getHexString(t)+"\thost=" + new String(url)+"\tdowntime="+Bytes.toInt(time)+"\tloadtime="+loadtime);
				
//				System.out.println("rowkey=" + NumberUtil.getHexString(t)
//						+ "\turl=" + new String(url) + "\tinsert="
//						+ dateformat.format(ddate) + "\t"
//						+ NumberUtil.getHexString(download)+"\tfilter time="+NumberUtil.getHexString(mm));

				// System.out.println();
				// output.write(url);
				// output.write("\r\n".getBytes());
				// String host = new String(hostbytes);
				// if (host.length() == 0 || host.getBytes().length !=
				// hostbytes.length) {
				// Log.info("error host" + host + "\t src bytes=" +
				// NumberUtil.getHexString(hostbytes));
				//
				// return;
				// }
				// String domain = URLUtil.getDomainByHost(host);
				// if (domain == null) {
				// Log.info("domain is null" + host);
				// return;
				// }
				// byte digest[] = MD5.digest8(domain.getBytes()).getDigest();
				// info.setDomain(digest);
				// String reverseHost = URLUtil.hostReverse(host);
				// info.setHost(reverseHost.getBytes());
				// loadWriter.append(info, new Text(info.getHost()));
				// System.out.println(info.toString());

				// byte ac[] = t;
				// byte a = ac[7];
				// byte b = ac[6];
				// int t2 = a & 0x000000ff;
				// int t3 = b & 0x00000003;
				// if (true) {
				// MD5 md2 = new MD5(result.getRow(), MD5Len.eight, 0);
				// System.out.println("url=\t" + md2.getHexString(false) + "\t"
				// + (t3 * 256 + t2) + "\t" + t.length);
				// System.out.println(t3 * 256 + t2);
				// continue;
				// }
				//
				// byte u[] = result.getValue(Bytes.toBytes("P"),
				// Bytes.toBytes("f"));
				// byte h[] = result.getValue(Bytes.toBytes("P"),
				// Bytes.toBytes("n"));
				// if (u != null && h != null) {
				// // output.write(u);
				// // output.write("\r\n".getBytes());
				// MD5 md2 = new MD5(result.getRow(), MD5Len.eight, 0);
				// System.out.println("url=\t" + md2.getHexString(false));
				// // MD5 md3 = new MD5(result.getRow(), MD5Len.sixten, 0);
				// // System.out.println("con=\t" + md3.getHexString(true));
				// i++;
				// }

				// System.out.println("u" + new
				// String(u)+"\t"+NumberUtil.getHexString(t)+"\tkey length="+t.length);
				//
				// System.out.println("row len=" + t.length);
				// KeyValue kv[] = result.raw();
				// for (KeyValue a : kv) {
				// System.out.println(a);
				// }
				// MD5 md = new MD5(t, MD5Len.eight, 0);
				// if (u != null)
				// System.out.println("key=" + md.getHexString() + "\turl=" +
				// new String(u) + "\t row len=" + t.length);
				// // else
				// System.out.println("key=" + md.getHexString() + "\trow len="
				// + t.length);
				// output.write(u);
				// output.write("\r\n".getBytes());
				// byte s[] = result.getValue(Bytes.toBytes("P"),
				// Bytes.toBytes("f"));
				// writer.append(new Text(t), NullWritable.get());
				// MD5 md5 = new MD5(t, MD5Len.eight, 0);
				// System.out.println(md5.getHexString(true));
				//
				// System.out.println("s=" + new String(s) + "\t" + s.length +
				// "\t" + (s[0] == 0) + "\t" + (s[0] == '0'));
				// String url = new String(u, "utf-8");
				// System.out.println("url="+url);
				// MD5 md5 = new MD5(t, MD5Len.eight, 0);
				// MD5 mdurl = MD5.digest8(url.getBytes("utf-8"));
				// if (!mdurl.getHexString().equals(md5.getHexString())) {
				// System.out.println(new String(u, "gbk") +
				// " is not equal rowkey" + " \t" + url);
				// }

				// i++;
				// System.out.println(i);
				// if (i >= 1000)
				// break;
				// Delete d = new Delete(t);
				// tlist.add(d);
				// if (tlist.size() > 1000) {
				// table.delete(tlist);
				// tlist.clear();
				// }
				// urlSet.add(url);
				// if (i > 10000)
				// break;
				// if (i % 10000 == 0)
				// System.out.print("read " + i);
			}
			loadWriter.close();
			output.close();
			System.out.println("empty url=" + i);
			table.delete(tlist);
			// writer.close();
			long start = System.currentTimeMillis();
			for (String url : urlSet) {
				MD5 t = MD5.digest8(url.getBytes());
			}
			long end = System.currentTimeMillis();
			System.out.println("time consume=" + (end - start));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
