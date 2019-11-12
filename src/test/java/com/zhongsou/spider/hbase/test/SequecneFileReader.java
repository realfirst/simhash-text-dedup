package com.zhongsou.spider.hbase.test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.URLUtil;
import com.zhongsou.spider.hadoop.HostInfo;
import com.zhongsou.spider.hbase.DataParser;
import com.zhongsou.spider.hbase.DataParser.PosAndOffset;

public class SequecneFileReader {

	static {
		System.loadLibrary("snappy");
		System.loadLibrary("hadoopsnappy");
	}

	public static void main(String[] args) throws IOException {

		String uri = null;
		if (args.length == 0)
			uri = "hdfs://hadoop-master-83:9900/user/kaifa/spider/folder_output/part-00000";
		else
			uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(new File("a.txt")));
		SequenceFile.Reader reader = null;
		try {
		
//			List<Statistics> list = FileSystem.getAllStatistics();
//			DistributedFileSystem dfs=(DistributedFileSystem)fs;
//			DatanodeInfo info[]=dfs.getDataNodeStats();
//			System.out.println("info length"+info.length);
//			for (DatanodeInfo datainfo : info) {
//				System.out.println(datainfo.getHostName());
//
//			}
//			if (true) {
//				System.exit(0);
//			}
			reader = new SequenceFile.Reader(fs, path, conf);
			System.out.println("block compressed" + reader.isBlockCompressed() + "\tcompressed=" + reader.isCompressed());
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			int t = 0;
			// finger = value.substr(12, 8);
			// new_url_count = *(int *) (value.c_str() + 20);
			// modify_count = *(int *) (value.c_str() + 24);
			// error_count = *(int *) (value.c_str() + 28);
			// int urlLen = *(int *) (value.c_str() + 32 + sizeof(time_t));
			// url = value.substr(36 + sizeof(time_t), urlLen);
			int new_url_count, modify_count, error_count, urlLen;
			String url;
			int i = 0, j = 0;
			MD5 md = MD5.digest8("http://product.cheshi.com/bseries_183/dianping_t211177_1.html".getBytes());
			HashSet<MD5> tset = new HashSet<MD5>();
			DataParser parser = new DataParser("F:nd,F:md,F:m,F:a,F:c,F:s,F:p", ",");
			long ts=System.currentTimeMillis();
			while (reader.next(key, value)) {
				System.out.println(key.toString() + "\t" + value.toString());
				//

				byte[] lineBytes = value.getBytes();
				List<PosAndOffset> parsedList = parser.parse(lineBytes, value.getLength());
				System.out.println("lineBytes length="+lineBytes.length+" list size="+parsedList.size());
				if (parsedList.size() <= 1)
					return;
			//	PosAndOffset p = parsedList.get(0);
				ImmutableBytesWritable rowKey = new ImmutableBytesWritable(key.getBytes(),0, key.getLength());
				Put put = new Put(rowKey.copyBytes());
				for (i = 0; i < parsedList.size(); i++) {
					PosAndOffset q = parsedList.get(i);
//					KeyValue kv = new KeyValue(key.getBytes(),0, key.getLength(), parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0, parser.getQualifier(i).length, ts,
//							KeyValue.Type.Put, lineBytes, q.getStart(), q.getLen());
					System.out.println("family:="+new String(parser.getFamily(i),0,parser.getFamily(i).length));
					System.out.println("q:="+new String(parser.getQualifier(i),0,parser.getQualifier(i).length));
					System.out.println("start"+q.getStart()+"\t"+ q.getLen());
						
					//put.add(kv);
				}
				
//				String domain = "";
//				String reverseDomain = "";
//				String host = "";
//				boolean first = true;
//				String lastLevelOneHost = "";
//				String lastLevelTwoHost = "";
//				int levelOneHostNum = 0;
//				int levelTwoHostNum = 0;
//
//				host = new String(key.getHost());
//
//				if (first) {
//					domain = URLUtil.getDomainByHost(URLUtil.hostReverse(host));
//					reverseDomain = URLUtil.hostReverse(domain);
//					first = false;
//				}
//
//				if (host.length() == reverseDomain.length() && host.equals(reverseDomain)) {
//					if (!lastLevelOneHost.equals(host)) {
//						lastLevelOneHost = host;
//						levelOneHostNum++;
//						Log.info("find level one host " + host + "\t" + levelOneHostNum + "\t" + domain);
//
//					}
//
//					continue;
//				}
//
//				int t22 = host.indexOf(reverseDomain);
//				if (t22 == -1) {
//					Log.info("error host" + host + "\t" + reverseDomain);
//
//					continue;
//
//				} else {
//					if (host.length() < reverseDomain.length()) {
//						continue;
//					}
//					int levelOneDot = host.indexOf(".", reverseDomain.length() + 1);
//					if (levelOneDot == -1) {
//						if (!lastLevelOneHost.equals(host)) {
//							lastLevelOneHost = host;
//							levelOneHostNum++;
//							Log.info("find level one host " + lastLevelOneHost + "\t" + levelOneHostNum + "\t" + domain);
//
//						}
//
//					} else {
//						String levelOneHost = host.substring(0, levelOneDot);
//						if (!levelOneHost.equals(lastLevelOneHost)) {
//							lastLevelOneHost = levelOneHost;
//							levelOneHostNum++;
//							Log.info("find level one host " + lastLevelOneHost + "\tnum=" + levelOneHostNum + "\tdomain=" + domain);
//
//						}
//
//						if (host.length() > levelOneHost.length()) {
//
//							int levelTwoDot = host.indexOf(".", levelOneHost.length() + 1);
//							String levelTwoHost = "";
//							if (levelTwoDot == -1) {
//								levelTwoHost = host;
//							} else {
//								levelTwoHost = host.substring(0, levelTwoDot);
//							}
//
//							if (!levelTwoHost.equals(lastLevelOneHost)) {
//								lastLevelOneHost = levelTwoHost;
//								levelTwoHostNum++;
//								Log.info("find level two host " + levelTwoHost + "\t level one host=" + levelOneHost + "\tlevel two num=" + levelTwoHostNum + "\tdomain=" + domain);
//
//							}
//						}
//
//					}
//
//				}

				// int urlsize = key.getLength();
				// byte bsize[] = NumberUtil.convertIntToC(urlsize);
				// stream.write(bsize);
				// stream.write(key.get(), key.getOffset(), key.getLength());
				//
				// int valuesize = value.getLength();
				//
				// byte vsize[] = NumberUtil.convertIntToShortC(valuesize);
				// stream.write(vsize);
				// stream.write(value.get(), value.getOffset(),
				// value.getLength());

				// System.out.println("score=" + key);
				// MD5 md5 = new MD5(key.get(), MD5Len.eight, 0);
				// byte v[] = value.getBytes();
				// i++;
				// url = new String(v, 0, value.getLength());
				// System.out.println("url=" + url + "\t" +
				// md5.getHexString(true));
				// int pack = NumberUtil.readInt(v, 1);
				// int offset = 5;
				// for (int k = 0; k < pack; k++) {
				// int packLen = NumberUtil.readInt(v, offset);
				// offset += 4;
				// offset += packLen;
				// }
				// MD5 v=new MD5(value.getBytes(),MD5Len.eight,0);
				// if (!tset.contains(md5))
				// System.out.println(md5.getHexString(true) + "\t" +
				// key.getLength()+"\t"+v.getHexString(true));
				// else
				// continue;

				// byte lineBytes[] = value.getBytes();
				/*
				 * MD5 md5 = new MD5(lineBytes, MD5Len.eight, 0);
				 * 
				 * new_url_count = NumberUtil.readInt(lineBytes, 8);
				 * modify_count = NumberUtil.readInt(lineBytes, 12); error_count
				 * = NumberUtil.readInt(lineBytes, 16); urlLen =
				 * NumberUtil.readInt(lineBytes, 36); //
				 * System.out.println("urlLen=" + urlLen); url = new
				 * String(lineBytes, 40, urlLen); if
				 * (md5.getHexString().equals(md.getHexString())) {
				 * System.out.println("score=" + key + "md5=" +
				 * md5.getHexString() + "\tnew_url_count=" + new_url_count +
				 * "\tmodify_count=" + modify_count + "\terror_count=" +
				 * error_count + " url=" + url); i++; } else { j++; }
				 */

				// t++;
				// MD5 md5 = new MD5(key.getBytes(), MD5Len.eight, 0);
				// System.out.println(md5.getHexString(true) + "\t" +
				// lineBytes.length);
				// int length = lineBytes.length;
				// int len = 0;
				// int times = 0;
				// int packet = NumberUtil.readInt(lineBytes, 1);
				// System.out.println("packet=" + packet);
				// Calendar cal = new GregorianCalendar();
				// int day = cal.get(Calendar.DAY_OF_MONTH);
				// System.out.println("first=" + lineBytes[0]);
				// for (int offset = 5; offset + 4 < length && times < packet;)
				// {
				// len = NumberUtil.readInt(lineBytes, offset);
				// System.out.println("offset=" + offset + "len=" + len);
				// offset += 4;
				// System.out.println("value=" + new String(lineBytes, offset,
				// len));
				// offset += len;
				// times++;
				// }

				// score = *(float *) (key.c_str());
				// oldfinger = *(uint64_t *) (v);
				// newURLToday = *(int *) (v + 8);
				// modifyCount = *(int *) (v + 12);
				// errorCount = *(int *) (v + 16);
				// newURLBitmap = *(uint *) (v + 20);
				// modifyBitmap = *(uint *) (v + 24);
				//
				// lastModifyTime = *(time_t *) (v + 28);
				// urlLen = *(int *) (v + 28 + sizeof(time_t));
				// url = value.substr(40, urlLen);

				/*
				 * urlLen = NumberUtil.readInt(lineBytes, 36); url = new
				 * String(lineBytes, 40, urlLen); System.out.println("urlLen=" +
				 * urlLen + " url=" + url); int contentLen =
				 * NumberUtil.readInt(lineBytes, 40 + urlLen); String content =
				 * new String(lineBytes, contentLen / 3, 40 + urlLen + 4);
				 */
				// System.out.println("urlLen=" + urlLen + " url=" + url +
				// " contentLen=" + contentLen + " content ");
				// String syncSeen = reader.syncSeen() ? "*" : "";
				// System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
				// key, value);
				// position = reader.getPosition(); // beginning of next record
				// value.clear();
			}
			stream.close();
			System.out.println("i=" + i + " j=" + j);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
