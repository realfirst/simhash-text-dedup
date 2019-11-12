package com.zhongsou.spider.hbase.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.MD5.MD5Len;
import com.zhongsou.spider.common.util.NumberUtil;

public class FingerTest {
	Map<ImmutableBytesWritable, LinkedList<ImmutableBytesWritable>> hashMap = new HashMap<ImmutableBytesWritable, LinkedList<ImmutableBytesWritable>>();

	ArrayList<LinkedList<byte[]>> firstByteList = new ArrayList<LinkedList<byte[]>>(65536);
	ArrayList<LinkedList<byte[]>> secondByteList = new ArrayList<LinkedList<byte[]>>(65536);
	ArrayList<LinkedList<byte[]>> thirdByteList = new ArrayList<LinkedList<byte[]>>(65536);
	ArrayList<LinkedList<byte[]>> fourByteList = new ArrayList<LinkedList<byte[]>>(65536);

	private void createTable(Text key, Text value) {
		int i = Bytes.toInt(key.getBytes(), 0, 2);
		if (i < 0) {
			System.out.println(i);
		}

	}

	public FingerTest(String fileName) {
		Configuration conf = new Configuration();

		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(fileName), conf);
			Path path = new Path(fileName);
			SequenceFile.Reader reader = null;
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = new Text();
			Text value = new Text();
			while (reader.next(key, value)) {
				byte a[] = new byte[8];
				System.arraycopy(key.getBytes(), 0, a, 0, key.getLength());
				ImmutableBytesWritable row = new ImmutableBytesWritable();
				row.set(a);
				byte b[] = new byte[8];
				System.arraycopy(value.getBytes(), 0, b, 0, key.getLength());
				ImmutableBytesWritable finger = new ImmutableBytesWritable();
				finger.set(b);
				LinkedList<ImmutableBytesWritable> list = this.hashMap.get(finger);
				if (list == null) {
					list = new LinkedList<ImmutableBytesWritable>();
					list.add(row);
					this.hashMap.put(finger, list);
				} else {
					list.add(row);
				}
				// createTable(key, value);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		File f = new File("/home/dape/a.txt");
		BufferedWriter writer = null;
		int i=0;
		try {
			writer = new BufferedWriter(new FileWriter(f));
			
			for (Map.Entry<ImmutableBytesWritable, LinkedList<ImmutableBytesWritable>> kv : this.hashMap.entrySet()) {
				if (kv.getValue().size() > 2) {
					i++;
					// System.out.println(kv.getValue().size());
					writer.write(kv.getValue().size() + "\t");
					MD5 md5;
					try {
						md5 = new MD5(kv.getKey().get(), MD5Len.eight, 0);
//						System.out.print(md5.getHexString(true) + "\t");
						writer.write(md5.getHexString(true) + "\t");
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}

					for (ImmutableBytesWritable temp : kv.getValue()) {
						// System.out.println(temp.getLength());
						try {
							md5 = new MD5(temp.get(), MD5Len.eight, 0);
							writer.write(md5.getHexString(true) + "\t");
//							System.out.print(md5.getHexString(true) + "\t");
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					writer.write("\r\n");
					// System.out.println();
				}
			}
			
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("load t" + this.hashMap.size()+"\tduplicate i="+i);

	}

	public static void main(String args[]) {
		FingerTest fingerTest = new FingerTest("file:////home/dape/finger.seq");
		String a="70f56d4e79f9af4870f56d4e79f9af480000000001000000000000000000000000000000a62d7f5000000000";
		byte b[]=StringUtils.hexStringToByte(a);
		
		int newURLToday = 0, modifyCount = 0, errorCount = 0, newURLBitmap = 0, modifyBitmap = 0;
		long lastModifyTime = 0;
		long nextDownloadTime = 0;
		float pageRank = 0;

		byte metaBytes[] = b;
		boolean isChanged = Bytes.compareTo(metaBytes, 0, 8, metaBytes, 16, 8) == 0 ? false : true;
		lastModifyTime = NumberUtil.readLong(metaBytes, 36);

		newURLToday = NumberUtil.readInt(metaBytes, 16);
		modifyCount = NumberUtil.readInt(metaBytes, 20);
		
		errorCount = NumberUtil.readInt(metaBytes, 24);
		newURLBitmap = NumberUtil.readInt(metaBytes, 28);
		modifyBitmap = NumberUtil.readInt(metaBytes, 32);
		DateFormat format=new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(isChanged+"\t"+format.format(new java.util.Date(lastModifyTime*1000))+"\t"+newURLToday+"\t"+modifyCount+"\t"+errorCount+"\t");
	}

}
