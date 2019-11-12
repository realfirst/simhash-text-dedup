package com.zhongsou.spider.hbase.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.MD5.MD5Len;
import com.zhongsou.spider.common.util.NumberUtil;

public class CheckHbaseURL {
	HTable table;
	File folder;
	Map<ImmutableBytesWritable, Text> tmap = new HashMap<ImmutableBytesWritable, Text>();

	SequenceFile.Writer writer = null;

	public void getURL(List<Get> tlist) {
		byte f[] = Bytes.toBytes("F");
		byte u[] = Bytes.toBytes("u");
		try {
			Result t[] = table.get(tlist);
			if (t.length != tlist.size()) {
				System.out.println("get size error result length=" + t.length + "\t tlist size=" + tlist.size());
			}
			int i = 0;
			for (Result result : t) {
				byte a[] = result.getValue(f, u);
				if (a == null) {
					if (result.getRow() == null) {
						continue;
					}
					MD5 md5 = new MD5(result.getRow(), MD5Len.eight, 0);
					System.out.println("url is null row key=" + md5.getHexString(true));
				} else {
					String url = new String(a);
					if (url.startsWith("http")) {
						// System.out.println("url=" + url);
						this.tmap.remove(new ImmutableBytesWritable(result.getRow()));
						i++;
					} else {
						System.out.println("url=" + url);
					}
				}
			}
			if (i != tlist.size()) {
				System.out.println("result size " + i + " is not equal to " + tlist.size() + " tmap size=" + tmap.size());
				for (Map.Entry<ImmutableBytesWritable, Text> kv : tmap.entrySet()) {
					writer.append(kv.getKey(), kv.getValue());
				}

			} else {
				System.out.println("check finish one list " + i);
			}
			tlist.clear();
			tmap.clear();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public CheckHbaseURL(String file) {
		folder = new File(file);
		Configuration conf = HBaseConfiguration.create();
		try {
			table = new HTable(conf, "webDB");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			FileSystem fs = FileSystem.get(conf);
			writer = SequenceFile.createWriter(fs, conf, new Path("file:////home/dape/missing_url_2.seq"), ImmutableBytesWritable.class, Text.class);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void check() {
		List<Get> tlist = new ArrayList<Get>();
		File f[] = folder.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				// TODO Auto-generated method stub
				if (name.startsWith("part"))
					return true;
				else
					return false;
			}

		});
		Configuration conf = new Configuration();
		FileSystem fs;
		FloatWritable key = new FloatWritable();
		Text value = new Text();
		try {
			fs = FileSystem.get(conf);
			SequenceFile.Reader reader = null;
			for (File tempFile : f) {
				System.out.println(tempFile.getAbsolutePath());
				reader = new SequenceFile.Reader(fs, new Path("file:////" + tempFile.getAbsolutePath()), conf);
				while (reader.next(key, value)) {
					byte a[] = value.getBytes();
					int urlLen = NumberUtil.readInt(a, 36);
					MD5 md5 = MD5.digest8(a, 40, urlLen);
					// System.out.println(new String(a, 40, urlLen));
					Get get = new Get(md5.getDigest());
					get.addColumn(Bytes.toBytes("F"), Bytes.toBytes("u"));
					Text text = new Text();
					text.set(a, 40, urlLen);
					tmap.put(new ImmutableBytesWritable(md5.getDigest()), text);
					tlist.add(get);
					if (tlist.size() > 1000) {
						this.getURL(tlist);
					}

				}
				IOUtils.closeStream(reader);

			}
			IOUtils.closeStream(this.writer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {
//		CheckHbaseURL check = new CheckHbaseURL("/home/dape/sort_outuput_2");
//		check.check();
		
		byte []a=new byte[1024*1024*2];
		long start=System.currentTimeMillis();
		for(int i=0;i<1000;i++)
		{
			Arrays.fill(a, (byte )'0');
		}
		long end=System.currentTimeMillis();
		
		
		System.out.println("time="+(end-start));
	}
}
