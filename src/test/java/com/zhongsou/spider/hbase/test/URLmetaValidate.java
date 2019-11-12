package com.zhongsou.spider.hbase.test;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.MD5.MD5Len;
import com.zhongsou.spider.common.util.NumberUtil;

public class URLmetaValidate {
	public String urlfolder;
	public String urlMetaFolder;
	HashMap<ImmutableBytesWritable, String> urldocidMap = new HashMap<ImmutableBytesWritable, String>();
	public static final String FILE_PREFIX = "file://";

	public URLmetaValidate(String urlfolder, String urlMetaFolder) {
		this.urlfolder = urlfolder;
		this.urlMetaFolder = urlMetaFolder;
		this.parseDocid();
	}

	private void parseDocid() {
		File file = new File(this.urlfolder);
		File[] files = file.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				// TODO Auto-generated method stub
				if (name.startsWith("part-r"))
					return true;
				else
					return false;
			}
		});
		Configuration conf = new Configuration();

		SequenceFile.Reader reader = null;
		for (File s : files) {
			System.out.println(s.getAbsolutePath());
			String uri = FILE_PREFIX + s;
			try {
				FileSystem fs = FileSystem.get(URI.create(uri), conf);
				Path path = new Path(uri);
				reader = new SequenceFile.Reader(fs, path, conf);
				FloatWritable key = (FloatWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				while (reader.next(key, value)) {
					ImmutableBytesWritable v = new ImmutableBytesWritable();

					int packLen = 0, offset = 36;
					packLen = NumberUtil.readInt(value.getBytes(), offset);
					offset += 4;

					MD5 md5 = MD5.digest8(value.getBytes(), offset, packLen);
					String url = new String(value.getBytes(), offset, packLen);
					// System.out.println("md5=" + md5.getHexString() + "\turl="
					// + url);

					v.set(md5.getDigest());
					this.urldocidMap.put(v, url);
				}
				reader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("docid map size=" + this.urldocidMap.size());

	}

	public void validate() {

		File file = new File(this.urlMetaFolder);
		File[] files = file.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				// TODO Auto-generated method stub
				if (name.startsWith("URLMeta-r"))
					return true;
				else
					return false;
			}
		});
		Configuration conf = new Configuration();

		SequenceFile.Reader reader = null;
		int i = 0, j = 0, k = 0;
		for (File s : files) {
			System.out.println(s.getAbsolutePath());
			String uri = FILE_PREFIX + s;
			try {
				FileSystem fs = FileSystem.get(URI.create(uri), conf);
				Path path = new Path(uri);
				reader = new SequenceFile.Reader(fs, path, conf);
				ImmutableBytesWritable key = (ImmutableBytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				ImmutableBytesWritable value = (ImmutableBytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				while (reader.next(key, value)) {

					if (!this.urldocidMap.containsKey(key)) {
						MD5 md5 = new MD5(key.get(), MD5Len.eight, 0);
						System.out.println("url md5=" + md5.getHexString());
						if (md5.getHexString().equals("ee6e5271da9ec16b")) {
							System.out.println("status=" + md5.getHexString());
						}
						j++;
					} else {
						MD5 md5 = new MD5(key.get(), MD5Len.eight, 0);
						// System.out.println(this.urldocidMap.get(key));
						if (this.urldocidMap.get(key).equals("http://qiannan.51chudui.com/Class_434_1_0/")) {
							System.out.println(md5.getHexString());
						}
						k++;
					}
					i++;
				}
				reader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("url meta=" + i + "existed k=" + k + " not existed=j=" + j);

	}

	public String getURL(String hex) {
		byte a[] = StringUtils.hexStringToByte(hex);
		ImmutableBytesWritable url = new ImmutableBytesWritable();
		url.set(a);
		return this.urldocidMap.get(url);
	}

	public static void main(String args[]) {
		URLmetaValidate validate = new URLmetaValidate("/home/dape/sort_2012090300", "/home/dape/parsed_urlmeta");
		validate.validate();
		System.out.println(validate.getURL("05b72a33a0f1df89"));
	}

}
