package com.zhongsou.spider.hbase.test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.NumberUtil;

public class URLAndContentValidator {
	static {
		System.loadLibrary("snappy");
	}
	private String urlPath;
	private String contentPath;

	int newClaweredURL = 0;
	int oldURL = 0;
	HashSet<MD5> urlset;
	int succeedDownload = 0;
	int missing = 0;

	public static final String FILE_PREFIX = "file://";

	public URLAndContentValidator(String urlPath, String contentPath) {
		this.urlPath = urlPath;
		this.contentPath = contentPath;
		this.urlset = new HashSet<MD5>();
		this.readURL();
	}

	public boolean checkContent() {
		File file = new File(this.contentPath);
		File[] files = file.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				// TODO Auto-generated method stub
				if (name.startsWith("clawer_"))
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
				Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				System.out.println("block compressed" + reader.isBlockCompressed() + "\tcompressed=" + reader.isCompressed());
				while (reader.next(key, value)) {
					byte lineBytes[] = value.getBytes();
					// finger = value.substr(12, 8);
					// new_url_count = *(int *) (value.c_str() + 20);
					// modify_count = *(int *) (value.c_str() + 24);
					// error_count = *(int *) (value.c_str() + 28);
					// int urlLen = *(int *) (value.c_str() + 32 +
					// sizeof(time_t));
					// url = value.substr(36 + sizeof(time_t), urlLen);
					// urlScore += basicScore;

					int urlLen = NumberUtil.readInt(lineBytes, 36);
					// System.out.println("urlLen" + urlLen);
					String url = new String(lineBytes, 40, urlLen);
					// System.out.println("urlLen" + urlLen + " url=" + url);
					if (!url.equals(key.toString())) {
						System.out.println("error key doesn't equal url" + key.toString());
					}
					if (url.endsWith(".pdf") || url.endsWith(".doc")) {
						System.out.println(url);
					}
					MD5 md5 = MD5.digest8(lineBytes, 40, urlLen);
					if (!this.urlset.contains(md5)) {
						System.out.println("error,urlset doesn't contain url=" + url);
						missing++;
					} else {
						succeedDownload++;
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				IOUtils.closeStream(reader);
			}

		}
		if (missing > 0)
			return false;
		else
			return true;
	}

	public void compareURL(String fpath) {
		File file = new File(fpath);
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
					byte lineBytes[] = value.getBytes();
					// finger = value.substr(12, 8);
					// new_url_count = *(int *) (value.c_str() + 20);
					// modify_count = *(int *) (value.c_str() + 24);
					// error_count = *(int *) (value.c_str() + 28);
					// int urlLen = *(int *) (value.c_str() + 32 +
					// sizeof(time_t));
					// url = value.substr(36 + sizeof(time_t), urlLen);
					// urlScore += basicScore;

					int urlLen = NumberUtil.readInt(lineBytes, 36);
					// System.out.println("urlLen" + urlLen);
					String url = new String(lineBytes, 40, urlLen);

					// System.out.println("urlLen" + urlLen + " url=" + url);
					MD5 md5 = MD5.digest8(lineBytes, 40, urlLen);
					if (!this.urlset.contains(md5)) {
						if (key.get() > 0) {
							oldURL++;
						} else {
							newClaweredURL++;
						}
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				IOUtils.closeStream(reader);
			}

		}
	}

	private void readURL() {
		File file = new File(this.urlPath);
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
					byte lineBytes[] = value.getBytes();
					// finger = value.substr(12, 8);
					// new_url_count = *(int *) (value.c_str() + 20);
					// modify_count = *(int *) (value.c_str() + 24);
					// error_count = *(int *) (value.c_str() + 28);
					// int urlLen = *(int *) (value.c_str() + 32 +
					// sizeof(time_t));
					// url = value.substr(36 + sizeof(time_t), urlLen);
					// urlScore += basicScore;
					// System.out.println(key);
					int urlLen = NumberUtil.readInt(lineBytes, 36);
					// System.out.println("urlLen" + urlLen);

					// System.out.println("urlLen" + urlLen + " url=" + url);
					MD5 md5 = MD5.digest8(lineBytes, 40, urlLen);
					this.urlset.add(md5);

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				IOUtils.closeStream(reader);
			}

		}

	}

	public static void main(String args[]) {
		if (args.length < 2) {
			System.out.println("usage java com.zhongsou.spider.hbase.test.URLAndContentValidator -classpth=$CLASSPATH:/home/hadoop/xiqi/spider_common-0.0.1-SNAPSHOT.jar folder1 folder2");
			System.exit(0);
		}
		URLAndContentValidator p = new URLAndContentValidator(args[0], "/home/dape/clawer_201207111519");
		System.out.println(p.urlset.size());
		// System.out.println("check finished" + p.checkContent());
		p.compareURL(args[1]);
		// System.out.println("tset size=" + p.tset.size());

		System.out.println(args[1] + " contains different old url size=" + p.oldURL + "\t new url size=" + p.newClaweredURL);

	}
}
