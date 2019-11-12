package com.zhongsou.spider.hbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.zhongsou.spider.hadoop.HDFSUtil;

public class DownloadHDFS {

	public static void main(String args[]) {
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			HDFSUtil.download(fs, "file:////home/dape/error_url", "hdfs://hadoop-master-83:9900/user/kaifa/error_url/");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
