package com.zhongsou.spider.hbase.test;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import com.zhongsou.spider.hbase.mapreduce.SendWebPage;

public class SendpageDataTest {

	public SendpageDataTest() {

	}

	static void run() {

		String fileName = "/user/kaifa/test/aa";
		String fileNameb = "/user/kaifa/test/bb";

		String inputFile = "";

		FileSystem fs;
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
			String t =  "a";
			Path lpath = new Path(fileName);
			SequenceFile.Writer loadWriter = SequenceFile.createWriter(fs, conf, lpath, ImmutableBytesWritable.class, NullWritable.class);

			Path tpath = new Path(fileNameb);

			SequenceFile.Writer loadWriter2 = SequenceFile.createWriter(fs, conf, tpath, ImmutableBytesWritable.class, NullWritable.class);

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(t), conf);

			ImmutableBytesWritable key = new ImmutableBytesWritable();

			ImmutableBytesWritable v = new ImmutableBytesWritable();
			int i = 0;
			Random rand = new Random();
			while (reader.next(key, v) && i < 100000) {
				int ra = rand.nextInt(3);
				if (ra == 0) {
					loadWriter.append(key, NullWritable.get());
				} else if (ra == 1) {
					loadWriter2.append(key, NullWritable.get());
				}
			}

			loadWriter.close();
			loadWriter2.close();
			reader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {
		SendpageDataTest.run();
	}

}
