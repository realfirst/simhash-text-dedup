package com.zhongsou.spider.hbase.test;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.hadoop.FileInfo;
import com.zhongsou.spider.hadoop.HDFSUtil;

public class ConvertSeqFile {
	static {
		System.loadLibrary("snappy");
		System.loadLibrary("hadoopsnappy");
	}
	String inputPath;
	String outputPath;

	public ConvertSeqFile(String input, String output) {
		this.inputPath = input;
		this.outputPath = output;
	}

	public void convert() {
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(this.inputPath), conf);
			Path path = new Path(this.inputPath);
			List<FileInfo> infoList = new LinkedList<FileInfo>();
			HDFSUtil.listFile(fs, this.inputPath, infoList, false);
			SequenceFile.Writer stream = SequenceFile.createWriter(fs, conf, new Path(this.outputPath), ImmutableBytesWritable.class, ImmutableBytesWritable.class);
			SequenceFile.Reader reader = null;
			int i = 0;
			byte begin[] = "#*#*#*#*#*#*#WEB PAGE URL\t".getBytes();
			byte end[] = "#*#*#*#*#*#*#END OF ONE PAGE".getBytes();
			System.out.println("list size=" + infoList.size());

			outer: for (FileInfo info : infoList) {
				System.out.println("process:" + info.getPath());
				reader = new SequenceFile.Reader(fs, new Path(info.getPath()), conf);
				ImmutableBytesWritable key = (ImmutableBytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				ImmutableBytesWritable value = (ImmutableBytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				ImmutableBytesWritable docid = new ImmutableBytesWritable();
				while (reader.next(key, value)) {
					// int urlsize = key.getLength();
					// byte bsize[] = NumberUtil.convertIntToC(urlsize);
					// // stream.write(bsize);
					// // stream.write(key.get(), key.getOffset(),
					// // key.getLength());
					// stream.write(begin);
					// stream.write(key.get(), key.getOffset(),
					// key.getLength());
					// int valuesize = value.getLength();
					//
					// byte vsize[] = NumberUtil.convertIntToShortC(valuesize);
					// stream.write(vsize);
					// stream.write(value.get(), value.getOffset(),
					// value.getLength());
					// stream.write(end);
					// i++;
					if (i % 1000 == 0)
						System.out.println("read i=" + i);
					i++;
					// if (i > 100000)
					// break outer;
					MD5 md5 = MD5.digest8(key.get(), key.getOffset(), key.getLength());
					docid.set(md5.getDigest());
					stream.append(docid, key);

				}
				IOUtils.closeStream(reader);

			}
			IOUtils.closeStream(stream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {
		if (args.length < 2) {
			System.out
					.println("usage: java -classpath $CLASSPATH:/home/hadoop/xiqi/spider_common/target/spider_common-0.0.1-SNAPSHOT.jar:/home/hadoop/xiqi/spider_common/target/test-classes -Djava.library.path=/usr/local/hadoop/lib/native/Linux-amd64-64/ com.zhongsou.spider.hbase.test.ConvertSeqFile hdfs://hadoop-master:9000/user/hadoop/source_htm/ file:////home/hadoop/xiqi/output.res");
			System.exit(0);
		}
		ConvertSeqFile file = new ConvertSeqFile(args[0], args[1]);
		file.convert();
	}

}
