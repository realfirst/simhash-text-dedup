package com.zhongsou.spider.hbase.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.hadoop.ClawerConvert;
import com.zhongsou.spider.hadoop.FileInfo;
import com.zhongsou.spider.hadoop.HDFSUtil;

public class ClawerConvertTest {
	String inputPath;
	String outputPath;

	public ClawerConvertTest(String filePath, String outputPath) {
		this.inputPath = filePath;
		this.outputPath = outputPath;
	}

	public void convert() {
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(this.inputPath), conf);
			Path path = new Path(this.inputPath);
			List<FileInfo> infoList = new LinkedList<FileInfo>();
			HDFSUtil.listFile(fs, this.inputPath, infoList, false);
			BufferedWriter stream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(this.outputPath))));
			SequenceFile.Reader reader = null;

			System.out.println("list size=" + infoList.size());
			ClawerConvert converter = new ClawerConvert();
			long start, end;
			long begin = System.currentTimeMillis();
			int sum = 0;
			outer: for (FileInfo info : infoList) {
				System.out.println("process:" + info.getPath());
				reader = new SequenceFile.Reader(fs, new Path(info.getPath()), conf);
				Text k = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				Text v = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				while (reader.next(k, v)) {
					sum++;
					if (sum % 1000 == 0) {
						System.out.println("sum=" + sum);
					}
					byte[] b = v.getBytes();
					byte[] newContent = null;
					// 下载成功
					if (b[0] == '1') {
						start = System.currentTimeMillis();
						int packetNum = NumberUtil.readInt(b, 1);
						int packetLen = 0, offset = 5;
						int headerLen = 0, contentLen = 0;
						int urlLen = 0;
						int urloffset = 0;
						if (packetNum == 3) {
							for (int i = 0; i < 2; i++) {
								if (i == 1) {
									urlLen = NumberUtil.readInt(b, offset);
									urloffset = offset + 4;
								}
								packetLen = NumberUtil.readInt(b, offset);
								offset += packetLen + 4;
							}
							contentLen = NumberUtil.readInt(b, offset);
							if (contentLen < 0) {

							}
							converter.reset(k.getBytes(), b, headerLen, contentLen, offset + 4, offset + 4);

						} else if (packetNum == 4) {
							for (int i = 0; i < 2; i++) {
								if (i == 1) {
									urlLen = NumberUtil.readInt(b, offset);
									urloffset = offset + 4;
								}
								packetLen = NumberUtil.readInt(b, offset);
								offset += packetLen + 4;
							}

							headerLen = NumberUtil.readInt(b, offset);
							contentLen = NumberUtil.readInt(b, offset + 4 + headerLen);
							converter.reset(k.getBytes(), b, headerLen, contentLen, offset + 4 + headerLen + 4, offset + 4);
						} else {

						}
						newContent = converter.convert();
						end = System.currentTimeMillis();

						if (newContent != null) {
							stream.write("true\t" + new String(b, urloffset, urlLen) + "\t" + (end - start) + "\r\n");
						} else {
							// 转化失败
							// LOG.error("convert data format failed,url=" + new
							// String(b, urloffset,
							// urlLen)+"\turllen="+urlLen+"\toffset="+urloffset+"\tlength="+v.getLength());
							stream.write("false\t" + new String(b, urloffset, urlLen) + "\t" + (end - start) + "\r\n");
							// v.set(b, 0, v.getLength());
						}

					}
				}
				IOUtils.closeStream(reader);

			}
			stream.write("All consume=" + (System.currentTimeMillis() - begin) + "\r\n");
			stream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {
		if (args.length < 2) {
			System.out
					.println("usage: java -classpath $CLASSPATH:/home/kaifa/xiqi/spider_common/target/spider_common-0.0.1-SNAPSHOT.jar:/home/kaifa/xiqi/spider_common/target/test-classes -Djava.library.path=/home/kaifa/Appstorage/hadoop-1.0.3/lib/native/Linux-amd64-64 com.zhongsou.spider.hbase.test.ClawerConvertTest hdfs://hadoop-master-83:9900/user/kaifa/clawer_output/clawer_201210161957 /home/kaifa/xiqi/convert.res");
			System.exit(0);
		}
		ClawerConvertTest d = new ClawerConvertTest(args[0], args[1]);
		d.convert();

	}

}
