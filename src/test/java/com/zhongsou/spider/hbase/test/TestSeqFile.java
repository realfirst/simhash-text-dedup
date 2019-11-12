package com.zhongsou.spider.hbase.test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import com.zhongsou.incload.DupPair;
import com.zhongsou.incload.KeyValuePair;
import com.zhongsou.incload.PageNode;
import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.hadoop.FileInfo;
import com.zhongsou.spider.hadoop.HDFSUtil;

public class TestSeqFile {

	// public static void writeFile(String fileName) {
	// List<DupPair> values = new ArrayList<DupPair>();
	// PageNode node1 = new PageNode();
	// PageNode d1n1 = new PageNode(new
	// BytesWritable(StringUtils.hexStringToByte("a3c216cc058ad8ba")), new
	// BytesWritable(StringUtils.hexStringToByte("5f412f17e0b995e7")), new
	// BytesWritable(
	// StringUtils.hexStringToByte("3ea94d20")), new
	// BytesWritable(StringUtils.hexStringToByte("00000000")), new
	// BytesWritable(StringUtils.hexStringToByte("00000000")));
	// PageNode d1n2 = null;
	// DupPair d1 = new DupPair(d1n1, d1n2);
	// values.add(d1);
	//
	// PageNode d2n1 = null;
	// PageNode d2n2 = new PageNode(new
	// BytesWritable(StringUtils.hexStringToByte("000000b4e8e1cab2")), new
	// BytesWritable(StringUtils.hexStringToByte("5f412f17e0b995e7")), new
	// BytesWritable(
	// StringUtils.hexStringToByte("3ea94d20")), new
	// BytesWritable(StringUtils.hexStringToByte("00000000")), new
	// BytesWritable(StringUtils.hexStringToByte("00000000")));
	// DupPair d2 = new DupPair(d2n1, d2n2);
	// values.add(d2);
	//
	// Configuration conf = new Configuration();
	// FileSystem fs;
	// try {
	// fs = FileSystem.get(conf);
	// Path path = new Path(fileName);
	// SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
	// ImmutableBytesWritable.class, DupPair.class);
	// ImmutableBytesWritable key = new ImmutableBytesWritable();
	// byte a[] = new byte[1];
	// a[0] = 1;
	// key.set(a);
	// for (DupPair value : values) {
	// Log.info(value.toString());
	// try {
	// writer.append(key, value);
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// IOUtils.closeStream(writer);
	// SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
	// while (reader.next(key, d1)) {
	// Log.info("111-" + d1.toString());
	// }
	// } catch (IOException e1) {
	// // TODO Auto-generated catch block
	// e1.printStackTrace();
	// }
	//
	// }

	public static void readSequenceFile(String rPath) throws IOException {

		SequenceFile.Reader reader = null;

		Character prefix = null;
		Map<Character, Integer> prefixCountMap = null;
		try {

			ImmutableBytesWritable mapKey = null;
			byte[] mapKeyArray = null;
			ImmutableBytesWritable mapValue = null;
			byte[] mapValueArray = null;

			List<FileInfo> list = new LinkedList<FileInfo>();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			HDFSUtil.listFile(fs, rPath, list, false);
			BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(new File("/home/dape/a.txt")));
			for (FileInfo info : list) {
				if (reader != null)
					reader.close();
				System.out.println(info.getPath());
				reader = new SequenceFile.Reader(fs, new Path(info.getPath()), conf); // Reads
				// key/value
				// pairs
				// from
				// a sequence-format file
				ImmutableBytesWritable key = (ImmutableBytesWritable) // key --
				// urlid
				ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				DupPair pair = (DupPair) // value
											// --
											// finger
				ReflectionUtils.newInstance(reader.getValueClass(), conf);

				while (reader.next(key, pair)) {

					PageNode n1 = pair.getN1();
					PageNode n2 = pair.getN2();
					stream.write(NumberUtil.getHexString(key.get()).getBytes());
					stream.write("\t".getBytes());
					if (n1.getUrlid() != null) {
						stream.write(NumberUtil.getHexString(n1.getUrlid()).getBytes());
						stream.write("\t".getBytes());
						stream.write(NumberUtil.getHexString(n1.getFinger()).getBytes());
					} else {
						stream.write("\t".getBytes());
						stream.write("\t".getBytes());
						stream.write("\t".getBytes());
					}
					stream.write("\t".getBytes());
					if (n2.getUrlid() != null) {
						stream.write(NumberUtil.getHexString(n2.getUrlid()).getBytes());
						stream.write("\t".getBytes());
						stream.write(NumberUtil.getHexString(n2.getFinger()).getBytes());
					} else {
						stream.write("\t".getBytes());
						stream.write("\t".getBytes());
						stream.write("\t".getBytes());
					}
					stream.write("\n".getBytes());
				}

			}
			stream.close();

		} finally {
			IOUtils.closeStream(reader);
		}
	}

	public static void main(String args[]) {
		try {
		//	TestSeqFile.readSequenceFile("/spider_data_old/generate_dup_pair_output/20121128183207");
			byte a[]=new byte[8];
			System.out.println(NumberUtil.getHexString(a));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
