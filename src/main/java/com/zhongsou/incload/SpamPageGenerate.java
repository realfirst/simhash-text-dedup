package com.zhongsou.incload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import com.zhongsou.spider.hadoop.jobcontrol.SelectAndSendJob;
import com.zhongsou.spider.hadoop.jobcontrol.SpiderJob;

public class SpamPageGenerate {

	private Map<ImmutableBytesWritable, ImmutableBytesWritable> urlidFingerMap = new HashMap<ImmutableBytesWritable, ImmutableBytesWritable>();
	private Map<ImmutableBytesWritable, Integer> fingerCounterMap = new HashMap<ImmutableBytesWritable, Integer>();

	private String newLoadURLAndFingerFile;
	private String spamFile;
	private String input;

	public SpamPageGenerate(String urlAndFingerFolder, String seqTime) {
		newLoadURLAndFingerFile = SpiderJob.ROOT_DIR + "/" + SpiderJob.NEW_URL_AND_FINGER_DIR + "/" + seqTime;
		spamFile = SpiderJob.ROOT_DIR + "/" + SpiderJob.SAME_FINGER_DOCID_DELETE_DIR + "/" + seqTime;
		this.input = urlAndFingerFolder;
	}

	public int process(Configuration conf, FileSystem fs) {
		int res = -1;

		SequenceFile.Reader reader = null;
		SequenceFile.Writer normalPageWriter = null;
		SequenceFile.Writer spamPageWriter = null;

		int normalCount = 0;
		int spamCount = 0;

		try {
			normalPageWriter = new SequenceFile.Writer(fs, conf, new Path(newLoadURLAndFingerFile), ImmutableBytesWritable.class, ImmutableBytesWritable.class);
			spamPageWriter = new SequenceFile.Writer(fs, conf, new Path(spamFile), ImmutableBytesWritable.class, ImmutableBytesWritable.class);
			List<Path> paths = getPaths(input);
			for (Path path : paths) {
				reader = new SequenceFile.Reader(fs, path, conf);
				ImmutableBytesWritable urlid = (ImmutableBytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				ImmutableBytesWritable finger = (ImmutableBytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				ImmutableBytesWritable ibFinger =  new ImmutableBytesWritable(new byte[0]);
				ImmutableBytesWritable ibUrlid = new ImmutableBytesWritable(new byte[0]);

				while (reader.next(urlid, finger)) {
					ibFinger=new ImmutableBytesWritable(finger.copyBytes());
					ibUrlid=new ImmutableBytesWritable(urlid.copyBytes());

					if (fingerCounterMap.keySet().contains(ibFinger)) {
						// 把第二次出现的docid加入到map之中,最后统计相同指纹出现小于1000次的指纹写入正常文件
						urlidFingerMap.put(ibUrlid, ibFinger);
						fingerCounterMap.put(ibFinger, fingerCounterMap.get(ibFinger) + 1);
					} else {
						// 第一次出现的指纹马上写入正常的文件，这样保证至少保留一个出现次数大于1000的指纹
						normalPageWriter.append(ibUrlid, ibFinger);
						normalCount++;
						fingerCounterMap.put(ibFinger, 1);
					}
				}
				IOUtils.closeStream(reader);
			}

			ImmutableBytesWritable f = null;
			for (ImmutableBytesWritable u : urlidFingerMap.keySet()) {
				f = urlidFingerMap.get(u);
				if (fingerCounterMap.get(f) > 1000) {
					spamPageWriter.append(u, f);
					spamCount++;
				} else {
					normalPageWriter.append(u, f);
					normalCount++;
				}
			}
			res = spamCount;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(normalPageWriter);
			IOUtils.closeStream(spamPageWriter);
			SelectAndSendJob.logger.info("delete spam finished! normal:\t" + normalCount + "\tspam:" + spamCount);
		}
		return res;
	}

	private static List<Path> getPaths(String rPath) {
		List<Path> pl = new ArrayList<Path>();
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(rPath));
			for (int i = 0; i < status.length; i++) {
				pl.add(status[i].getPath());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return pl;
	}

	private static final long MEGABYTE = 1024L * 1024L;

	public static long bytesToMegabytes(long bytes) {
		return bytes / MEGABYTE;
	}

	public static void main(String[] args) throws IOException {
		Runtime runtime = Runtime.getRuntime();
		long start = runtime.totalMemory();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		new SpamPageGenerate("/user/kaifa/new-urlid-finger/9", "20121128183207").process(conf, fs);
		long finish = runtime.totalMemory();
		// // runtime.gc();
		// System.out.println(bytesToMegabytes(runtime.totalMemory()));
		// System.out.println(bytesToMegabytes(runtime.freeMemory()));
		// System.out.println(bytesToMegabytes(runtime.maxMemory()));
		long memory = start - finish;
		System.out.println("used memory is bytes: " + memory);
		System.out.println("used memory is megabytes: " + bytesToMegabytes(memory));
		while (true) {
			System.out.println("sleep");
			try {
				Thread.sleep(1000 * 10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}
