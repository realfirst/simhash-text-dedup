package com.zhongsou.spider.hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

public class TimeMultipleOutoutFormat<K, V> extends MultipleOutputFormat<K, V> {
	Logger logger = Logger.getLogger(TimeMultipleOutoutFormat.class);
	private SequenceFileOutputFormat<K, V> output = null;
	private long lastTime = 0;
	public static int INTERVAL = 1000 * 60 * 15;
	private SimpleDateFormat format;
	private String lastFileName = null;

	public TimeMultipleOutoutFormat() {
		super();
		format = new java.text.SimpleDateFormat("yyyyMMddHHmm");
	}

	public RecordWriter<K, V> getRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3) throws IOException {

		final FileSystem myFS = fs;
		final String myName = generateLeafFileName(name);
		final JobConf myJob = job;
		final Progressable myProgressable = arg3;

		return new RecordWriter<K, V>() {

			// a cache storing the record writers for different output files.
			// TreeMap<String, RecordWriter<K, V>> recordWriters = new
			// TreeMap<String, RecordWriter<K, V>>();
			HashSet<String> writerSet = new HashSet<String>();
			RecordWriter<K, V> currentWriter = null;

			public void write(K key, V value) throws IOException {

				// get the file name based on the key
				String keyBasedPath = generateFileNameForKeyValue(key, value, myName);

				// get the file name based on the input file name
				String finalPath = getInputFileBasedOutputFileName(myJob, keyBasedPath);

				// get the actual key
				K actualKey = generateActualKey(key, value);
				V actualValue = generateActualValue(key, value);

				if (currentWriter == null) {
					currentWriter = getBaseRecordWriter(myFS, myJob, finalPath, myProgressable);
					this.writerSet.add(finalPath);
				} else {
					// get a new filename,we close this record writer,and then
					// add this
					// path to set
					if (!this.writerSet.contains(finalPath)) {
						this.currentWriter.close(null);
						logger.info("create a new file path =" + finalPath);
						currentWriter = getBaseRecordWriter(myFS, myJob, finalPath, myProgressable);
						this.writerSet.add(finalPath);
					}
				}
				currentWriter.write(actualKey, actualValue);
			};

			public void close(Reporter reporter) throws IOException {
				if (currentWriter != null) {
					this.currentWriter.close(reporter);
				}
				this.writerSet.clear();
			};
		};
	}

	@Override
	protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3) throws IOException {
		// TODO Auto-generated method stub
		if (output == null) {
			output = new SequenceFileOutputFormat<K, V>();
		}
		if (INTERVAL == 1000 * 60 * 15) {
			INTERVAL = job.getInt("multiple.file.name.interval", 1000 * 60 * 15);
		}
		return output.getRecordWriter(fs, job, name, arg3);
	}

	@Override
	protected String generateFileNameForKeyValue(K key, V value, String name) {
		// TODO Auto-generated method stub
		long now = System.currentTimeMillis();
		long currM = now / INTERVAL;
		if (currM == lastTime && lastFileName != null) {
			return lastFileName;
		} else {
			lastFileName = "clawer_" + format.format(new java.util.Date(currM * INTERVAL)) + "/" + name;
			this.lastTime = currM;
			return lastFileName;
		}
	}

	public static void main(String args[]) {
		SimpleDateFormat format = new java.text.SimpleDateFormat("yyyyMMddHHmm");
		long t = System.currentTimeMillis();
		t = t / INTERVAL;
		t *= INTERVAL;
		String lastFileName = "clawer_r_" + format.format(new java.util.Date(t));
		System.out.println(lastFileName);
	}
}
