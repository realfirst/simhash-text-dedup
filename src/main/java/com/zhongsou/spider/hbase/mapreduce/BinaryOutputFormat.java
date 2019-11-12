package com.zhongsou.spider.hbase.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class BinaryOutputFormat<K, V> extends TextOutputFormat<K, V> {

	class MyLineRecordWriter<K, V> extends LineRecordWriter {
		private static final String utf8 = "UTF-8";

		public MyLineRecordWriter(DataOutputStream out, String keyValueSeparator) {
			super(out, keyValueSeparator);
			// TODO Auto-generated constructor stub
		}

		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else if (o instanceof ImmutableBytesWritable) {
				ImmutableBytesWritable v = (ImmutableBytesWritable) o;
				out.write(v.get(), v.getOffset(), v.getLength());
			} else {
				out.write(o.toString().getBytes());
			}
		}

		@Override
		public synchronized void write(Object key, Object value) throws IOException {
			// TODO Auto-generated method stub
			boolean nullKey = key == null || key instanceof NullWritable;
			boolean nullValue = value == null || value instanceof NullWritable;
			if (nullKey && nullValue) {
				return;
			}
			if (!nullKey) {
				writeObject(key);
			}

			if (!nullValue) {
				writeObject(value);
			}

		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		String keyValueSeparator = conf.get("mapred.textoutputformat.separator", "\t");
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new MyLineRecordWriter<K, V>(fileOut, keyValueSeparator);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new MyLineRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
		}
	}

}
