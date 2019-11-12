package com.zhongsou.spider.hadoop;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class MyTextOutputFormat<K, V> extends TextOutputFormat<K, V> {
	class MyLineRecordWriter<K,V> extends LineRecordWriter {
		private static final String utf8 = "UTF-8";

		public MyLineRecordWriter(DataOutputStream out, String keyValueSeparator) {
			super(out, keyValueSeparator);
			// TODO Auto-generated constructor stub
		}

		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
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

	};

	@Override
	public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
		// TODO Auto-generated method stub
		 boolean isCompressed = getCompressOutput(job);
		    String keyValueSeparator = job.get("mapred.textoutputformat.separator", 
		                                       "\t");
		    if (!isCompressed) {
		      Path file = FileOutputFormat.getTaskOutputPath(job, name);
		      FileSystem fs = file.getFileSystem(job);
		      FSDataOutputStream fileOut = fs.create(file, progress);
		      return new MyLineRecordWriter<K, V>(fileOut, keyValueSeparator);
		    } else {
		      Class<? extends CompressionCodec> codecClass =
		        getOutputCompressorClass(job, GzipCodec.class);
		      // create the named codec
		      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
		      // build the filename including the extension
		      Path file = 
		        FileOutputFormat.getTaskOutputPath(job, 
		                                           name + codec.getDefaultExtension());
		      FileSystem fs = file.getFileSystem(job);
		      FSDataOutputStream fileOut = fs.create(file, progress);
		      return new MyLineRecordWriter<K, V>(new DataOutputStream
		                                        (codec.createOutputStream(fileOut)),
		                                        keyValueSeparator);
		    }
	}

}
