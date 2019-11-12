package com.zhongsou.spider.hadoop;

import java.io.IOException;
import java.text.DateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 * 为每一个输出文件自动加一个时间戳的后缀
 * 
 * @author Xi Qi
 *
 * @param <K>
 * @param <V>
 */
public class TimeSequenceFileOutputFormat<K, V> extends SequenceFileOutputFormat<K, V> {
	static DateFormat format = new java.text.SimpleDateFormat("yyyyMMddHHmm");

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();

		CompressionCodec codec = null;
		CompressionType compressionType = CompressionType.NONE;
		if (getCompressOutput(context)) {
			// find the kind of compression to do
			compressionType = getOutputCompressionType(context);

			// find the right codec
			Class<?> codecClass = getOutputCompressorClass(context, DefaultCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		}
		// get the path of the temporary output file
		Path file = getDefaultWorkFile(context, "-"+format.format(new java.util.Date()));
		FileSystem fs = file.getFileSystem(conf);
		final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, file, context.getOutputKeyClass(), context.getOutputValueClass(), compressionType, codec, context);

		return new RecordWriter<K, V>() {

			public void write(K key, V value) throws IOException {

				out.append(key, value);
			}

			public void close(TaskAttemptContext context) throws IOException {
				out.close();
			}
		};
	}

}
