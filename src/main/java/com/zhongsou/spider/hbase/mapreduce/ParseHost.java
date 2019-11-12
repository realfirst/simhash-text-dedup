package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zhongsou.spider.common.util.URLUtil;

public class ParseHost {
	public static class ParseMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, IntWritable> {
		byte[] f = Bytes.toBytes("F");
		byte[] u = Bytes.toBytes("u");
		byte[] h = Bytes.toBytes("H");
		byte[] s = Bytes.toBytes("s");
		byte[] p = Bytes.toBytes("P");
		byte[] finger = Bytes.toBytes("f");
		ImmutableBytesWritable k2 = new ImmutableBytesWritable();
		HashSet<String> hostSet = new HashSet<String>();

		@Override
		protected void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String url = new String(key.get());
			String domain = URLUtil.getDomainName(url);
			if (!hostSet.contains(domain)) {
				k2.set(domain.getBytes());
				context.write(k2, new IntWritable(1));
				hostSet.add(domain);
			}
		}

	}

	public static class ParseReducer extends Reducer<ImmutableBytesWritable, IntWritable, Text, NullWritable> {

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text(key.get()), NullWritable.get());
		}

	}

	static Job createSubmitJob(Configuration conf, String args[]) {
		try {
//			conf.setBoolean("mapred.compress.map.output", true);
//			conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
//			conf.setBoolean("mapred.output.compress", true);
//			conf.set("mapred.output.compression.type", "BLOCK");
//			conf.set("mapred.output.compression", "org.apache.hadoop.io.compress.SnappyCodec");
			Job job = new Job(conf);
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			FileInputFormat.addInputPath(job, new Path(args[1]));
			job.setMapperClass(ParseMapper.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
//			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setReducerClass(ParseReducer.class);
			job.setNumReduceTasks(1);
			return job;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String args[]) {
		Configuration conf = HBaseConfiguration.create();
		try {
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.out.println("Wrong number of arguments: " + otherArgs.length + " usage:inputpath outputpath");
				System.exit(-1);
			}
			Job job = ParseHost.createSubmitJob(conf, otherArgs);

			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
