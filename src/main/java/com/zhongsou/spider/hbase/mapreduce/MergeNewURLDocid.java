package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zhongsou.spider.hadoop.TimeSequenceFileOutputFormat;

public class MergeNewURLDocid {

	public static class MergeDocidMap extends Mapper<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, NullWritable> {

		@Override
		protected void map(ImmutableBytesWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, value);
		}

	}

	public static class MergeDocidReducer extends Reducer<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, NullWritable> {

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
		}

	}

	static Job createMergeJob(Configuration conf, String args[]) {
		Job job = null;
		try {
			job = new Job(conf);
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setJobName("MergeDocidJob");
			job.setMapperClass(MergeDocidMap.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setReducerClass(MergeDocidReducer.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(NullWritable.class);
			job.setOutputFormatClass(TimeSequenceFileOutputFormat.class);
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return job;
	}

	public static void main(String args[]) {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 3) {
				System.out.println("Wrong number of arguments: " + otherArgs.length + " usage: input outpu numofreducer");
				System.exit(-1);
			}
			Job job = createMergeJob(conf, otherArgs);
			job.waitForCompletion(true);

		} catch (IOException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) { // TODO Auto-generated
			e.printStackTrace();
		}
	}
}
