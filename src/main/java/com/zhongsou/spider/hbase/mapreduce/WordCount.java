package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class WordCountMap extends
			Mapper<Text, Text, Text, IntWritable> {
		IntWritable one = new IntWritable(1);

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// context.write(key, value);
			String args[] = value.toString().split("[\\s\t\r\n]{1,}");
			if (args != null) {
				for (String s : args) {
					context.write(new Text(s), one);
				}
			}

		}

	}
	
	public static class WordCountTextMap extends
	Mapper<LongWritable, Text, Text, IntWritable> {
IntWritable one = new IntWritable(1);

@Override
protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	// context.write(key, value);
	String args[] = value.toString().split("[\\s\t\r\n]{1,}");
	if (args != null) {
		for (String s : args) {
			context.write(new Text(s), one);
		}
	}

}

}

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int a = 0;
			for (IntWritable t : values) {
				a += t.get();
			}
			context.write(key, new IntWritable(a));

		}

	}

	static Job createJob(Configuration conf, String args[]) {
		Job job = null;
		try {
			job = new Job(conf);
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setJobName("WordCountJob");
			
			String inputFormat = conf.get("input_format");
			if (inputFormat == null || inputFormat.equals("")) {
				job.setInputFormatClass(TextInputFormat.class);
				job.setMapperClass(WordCountTextMap.class);
			} else {
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setMapperClass(WordCountMap.class);
			}
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setReducerClass(WordCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			String outputFormat=conf.get("output_format");
			if(outputFormat==null||outputFormat.equals(""))
			{
			job.setOutputFormatClass(TextOutputFormat.class);
			}
			else
			{
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
			}
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
				System.out
						.println("Wrong number of arguments: "
								+ otherArgs.length
								+ " usage: input outpu numofreducer");
				System.exit(-1);
			}
			Job job = createJob(conf, otherArgs);
			job.waitForCompletion(true);

		} catch (IOException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) { // TODO Auto-generated
			e.printStackTrace();
		}
	}
}
