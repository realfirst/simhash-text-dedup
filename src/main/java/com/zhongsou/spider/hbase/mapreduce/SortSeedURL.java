package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.InputSampler;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;
import com.zhongsou.spider.hbase.mapreduce.ScoreURLStatistic.ScoreURLMapper;

public class SortSeedURL extends Configured implements MapreduceV2Job {

	static class SortSeedURLMapper extends
			Mapper<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text> {

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stubc

		}

		ImmutableBytesWritable docid = new ImmutableBytesWritable();
		byte a[] = new byte[8];

		@Override
		protected void map(ImmutableBytesWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.arraycopy(key.get(), 0, a, 0, 8);
			docid.set(a);
			context.write(docid, value);
		}
	}

	static class SortSeedURLReducer extends
			Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text> {

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

		@Override
		protected void reduce(ImmutableBytesWritable key,
				Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			for (Text o : values) {
				context.write(key, o);
			}
		}
	}

	static Job createJob(Configuration conf, String[] args) {
		Job job;
		try {
			job = new Job(conf);
			// 旧的url docid

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setMapperClass(SortSeedURLMapper.class);
			job.setReducerClass(SortSeedURLReducer.class);
			job.setPartitionerClass(TotalOrderPartitioner.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			InputSampler.Sampler<ImmutableBytesWritable, Text> sampler = new InputSampler.RandomSampler<ImmutableBytesWritable, Text>(
					0.1, 10000, 10);
			Path input = new Path(args[0]);
			input = input.makeQualified(input.getFileSystem(conf));
			Path partitionFile = new Path(input, "_partitions");
			FileSystem fs = partitionFile.getFileSystem(job.getConfiguration());
			TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
					partitionFile);
			InputSampler.writePartitionFile(job, sampler);
			URI partitionUri = new URI(partitionFile.toString()
					+ "#_partitions");
			DistributedCache.addCacheFile(partitionUri, conf);
			DistributedCache.createSymlink(conf);
			fs.deleteOnExit(partitionFile);

			return job;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	public static void main(String args[]) {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.out
						.println("Wrong number of arguments: "
								+ otherArgs.length
								+ " args: urldocid newurlinfo outputpath number_of_reduce");
				System.exit(-1);
			}
			Job job = createJob(conf, otherArgs);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Job createRunnableJob(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		this.setConf(conf);
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.out
						.println("Wrong number of arguments: "
								+ otherArgs.length
								+ " args: urldocid newurlinfo outputpath number_of_reduce");
				System.exit(-1);
			}
			Job job = createJob(conf, otherArgs);
			return job;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

}
