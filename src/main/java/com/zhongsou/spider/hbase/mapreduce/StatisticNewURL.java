package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;
import com.zhongsou.spider.hbase.mapreduce.StatisticalURL.NewURLInfoMapper;
import com.zhongsou.spider.hbase.mapreduce.StatisticalURL.NewURLReducer;

public class StatisticNewURL extends Configured implements MapreduceV2Job {
	static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

	private static Class<? extends Partitioner> getTotalOrderPartitionerClass() throws ClassNotFoundException {
		Class<? extends Partitioner> clazz = null;
		try {
			clazz = (Class<? extends Partitioner>) Class.forName("org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner");
		} catch (ClassNotFoundException e) {
			clazz = (Class<? extends Partitioner>) Class.forName("org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner");
		}
		return clazz;
	}

	private static List<ImmutableBytesWritable> getRegionStartKeys(HTable table) throws IOException {
		byte[][] byteKeys = table.getStartKeys();
		ArrayList<ImmutableBytesWritable> ret = new ArrayList<ImmutableBytesWritable>(byteKeys.length);
		for (byte[] byteKey : byteKeys) {
			ret.add(new ImmutableBytesWritable(byteKey));
		}
		return ret;
	}

	private static void writePartitions(Configuration conf, Path partitionsPath, List<ImmutableBytesWritable> startKeys) throws IOException {
		if (startKeys.isEmpty()) {
			throw new IllegalArgumentException("No regions passed");
		}

		// We're generating a list of split points, and we don't ever
		// have keys < the first region (which has an empty start key)
		// so we need to remove it. Otherwise we would end up with an
		// empty reducer with index 0
		TreeSet<ImmutableBytesWritable> sorted = new TreeSet<ImmutableBytesWritable>(startKeys);

		ImmutableBytesWritable first = sorted.first();
		if (!first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
			throw new IllegalArgumentException("First region of table should have empty start key. Instead has: " + Bytes.toStringBinary(first.get()));
		}
		sorted.remove(first);

		// Write the actual file
		FileSystem fs = partitionsPath.getFileSystem(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, partitionsPath, ImmutableBytesWritable.class, NullWritable.class);

		try {
			for (ImmutableBytesWritable startKey : sorted) {
				writer.append(startKey, NullWritable.get());
			}
		} finally {
			writer.close();
		}
	}

	static void configureCompression(HTable table, Configuration conf) throws IOException {
		StringBuilder compressionConfigValue = new StringBuilder();
		HTableDescriptor tableDescriptor = table.getTableDescriptor();
		if (tableDescriptor == null) {
			// could happen with mock table instance
			return;
		}
		Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
		int i = 0;
		for (HColumnDescriptor familyDescriptor : families) {
			if (i++ > 0) {
				compressionConfigValue.append('&');
			}
			compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
			compressionConfigValue.append('=');
			compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getCompression().getName(), "UTF-8"));
		}
		// Get rid of the last ampersand
		conf.set(COMPRESSION_CONF_KEY, compressionConfigValue.toString());
	}

	static class NewURLInfoMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

		@Override
		protected void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			context.write(key, value);
			if (key.get().length > 8) {
				context.getCounter("statisticurl", "newurlmap_wrongkey").increment(1);
				return;
			}
			context.getCounter("statisticurl", "newurlmap").increment(1);
		}

	}

	static class NewURLReducer extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {

		byte F[] = Bytes.toBytes("F");
		byte s[] = Bytes.toBytes("s");
		byte i[] = Bytes.toBytes("i");
		byte u[] = Bytes.toBytes("u");
		byte newurls[] = Bytes.toBytes("0");
		LinkedList<KeyValue> kvList = new LinkedList<KeyValue>();

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			Iterator<ImmutableBytesWritable> e = values.iterator();
			ImmutableBytesWritable url = e.next();
			kvList.clear();
			// 新insert标记
			KeyValue kv = new KeyValue(key.get(), F, s, newurls);
			kvList.add(kv);
			// url
			kv = new KeyValue(key.get(), F, u, url.get());
			kvList.add(kv);
			// insert time,string
			int insertTime=(int)(System.currentTimeMillis() / 1000);
			kv = new KeyValue(key.get(), F, i,  NumberUtil.convertIntToC((int)(insertTime)));
			kvList.add(kv);
			Collections.sort(kvList, KeyValue.COMPARATOR);
			for (KeyValue keyvalue : kvList) {
				context.write(key, keyvalue);
			}
			context.getCounter("statisticurl", "newurlreduce").increment(1);
			// 新url只有3个状态，返回
			return;

		}

	}

	static Job createNewURLJob(Configuration conf, String args[]) {
		Job job = null;
		try {
			job = new Job(conf);
			MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class, NewURLInfoMapper.class);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(ImmutableBytesWritable.class);
			job.setReducerClass(NewURLReducer.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(KeyValue.class);
			job.setOutputFormatClass(HFileOutputFormat.class);
			String tableName = conf.get("importTable", "webDB");
			HTable table = new HTable(conf, tableName);

			Class<? extends Partitioner> topClass;
			try {
				topClass = getTotalOrderPartitionerClass();
			} catch (ClassNotFoundException e) {
				throw new IOException("Failed getting TotalOrderPartitioner", e);
			}
			job.setPartitionerClass(topClass);

			Log.info("Looking up current regions for table " + table);
			List<ImmutableBytesWritable> startKeys = getRegionStartKeys(table);
			Log.info("Configuring " + startKeys.size() + " reduce partitions " + "to match current region count");
			job.setNumReduceTasks(startKeys.size());

			Path partitionsPath = new Path(job.getWorkingDirectory(), "partitions_" + System.currentTimeMillis());
			Log.info("Writing partition information to " + partitionsPath);
			FileSystem fs = partitionsPath.getFileSystem(conf);
			writePartitions(conf, partitionsPath, startKeys);
			TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionsPath);
			// writePartitionFile(job, sampler);
			URI partitionUri = new URI(partitionsPath.toString() + "#_partitions");
			DistributedCache.addCacheFile(partitionUri, conf);
			DistributedCache.createSymlink(conf);
			Log.info("Incremental table output configured.");
			TableMapReduceUtil.addDependencyJars(job);
			fs.deleteOnExit(partitionsPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return job;
	}

	@Override
	public Job createRunnableJob(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.out.println("Wrong number of arguments: " + otherArgs.length + " usage:");
				System.exit(-1);
			}
			Job job = createNewURLJob(conf, otherArgs);
			return job;
		} catch (IOException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) { // TODO Auto-generated
			e.printStackTrace();
		}
		return null;
	}

}
