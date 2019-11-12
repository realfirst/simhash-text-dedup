package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

public class GenerateHFile {
	static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

	public static class FileMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {

		byte P[] = Bytes.toBytes("P");
		byte la[] = Bytes.toBytes("la");
		byte F[] = Bytes.toBytes("F");
		byte a[] = Bytes.toBytes("a");
		byte d[] = Bytes.toBytes("d");
		byte i[] = Bytes.toBytes("i");
		byte[] m = Bytes.toBytes("m");
		byte []e=Bytes.toBytes("e");
		byte v[];

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generate

			int serial = Integer.parseInt(context.getConfiguration().get("serialNum", "0000"));
			Log.info("load searial num=" + serial);
			v = Bytes.toBytes(serial);

		}

		@Override
		protected void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			KeyValue kv1 = new KeyValue(key.get(), P, la, v);
			context.write(key, kv1);
		}
	}

	static class ImportReducer extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {
		List<KeyValue> kvList = new LinkedList<KeyValue>();

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<KeyValue> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			kvList.clear();
			Iterator<KeyValue> e = values.iterator();
			int i = 0;
			while (e.hasNext()) {
				// kvList.add(e.next());
				context.write(key, e.next());
				i++;
			}

		}

	}

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

	public static Job run(Configuration conf, String[] args) throws Exception {
		// TODO Auto-generated method stub

		// JobConf job = new JobConf(conf, ParseResultConverterJob.class);

		int now = (int) (System.currentTimeMillis() / 1000);

		conf.set("serialNum", String.valueOf(now));

		Job job = new Job(conf, "generate_import");
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.addInputPath(job, in);

		FileOutputFormat.setOutputPath(job, out);

		job.setMapperClass(FileMapper.class);
		job.setReducerClass(ImportReducer.class);
		// conf.setBoolean("url_and_host", true);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
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
		fs.deleteOnExit(partitionsPath);
		TableMapReduceUtil.addDependencyJars(job);

		return job;
	}

	public static void main(String args[]) {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.out.println("Wrong number of arguments: " + otherArgs.length);
				System.exit(-1);
			}
			Job job = run(conf, otherArgs);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
