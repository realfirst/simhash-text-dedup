package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

public class GenerateURLData {
	static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

	public static class ParseMapper extends Mapper<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, NullWritable> {

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);

		}

		@Override
		protected void map(ImmutableBytesWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
		}

	}

	public static class ParseReducer extends Reducer<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, KeyValue> {
		LinkedList<KeyValue> kvList = new LinkedList<KeyValue>();
		ImmutableBytesWritable k2 = new ImmutableBytesWritable();
		Random rand = new Random();
		static byte num[] = "abcdefghijklmnopqrstuvwxyz0123456789".getBytes();
		StringBuffer buffer = new StringBuffer();
		private byte[] f = Bytes.toBytes("F");
		private byte[] p = Bytes.toBytes("P");
		private byte[] h = Bytes.toBytes("H");
		private byte[] u = Bytes.toBytes("u");
		private byte[] l = Bytes.toBytes("l");
		private byte[] n = Bytes.toBytes("n");
		private byte[] s = Bytes.toBytes("s");
		private byte[] c = Bytes.toBytes("c");

		private byte[] yes = Bytes.toBytes("1");
		private byte[] no = Bytes.toBytes("0");

		byte[] htmlbytes = new byte[1024 * 20];
		byte[] parsedbytes = new byte[1024];

		private void shuffle() {
			int htmllength = htmlbytes.length;

			int times = rand.nextInt(1000) + 1000;
			int a, b;
			for (int i = 0; i < times; i++) {
				a = rand.nextInt(htmllength) % htmllength;
				b = rand.nextInt(htmllength) % htmllength;
				if (a != b) {
					byte c = htmlbytes[a];
					htmlbytes[a] = htmlbytes[b];
					htmlbytes[b] = c;
				}
			}
			int parsedLength = parsedbytes.length;
			times = rand.nextInt(100) + 100;
			for (int i = 0; i < parsedLength; i++) {
				a = rand.nextInt(parsedLength) % parsedLength;
				b = rand.nextInt(parsedLength) % parsedLength;
				if (a != b) {
					byte c = parsedbytes[a];
					parsedbytes[a] = parsedbytes[b];
					parsedbytes[b] = c;
				}
			}

		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);

			int htmllength = htmlbytes.length;
			for (int i = 0; i < htmllength; i++) {
				htmlbytes[i] = num[rand.nextInt(num.length)];
			}
			int parsedLength = parsedbytes.length;
			for (int i = 0; i < parsedLength; i++) {
				parsedbytes[i] = num[rand.nextInt(num.length)];
			}

		}

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			this.shuffle();
			KeyValue v = new KeyValue(key.get(), p, c, this.parsedbytes);
			kvList.add(v);
			int i = 0;
			// 1/4 load flag
			long finger = rand.nextLong();

			v = new KeyValue(key.get(), p, f, Bytes.toBytes(finger));
			kvList.add(v);
			if (rand.nextInt(4) == 0) {
				v = new KeyValue(key.get(), p, l, this.yes);
				kvList.add(v);
			} else {
				v = new KeyValue(key.get(), p, l, this.no);
				kvList.add(v);
				// 1/16 new flag
				if (rand.nextInt(3750) == 0) {
					v = new KeyValue(key.get(), p, n, this.yes);
					kvList.add(v);
				}
			}

			v = new KeyValue(key.get(), h, s, this.htmlbytes);
			kvList.add(v);

			Collections.sort(kvList, KeyValue.COMPARATOR);

			for (KeyValue v2 : this.kvList) {
				context.write(key, v2);
			}
			kvList.clear();
		}
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

	private static Class<? extends Partitioner> getTotalOrderPartitionerClass() throws ClassNotFoundException {
		Class<? extends Partitioner> clazz = null;
		try {
			clazz = (Class<? extends Partitioner>) Class.forName("org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner");
		} catch (ClassNotFoundException e) {
			clazz = (Class<? extends Partitioner>) Class.forName("org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner");
		}
		return clazz;
	}

	static Job createSubmitJob(Configuration conf, String args[]) {
		try {
			conf.setBoolean("mapred.compress.map.output", true);
			conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
			conf.setBoolean("mapred.output.compress", true);
			conf.set("mapred.output.compression.type", "BLOCK");
			conf.set("mapred.output.compression", "org.apache.hadoop.io.compress.SnappyCodec");
			Job job = new Job(conf);
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			FileInputFormat.addInputPath(job, new Path(args[1]));
			job.setMapperClass(ParseMapper.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(NullWritable.class);

			// job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setReducerClass(ParseReducer.class);

			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(KeyValue.class);
			job.setOutputFormatClass(HFileOutputFormat.class);

			String tableName = conf.get("importTable", "webpage_2");
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
			TableMapReduceUtil.addDependencyJars(job.getConfiguration(), com.google.common.base.Preconditions.class);

			return job;
		} catch (IOException e) {
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
		try {
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.out.println("Wrong number of arguments: " + otherArgs.length + " usage:inputpath outputpath");
				System.exit(-1);
			}
			Job job = GenerateURLData.createSubmitJob(conf, otherArgs);
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
