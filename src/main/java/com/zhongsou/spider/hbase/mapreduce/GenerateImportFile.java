package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.MD5.MD5Len;
import com.zhongsou.spider.common.util.URLUtil;
import com.zhongsou.spider.hbase.mapreduce.ParseResultImporter.ParseResultMapper;
import com.zhongsou.spider.hbase.mapreduce.ParseResultImporter.ParseResultReducer;

public class GenerateImportFile {
	static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

	public static class FileMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {
		boolean keyWithFolder = false;
		byte res[];
		byte h[] = Bytes.toBytes("H");
		byte s[] = Bytes.toBytes("s");
		byte u[] = Bytes.toBytes("u");
		byte f[] = Bytes.toBytes("F");
		boolean putSrc = false;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generate
			Configuration conf = context.getConfiguration();
			if (conf.getBoolean("url_and_host", false)) {
				this.keyWithFolder = true;
				res = new byte[16];
			} else {
				res = new byte[8];
			}
			if (conf.getBoolean("put_html", false)) {
				this.putSrc = true;
			} else {
				this.putSrc = false;
			}
		}

		@Override
		protected void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String url = new String(key.get(), key.getOffset(), key.getLength());
			if (!url.matches("http://.*sina.com.cn.*"))
				return;
			if (this.keyWithFolder) {
				String folder = "";
				if (url.lastIndexOf("/") != -1) {
					folder = url.substring(0, url.lastIndexOf("/"));
				} else {
					folder = url;
				}
				// String host = URLUtil.getHost(url);
				byte a[] = MD5.digest8(folder.getBytes()).getDigest();
				System.arraycopy(a, 0, res, 0, 8);
				byte docid[] = MD5.digest8(key.get(), key.getOffset(), key.getLength()).getDigest();
				System.arraycopy(docid, 0, res, 8, 8);
				ImmutableBytesWritable k = new ImmutableBytesWritable();
				k.set(res, 0, res.length);
				KeyValue kv1 = new KeyValue(res, f, u, 0, u.length, System.currentTimeMillis(), Type.Put, key.get(), key.getOffset(), key.getLength());
				context.write(k, kv1);
				try {
					MD5 md5 = new MD5(res, MD5Len.sixten, 0);
					Log.info("key url=" + md5.getHexString(true));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (this.putSrc) {
					try {
						MD5 md5 = new MD5(res, MD5Len.sixten, 0);
						Log.info("con url=" + md5.getHexString(true));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					KeyValue kv2 = new KeyValue(res, h, s, 0, s.length, System.currentTimeMillis(), Type.Put, value.get(), value.getOffset(), value.getLength());
					context.write(k, kv2);
				}

			} else {
				byte docid[] = MD5.digest8(key.get(), key.getOffset(), key.getLength()).getDigest();
				System.arraycopy(docid, 0, res, 0, 8);
				ImmutableBytesWritable k = new ImmutableBytesWritable();
				k.set(res);
				KeyValue kv1 = new KeyValue(res, f, u, 0, u.length, System.currentTimeMillis(), Type.Put, key.get(), key.getOffset(), key.getLength());
				context.write(k, kv1);
				if (this.putSrc) {
					KeyValue kv2 = new KeyValue(res, h, s, 0, s.length, System.currentTimeMillis(), Type.Put, value.get(), value.getOffset(), value.getLength());
					context.write(k, kv2);
				}
			}

		}
	}

	static class ImportHtmlReducer extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {
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
			assert (kvList.size() == 2);

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
	
		Job job = new Job(conf, "import_html");
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.addInputPath(job, in);

		FileOutputFormat.setOutputPath(job, out);

		job.setJobName("import_html_job");
		job.setMapperClass(FileMapper.class);
		job.setReducerClass(ImportHtmlReducer.class);
		// conf.setBoolean("url_and_host", true);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(KeyValue.class);
		job.setOutputFormatClass(HFileOutputFormat.class);

		String tableName = conf.get("importTable", "webpage_1");
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