package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.log.Log;

import com.zhongsou.spider.avro.ParserResultConverter;
import com.zhongsou.spider.bean.ParserResult;
import com.zhongsou.spider.bean.URLInfo;
import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.MD5.MD5Len;
import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;

/***
 * 
 * 
 * 转化分析后的结果，生成4输出文件 <br>
 * 1 新的指纹对文件，key docid,value finger 为加载列表使用<br>
 * 2 网页之中的分析的链接的url的文件，key docid value URLInfo ,包含链接url,url docid ,src url docid <br>
 * 3 hfile key docid value keyvalue，分别对应于不同的字段 <br>
 * 4 网页的源信息文件　key docid value BytesWriable 格式:
 * 
 * <pre>
 * newfinger(8)+oldfinger(8)+newURLToday(int 4)+modifyCount(int 4)+errorCount(int 4)+newURLBitmap(int 4)+
 * modifyBitmap(int 4)+lastModifyTime(long 8 c time second)
 * </pre>
 * 
 * @author Xi Qi
 * 
 */
public class ParseResultImporter extends Configured implements MapreduceV2Job {
	static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

	static class ParseResultMapper extends
			Mapper<Text, Text, ImmutableBytesWritable, Text> {
		ImmutableBytesWritable k2 = new ImmutableBytesWritable();

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			k2.set(key.getBytes());
			context.write(k2, value);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Log.info("conf=" + TotalOrderPartitioner.getPartitionFile(conf));
		}
	}

	static class ParseResultReducer
			extends
			Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, ImmutableBytesWritable> {
		private MultipleOutputs mos;
		private ParserResultConverter parseConverter;
		ImmutableBytesWritable hbaseKey = new ImmutableBytesWritable();
		LinkedList<KeyValue> kvList = new LinkedList<KeyValue>();
		byte P[] = Bytes.toBytes("P");
		byte n[] = Bytes.toBytes("n");
		byte old[] = Bytes.toBytes("0");

		DateFormat format = new java.text.SimpleDateFormat("yyyyMMddHHmmss");

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			mos = new MultipleOutputs(context);

			Configuration conf = context.getConfiguration();
			String timestamp = conf.get("seqTimestamp");
			if (timestamp != null) {
				try {
					Date d = format.parse(timestamp);
					if (d != null) {
						int m = (int) (d.getTime() / 1000);
						parseConverter = new ParserResultConverter(m,
								context.getConfiguration());
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			if (parseConverter == null) {
				parseConverter = new ParserResultConverter(
						context.getConfiguration());
				Log.info("error could not find seqtimestamp");
			}

			Log.info("conf=" + TotalOrderPartitioner.getPartitionFile(conf));
		}

		@Override
		protected void reduce(ImmutableBytesWritable key,
				Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub

			Iterator<Text> it = values.iterator();
			ImmutableBytesWritable k3 = new ImmutableBytesWritable();
			k3.set(key.get());
			while (it.hasNext()) {
				Text value = it.next();
				kvList.clear();
				ParserResult result = parseConverter.converter(key.get(),
						key.getLength(), value.getBytes(), value.getLength(),
						kvList);
				if (result != null) {
					if (result.getFinger() != null
							&& result.getFinger().length == 8) {
						ImmutableBytesWritable finger = new ImmutableBytesWritable();
						finger.set(result.getFinger(), 0, 8);
						context.write(k3, finger);
						ImmutableBytesWritable meta = new ImmutableBytesWritable();
						byte res[] = new byte[result.getMeta().length + 8];
						// 元数据格式 newfinger(8)+oldfinger(8)+newURLToday(int
						// 4)+modifyCount(int 4)+errorCount(int
						// 4)+newURLBitmap(int 4)+modifyBitmap(int
						// 4)+lastModifyTime(long 8 c time second)
						System.arraycopy(finger.get(), 0, res, 0, 8);
						System.arraycopy(result.getMeta(), 0, res, 8,
								result.getMeta().length);
						meta.set(res);
						Log.info("new url meta "
								+ StringUtils.byteToHexString(result
										.getUrlMd5())
								+ "\tmeta="
								+ StringUtils.byteToHexString(res, 0,
										meta.getLength()));
						mos.write("URLMeta", k3, meta);
					}
					Set<URLInfo> infoset = result.getInfoSet();
					if (infoset != null && infoset.size() > 0) {
						for (URLInfo info : infoset) {
							mos.write("parsedURL", k3, info);
						}
					}
					if (kvList.size() > 0) {
						try {
							// 在插入hbase之前要进行kv的排序
							Collections.sort(kvList, KeyValue.COMPARATOR);
							for (KeyValue kv : kvList) {
								if (key.getLength() == 8) {
									try {
										mos.write("hfileData", key, kv);
									} catch (Exception e) {
										e.printStackTrace();
										MD5 md5 = new MD5(kv.getRow(),
												MD5Len.eight, 0);
										Log.info("kv="
												+ md5.getHexString()
												+ "\tstring binary="
												+ Bytes.toStringBinary(kv
														.getRow()));
									}
								} else {
									Log.info("error key length " + key);
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						System.out.println("kv list size =" + kvList.size());
					}

				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			mos.close();
			super.cleanup(context);
		}

	}

	private static Class<? extends Partitioner> getTotalOrderPartitionerClass()
			throws ClassNotFoundException {
		Class<? extends Partitioner> clazz = null;
		try {
			clazz = (Class<? extends Partitioner>) Class
					.forName("org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner");
		} catch (ClassNotFoundException e) {
			clazz = (Class<? extends Partitioner>) Class
					.forName("org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner");
		}
		return clazz;
	}

	private static List<ImmutableBytesWritable> getRegionStartKeys(HTable table)
			throws IOException {
		byte[][] byteKeys = table.getStartKeys();
		ArrayList<ImmutableBytesWritable> ret = new ArrayList<ImmutableBytesWritable>(
				byteKeys.length);
		for (byte[] byteKey : byteKeys) {
			ret.add(new ImmutableBytesWritable(byteKey));
		}
		return ret;
	}

	private static void writePartitions(Configuration conf,
			Path partitionsPath, List<ImmutableBytesWritable> startKeys)
			throws IOException {
		if (startKeys.isEmpty()) {
			throw new IllegalArgumentException("No regions passed");
		}

		// We're generating a list of split points, and we don't ever
		// have keys < the first region (which has an empty start key)
		// so we need to remove it. Otherwise we would end up with an
		// empty reducer with index 0
		TreeSet<ImmutableBytesWritable> sorted = new TreeSet<ImmutableBytesWritable>(
				startKeys);

		ImmutableBytesWritable first = sorted.first();
		if (!first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
			throw new IllegalArgumentException(
					"First region of table should have empty start key. Instead has: "
							+ Bytes.toStringBinary(first.get()));
		}
		sorted.remove(first);

		// Write the actual file
		FileSystem fs = partitionsPath.getFileSystem(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
				partitionsPath, ImmutableBytesWritable.class,
				NullWritable.class);

		try {
			for (ImmutableBytesWritable startKey : sorted) {
				writer.append(startKey, NullWritable.get());
			}
		} finally {
			writer.close();
		}
	}

	static void configureCompression(HTable table, Configuration conf)
			throws IOException {
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
			compressionConfigValue.append(URLEncoder.encode(
					familyDescriptor.getNameAsString(), "UTF-8"));
			compressionConfigValue.append('=');
			compressionConfigValue.append(URLEncoder.encode(familyDescriptor
					.getCompression().getName(), "UTF-8"));
		}
		// Get rid of the last ampersand
		conf.set(COMPRESSION_CONF_KEY, compressionConfigValue.toString());
	}

	public static Job run(Configuration conf, String[] args) throws Exception {
		// TODO Auto-generated method stub

		// JobConf job = new JobConf(conf, ParseResultConverterJob.class);

		URI hosturl = new URI(
				"hdfs://hadoop-master-83:9900/user/kaifa/robots_output/part-r-00000"
						+ "#part-r-00000");
		URI domainurl = new URI(
				"hdfs://hadoop-master-83:9900/user/kaifa/robots_output/robotidx-r-00000"
						+ "#robotidx-r-00000");
		DistributedCache.addCacheFile(hosturl, conf);
		DistributedCache.addCacheFile(domainurl, conf);
		DistributedCache.createSymlink(conf);
		conf.set("robot_idx_name", "robotidx-r-00000");
		conf.set("robot_file_name", "part-r-00000");
		Job job = new Job(conf);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setMapperClass(ParseResultMapper.class);
		job.setReducerClass(ParseResultReducer.class);
		// job.setMapSpeculativeExecution(false);
		// job.setReduceSpeculativeExecution(false);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(ImmutableBytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, "URLMeta",
				SequenceFileOutputFormat.class, ImmutableBytesWritable.class,
				ImmutableBytesWritable.class); // 此处的文件名必须与getCollector设置的文件名相同
		MultipleOutputs.addNamedOutput(job, "parsedURL",
				SequenceFileOutputFormat.class, ImmutableBytesWritable.class,
				URLInfo.class); // 此处的文件名必须与getCollector设置的文件名相同
		MultipleOutputs.addNamedOutput(job, "hfileData",
				HFileOutputFormat.class, ImmutableBytesWritable.class,
				KeyValue.class);
		MultipleOutputs.setCountersEnabled(job, true);
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
		Log.info("Configuring " + startKeys.size() + " reduce partitions "
				+ "to match current region count");
		job.setNumReduceTasks(startKeys.size());

		Path partitionsPath = new Path(job.getWorkingDirectory(), "partitions_"
				+ System.currentTimeMillis());
		Log.info("Writing partition information to " + partitionsPath);
		FileSystem fs = partitionsPath.getFileSystem(conf);
		writePartitions(conf, partitionsPath, startKeys);
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
				partitionsPath);
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
				System.out.println("Wrong number of arguments: "
						+ otherArgs.length);
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

	@Override
	public Job createRunnableJob(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.out.println("Wrong number of arguments: "
						+ otherArgs.length);
				System.exit(-1);
			}
			Job job = run(conf, otherArgs);
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
