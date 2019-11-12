package com.zhongsou.incload;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;
import com.zhongsou.spider.hbase.mapreduce.BinaryOutputFormat;

/**
 * Describe class <code>DeDup</code> here. the dirve class for dedup MapReduce
 * job
 * 
 * @author <a href="mailto:dingje@zhongsou.com">Ding Jingen</a>
 * @version 1.0
 */
public class DeDup extends Configured implements Tool, MapreduceV2Job {
	static Logger logger = Logger.getLogger(DeDup.class);
	private static final byte[] cf = Bytes.toBytes("P");
	private static DateFormat dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss");

	public Job createJob(String args[]) throws IOException {
		Configuration conf = this.getConf();
		// Compress Map output
		conf.setBoolean("mapred.compress.map.output", true);
		conf.setClass("mapred.map.output.compression.codec", SnappyCodec.class, CompressionCodec.class);
		String table = conf.get("scanTable", "webDB");
		Scan scan = new Scan();
		scan.addColumn(cf, Bytes.toBytes("f"));
		scan.addColumn(cf, Bytes.toBytes("p"));
		scan.addColumn(cf, Bytes.toBytes("la"));
		scan.addColumn(cf, Bytes.toBytes("n"));
		int caching = conf.getInt("tableCaching", 500);
		scan.setCaching(caching);
		scan.setCacheBlocks(false);
		scan.setMaxVersions(1);
		String loadTime = conf.get("seqTimestamp", "0").trim();
		if (loadTime.equals("0")) {
			logger.info("must give a new load timestamp");
			return null;
		}
		int timestamp = 0;
		try {
			Date d = dateFormat.parse(loadTime);
			timestamp = (int) (d.getTime() / 1000l);
			conf.setInt("timestamp", timestamp);
			logger.info("loadtime " + loadTime + " convert to a int " + timestamp);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("parse date format error " + loadTime);
			return null;
		}
		int tsNf = timestamp;
		if (tsNf == 0) {
			Log.info("must give a new load timestamp");
			return null;
		}
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		SingleColumnValueFilter nff = null;
		SingleColumnValueFilter lff = null;
		nff = new SingleColumnValueFilter(cf, Bytes.toBytes("n"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(tsNf));
		nff.setFilterIfMissing(true);
		list.addFilter(nff);

		lff = new SingleColumnValueFilter(cf, Bytes.toBytes("la"), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("0"));
		lff.setFilterIfMissing(true);
		list.addFilter(lff);
		scan.setFilter(list);
		Job job = new Job(conf);
		job.setJarByClass(DeDup.class);
		TableMapReduceUtil.initTableMapperJob(table, scan, DeDupMapper.class, KeyValuePair.class, DupPair.class, job);

		job.setReducerClass(DeDupReducer.class);

		// group and partition by the first int in the pair
		job.setPartitionerClass(UrlidPartitioner.class);
		job.setGroupingComparatorClass(UrlidGroupingComparator.class);
		job.setMapSpeculativeExecution(false);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(DupPair.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		Path output = new Path(args[0]);
		SequenceFileOutputFormat.setOutputPath(job, output);
		
		//针对于新加载的网页并且指纹未修改，进入未加载列表，从而生成加载列表时，这类docid不再重复发送给索引
		MultipleOutputs.addNamedOutput(job, "unloadKey", SequenceFileOutputFormat.class, ImmutableBytesWritable.class, NullWritable.class);
		// job.setNumReduceTasks(80); // no need the number reduce is equal the
		// size of
		return job;

	}

	public int run(String[] args) throws Exception {
		int i = 0;
		for (String arg : args) {
			System.out.println("arg " + i + ":" + arg);
			i++;
		}

		Job job = this.createJob(args);
		if (job != null)
			return job.waitForCompletion(true) ? 0 : -1;
		else
			return -1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 1) {
				System.out.println("Wrong number of arguments: " + otherArgs.length + " args: urldocid newurlinfo outputpath number_of_reduce");
				System.exit(-1);
			}
			DeDup p = new DeDup();
			p.setConf(conf);
			Job job = p.createJob(otherArgs);
			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Partition based on urlid
	 */
	public static class UrlidPartitioner extends Partitioner<KeyValuePair, DupPair> {
		@Override
		public int getPartition(KeyValuePair key, DupPair value, int numPartitions) {
			return (key.getDupUrlid().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	

	/**
	 * Compare only the first part of the pair, so that reduce is called once
	 * for each value of the first part.
	 */
	public static class UrlidGroupingComparator extends WritableComparator {

		protected UrlidGroupingComparator() {
			super(KeyValuePair.class, true);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1 + 4, 8, b2, s2 + 4, 8);
		}

	}

	@Override
	public Job createRunnableJob(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 1) {
				System.out.println("Wrong number of arguments: " + otherArgs.length + " args: urldocid newurlinfo outputpath number_of_reduce");
				System.exit(-1);
			}
			this.setConf(conf);
			Job job = createJob(otherArgs);
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
