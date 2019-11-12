package com.zhongsou.spider.hbase.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.CharacterCodingException;
import java.text.ParseException;
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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.NumberUtil;

public class ScanAndGenerateHfile {
	static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

	public static class ScanMapper extends
			TableMapper<ImmutableBytesWritable, KeyValue> {
		byte[] f = Bytes.toBytes("F");
		byte[] u = Bytes.toBytes("u");
		byte[] s = Bytes.toBytes("s");
		byte[] P = Bytes.toBytes("P");
		byte[] finger = Bytes.toBytes("f");
		byte la[] = Bytes.toBytes("la");
		byte h[] = Bytes.toBytes("h");
		byte unload[] = Bytes.toBytes("0");

		byte a[] = Bytes.toBytes("a");
		byte d[] = Bytes.toBytes("d");
		byte i[] = Bytes.toBytes("i");
		byte m[] = Bytes.toBytes("m");
		byte t[] = new byte[12];

		byte e[] = Bytes.toBytes("e");
		byte zero[] = Bytes.toBytes("0");

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// KeyValue kv1 = new KeyValue(key.get(), P, la, unload);
			// context.write(key, kv1);
			int atime = 0, mtime = 0, itime = 0, dtime = 0;
			byte access_time[] = null, insert_time[] = null, download_time[] = null, modify_time[] = null;
//			access_time = value.getValue(f, a);
			insert_time = value.getValue(f, i);
			download_time = value.getValue(f, d);
//			modify_time = value.getValue(f, m);
//			if (access_time != null) {
//				String t = new String(access_time).trim();
//				if (t != null && t.startsWith("135")) {
//					if (t.length() > 10) {
//						t = t.substring(0, 10);
//					}
//					atime = Integer.parseInt(t);
//				}
//
//			}
//			if (modify_time != null) {
//				String t = new String(modify_time).trim();
//				if (t != null && t.startsWith("135")) {
//					if (t.length() > 10) {
//						t = t.substring(0, 10);
//					}
//					mtime = Integer.parseInt(t);
//				}
//
//			}
			if (download_time != null) {
//				String t = new String(download_time).trim();
//				if (t != null && t.startsWith("135")) {
//					if (t.length() > 10) {
//						t = t.substring(0, 10);
//					}
//					dtime = Integer.parseInt(t);
//				}
				dtime=NumberUtil.readInt(download_time, 0);
			}
//			if (insert_time != null) {
//				String t = new String(insert_time).trim();
//				if (t != null && t.startsWith("135")) {
//					if (t.length() > 10) {
//						t = t.substring(0, 10);
//					}
//					itime = Integer.parseInt(t);
//				}
//			}
			// 4种不同的时间

//			access_time = NumberUtil.convertIntToC(atime);
//			modify_time = NumberUtil.convertIntToC(mtime);
			download_time =Bytes.toBytes(dtime);
//			insert_time = NumberUtil.convertIntToC(itime);
//			if (atime != 0) {
//				KeyValue kv1 = new KeyValue(key.get(), f, a, access_time);
//				context.write(key, kv1);
//			}
//			if (mtime != 0) {
//				KeyValue kv2 = new KeyValue(key.get(), f, m, modify_time);
//				context.write(key, kv2);
//			}
//			
//			if (itime != 0) {
//				KeyValue kv3 = new KeyValue(key.get(), f, i, insert_time);
//				context.write(key, kv3);
//			}
			
			if (dtime != 0) {
				KeyValue kv4 = new KeyValue(key.get(), f, d, download_time);
				context.write(key, kv4);
			}

			//
			// // System.arraycopy(insert_time, 0, t, 0, 4);
			// System.arraycopy(access_time, 0, t, 0, 4);
			// System.arraycopy(download_time, 0, t, 4, 4);
			// System.arraycopy(modify_time, 0, t, 8, 4);

			// KeyValue kv1 = new KeyValue(key.get(), f, e, zero);

			// List<KeyValue> tlist = value.getColumn(f, h);
			// if (tlist != null && tlist.size() >= 1) {
			// KeyValue kv = tlist.get(0);
			// KeyValue kv1 = new KeyValue(key.get(), f, h, kv.getTimestamp(),
			// Type.DeleteColumn);
			// context.write(key, kv1);
			// }

			// KeyValue kv2 = new KeyValue(key.get(), f, h, t);
			// context.write(key, kv2);

		}
	}

	public static class ScanReducer
			extends
			Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {
		List<KeyValue> kvList = new LinkedList<KeyValue>();

		@Override
		protected void reduce(ImmutableBytesWritable key,
				Iterable<KeyValue> values, Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			kvList.clear();
			Iterator<KeyValue> e = values.iterator();
			int i = 0;
			while (e.hasNext()) {
				kvList.add(e.next().clone());
				// context.write(key, e.next());
				// i++;
			}
			Collections.sort(kvList, KeyValue.COMPARATOR);
			for (KeyValue kv : kvList) {
				context.write(key, kv);
			}

		}

	}

	static String convertScanToString(Scan scan) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		scan.write(dos);
		return Base64.encodeBytes(out.toByteArray());
	}

	/**
	 * Parses a combined family and qualifier and adds either both or just the
	 * family in case there is not qualifier. This assumes the older colon
	 * divided notation, e.g. "data:contents" or "meta:".
	 * <p>
	 * Note: It will through an error when the colon is missing.
	 * 
	 * @param familyAndQualifier
	 *            family and qualifier
	 * @return A reference to this instance.
	 * @throws IllegalArgumentException
	 *             When the colon is missing.
	 */
	private static void addColumn(Scan scan, byte[] familyAndQualifier) {
		byte[][] fq = KeyValue.parseColumn(familyAndQualifier);
		if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
			scan.addColumn(fq[0], fq[1]);
		} else {
			scan.addFamily(fq[0]);
		}
	}

	/**
	 * Adds an array of columns specified using old format, family:qualifier.
	 * <p>
	 * Overrides previous calls to addFamily for any families in the input.
	 * 
	 * @param columns
	 *            array of columns, formatted as
	 * 
	 *            <pre>
	 * family:qualifier
	 * </pre>
	 */
	public static void addColumns(Scan scan, byte[][] columns) {
		for (byte[] column : columns) {
			addColumn(scan, column);
		}
	}

	/**
	 * Convenience method to help parse old style (or rather user entry on the
	 * command line) column definitions, e.g. "data:contents mime:". The columns
	 * must be space delimited and always have a colon (":") to denote family
	 * and qualifier.
	 * 
	 * @param columns
	 *            The columns to parse.
	 * @return A reference to this instance.
	 */
	private static void addColumns(Scan scan, String columns) {
		String[] cols = columns.split(" ");
		for (String col : cols) {
			addColumn(scan, Bytes.toBytes(col));
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

	static Job createSubmitJob(Configuration conf, String args[]) {
		try {

			Scan scan = new Scan();
			String tableName = conf.get("tableName", "webDB");
			scan.setCaching(500);
			// scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("a"));
//			scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("i"));
			 scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("d"));
			// scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("m"));
			// scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("e"));

			// scan.addColumn(Bytes.toBytes("H"), Bytes.toBytes("s"));
			// scan.addColumn(Bytes.toBytes("H"), Bytes.toBytes("s"));
			// scan.addFamily(Bytes.toBytes("H"));
			// scan.addFamily(Bytes.toBytes("P"));
			if (conf.get("scan_filter") != null) {
				Filter filter;
				try {
					ParseFilter parse = new ParseFilter();
					filter = parse.parseFilterString(conf.get("scan_filter"));
					System.out.println("set filter " + conf.get("scan_filter"));
					scan.setFilter(filter);
				} catch (CharacterCodingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} else {
				// SingleColumnValueFilter lff = null;
				// DateFormat d=new
				// java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				// java.util.Date date=d.parse("2012-01-01 00:00:00");
				// int m=(int)(date.getTime()/1000);
				// lff = new SingleColumnValueFilter(Bytes.toBytes("F"),
				// Bytes.toBytes("i"),
				// CompareFilter.CompareOp.LESS,NumberUtil.convertIntToC(m));
				ParseFilter parse = new ParseFilter();
				Filter filter = parse
						.parseFilterString("SingleColumnValueFilter('F','d',!=,'binary:0',true,true)");
				scan.setFilter(filter);
				// scan.setFilter(lff);
			}
			if (conf.get(TableInputFormat.SCAN_COLUMNS) != null) {
				addColumns(scan, conf.get(TableInputFormat.SCAN_COLUMNS));
			}

			if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
				scan.addFamily(Bytes.toBytes(conf
						.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
			}
			if (conf.get(TableInputFormat.SCAN_MAXVERSIONS) != null) {
				scan.setMaxVersions(Integer.parseInt(conf
						.get(TableInputFormat.SCAN_MAXVERSIONS)));
			}

			// scan.setMaxVersions(1);

			conf.set(TableInputFormat.SCAN, convertScanToString(scan));

			Job job = new Job(conf);
			job.setMapOutputValueClass(KeyValue.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(KeyValue.class);
			job.setOutputFormatClass(HFileOutputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			TableMapReduceUtil.initTableMapperJob(tableName, scan,
					ScanMapper.class, ImmutableBytesWritable.class,
					KeyValue.class, job);

			HTable table = new HTable(conf, tableName);
			Class<? extends Partitioner> topClass;
			try {
				topClass = getTotalOrderPartitionerClass();
			} catch (ClassNotFoundException e) {
				throw new IOException("Failed getting TotalOrderPartitioner", e);
			}
			job.setPartitionerClass(topClass);
			job.setReducerClass(ScanReducer.class);

			Log.info("Looking up current regions for table " + table);
			List<ImmutableBytesWritable> startKeys = getRegionStartKeys(table);
			Log.info("Configuring " + startKeys.size() + " reduce partitions "
					+ "to match current region count");
			job.setNumReduceTasks(startKeys.size());

			Path partitionsPath = new Path(job.getWorkingDirectory(),
					"partitions_" + System.currentTimeMillis());
			Log.info("Writing partition information to " + partitionsPath);
			FileSystem fs = partitionsPath.getFileSystem(conf);
			writePartitions(conf, partitionsPath, startKeys);
			TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
					partitionsPath);
			// writePartitionFile(job, sampler);
			URI partitionUri = new URI(partitionsPath.toString()
					+ "#_partitions");
			DistributedCache.addCacheFile(partitionUri, conf);
			DistributedCache.createSymlink(conf);
			Log.info("Incremental table output configured.");
			fs.deleteOnExit(partitionsPath);
			TableMapReduceUtil.addDependencyJars(job);

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
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
			Job job = ScanAndGenerateHfile.createSubmitJob(conf, otherArgs);
			if (otherArgs.length < 1) {
				System.out
						.println("Wrong number of arguments: "
								+ otherArgs.length
								+ " usage: outputpath SingleColumnValueFilter('F','s',=,'binary:5',true,true)");
				System.exit(-1);
			}
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
