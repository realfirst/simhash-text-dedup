package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

/**
 * 
 * 更新hbase状态的标记
 * 
 * 需要生成2种文件：
 * 
 * <pre>
 * 1  hfile 将要加载的docid的P:l置为2(预加载，用于下载数据选择的filter使用), 将要删除的docid的P:l清理掉，,即删除已经下载的列表状态 将这一批操作的P:n flag都清除掉
 * 2  hfile 将要加载的docid的P:l置为最终要加载的1
 * </pre>
 * 
 * 输入的文件:
 * 
 * <pre>
 *  1 parse_url_and_figner 进行选择逻辑的新的url的docid，用于清除掉所有的P:n 标记
 *  2 删除列表 用于清理l标记 
 *  3 加载列表 用于生成生成预加载的标记的hfile和加载完成的hfile
 * </pre>
 * 
 * 
 * @author Xi Qi
 * 
 */
public class UpdateHbaseStatus {
  static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

  static class NewDocidMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
    ImmutableBytesWritable v = new ImmutableBytesWritable();

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      byte a[] = new byte[1];
      a[0] = '1';
      v.set(a);
    }

    @Override
      protected void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      context.write(key, v);
      context.getCounter("updatehbasestatus", "newdocid").increment(1);

    }

  }

  static class DeleteLoadMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
    ImmutableBytesWritable v = new ImmutableBytesWritable();

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      byte a[] = new byte[1];
      a[0] = '2';
      v.set(a);
    }

    @Override
      protected void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      context.write(key, v);
      context.getCounter("updatehbasestatus", "deleteLoad").increment(1);
    }
  }

  static class LoadFlagMapper extends Mapper<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
    ImmutableBytesWritable v = new ImmutableBytesWritable();

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      byte a[] = new byte[1];
      a[0] = '3';
      v.set(a);

    }

    @Override
      protected void map(ImmutableBytesWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      context.write(key, v);
      context.getCounter("updatehbasestatus", "loadFlag").increment(1);
    }

  }

  static class DocidReducer extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {
    LinkedList<KeyValue> kvList = new LinkedList<KeyValue>();

    byte[] P = Bytes.toBytes("P");
    byte[] F = Bytes.toBytes("F");
    byte[] n = Bytes.toBytes("n");
    byte[] la = Bytes.toBytes("la");
    byte[] preload = Bytes.toBytes("2");
    byte[] load = Bytes.toBytes("1");
    byte[] nonew = Bytes.toBytes("0");

    byte finger[] = new byte[8];
    Calendar lastCal = new java.util.GregorianCalendar();
    Calendar now = new java.util.GregorianCalendar();
    static long ONE_DAY_SECONDS = 24 * 60 * 60;
    static DateFormat dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub

    }

    @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.cleanup(context);

    }

    LinkedList<KeyValue> kvlist = new LinkedList<KeyValue>();

    @Override
      protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      // TODO Auto-generated method stub
      ImmutableBytesWritable v = null;
      Iterator<ImmutableBytesWritable> it = values.iterator();
      kvlist.clear();
      while (it.hasNext()) {
        v = it.next();
        // 清除new标记
        if (v.get()[v.getOffset()] == '1') {
          KeyValue kv = new KeyValue(key.get(), P, n, System.currentTimeMillis(), Type.Put, nonew);
          kvlist.add(kv);
        }
        // 清除load标记
        else if (v.get()[v.getOffset()] == '2') {
          KeyValue kv = new KeyValue(key.get(), P, la, System.currentTimeMillis(), Type.Put, nonew);
          kvlist.add(kv);
        }
        // 生成预加载和加载完成的hfile
        else if (v.get()[v.getOffset()] == '3') {
          KeyValue kv = new KeyValue(key.get(), P, la, System.currentTimeMillis(), Type.Put, preload);
          kvlist.add(kv);

        }

      }

      Collections.sort(kvlist, KeyValue.COMPARATOR);
      for (KeyValue kv : kvlist) {
        context.write(key, kv);
      }
    }
  }

  static class NewLoadReducer extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {
    LinkedList<KeyValue> kvList = new LinkedList<KeyValue>();

    byte[] P = Bytes.toBytes("P");
    byte[] F = Bytes.toBytes("F");
    byte[] n = Bytes.toBytes("n");
    byte[] la = Bytes.toBytes("la");
    byte[] preload = Bytes.toBytes("2");
    byte[] load = Bytes.toBytes("1");

    byte finger[] = new byte[8];
    Calendar lastCal = new java.util.GregorianCalendar();
    Calendar now = new java.util.GregorianCalendar();
    static long ONE_DAY_SECONDS = 24 * 60 * 60;
    static DateFormat dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub

    }

    @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub

    }

    LinkedList<KeyValue> kvlist = new LinkedList<KeyValue>();

    @Override
      protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      // TODO Auto-generated method stub
      ImmutableBytesWritable v = null;
      Iterator<ImmutableBytesWritable> it = values.iterator();
      kvlist.clear();
      while (it.hasNext()) {
        v = it.next();
        KeyValue kv = new KeyValue(key.get(), P, la, System.currentTimeMillis(), Type.Put, load);
        context.write(key, kv);
      }
    }
  }

  private static Class<? extends Partitioner> getTotalOrderPartitionerClass() throws ClassNotFoundException {
    Class<? extends Partitioner> clazz = null;
    try {
      clazz = (Class<? extends Partitioner>)
              Class.forName("org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner");
    } catch (ClassNotFoundException e) {
      clazz = (Class<? extends Partitioner>)
              Class.forName("org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner");
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

  static Job createOldURLJob(Configuration conf, String args[]) {
    Job job = null;
    try {
      job = new Job(conf);
      if (args.length > 4)
        Log.info("newdoicd path=" + args[2] + "\tdeleteload path=" + args[4] + "\tloadflag" + args[3]);
      else
        Log.info("newdoicd path=" + args[2] + "\tloadflag" + args[3]);
      MultipleInputs.addInputPath(job, new Path(args[2]), SequenceFileInputFormat.class, NewDocidMapper.class);
      MultipleInputs.addInputPath(job, new Path(args[3]), SequenceFileInputFormat.class, LoadFlagMapper.class);
      if (args.length > 4)
        MultipleInputs.addInputPath(job, new Path(args[4]), SequenceFileInputFormat.class, DeleteLoadMapper.class);

      FileOutputFormat.setOutputPath(job, new Path(args[0]));
      job.setJobName("UpdateHbaseStatus");
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(ImmutableBytesWritable.class);
      job.setReducerClass(DocidReducer.class);
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

  static Job createNewURLLoadJob(Configuration conf, String args[]) {
    Job job = null;
    try {
      job = new Job(conf);
      if (args.length > 4)
        Log.info("newdoicd path=" + args[2] + "\tdeleteload path=" + args[4] + "\tloadflag" + args[3]);
      else
        Log.info("newdoicd path=" + args[2] + "\tloadflag" + args[3]);
      MultipleInputs.addInputPath(job, new Path(args[3]), SequenceFileInputFormat.class, LoadFlagMapper.class);

      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setJobName("UpdateHbaseStatus");
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(ImmutableBytesWritable.class);
      job.setReducerClass(NewLoadReducer.class);
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

  public static void main(String args[]) {

    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs;
    try {
      otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length < 4) {
        System.out.println("Wrong number of arguments: " + otherArgs.length + " usage:parsed_url_finger deletelist loadlist");
        System.exit(-1);
      }
      Job job = createOldURLJob(conf, otherArgs);
      if (job.waitForCompletion(true)) {
        Job loadJob = createNewURLLoadJob(conf, otherArgs);
        loadJob.waitForCompletion(true);
      }

    } catch (IOException e) { // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) { // TODO Auto-generated
      e.printStackTrace();
    }

  }

}
