package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;

/**
 * Describe class <code>HBLoadFlagUpdater</code> here.
 *
 * @version 1.0
 */
public class HBLoadFlagUpdater extends Configured implements MapreduceV2Job {

  /**
   * Describe class <code>ModifyListMapper</code> here. update the la column
   * to "0" for loaded webpages
   */
  static class ModifyListMapper
      extends
      Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {
    private byte[] ts;
    private byte[] P = Bytes.toBytes("P");
    private byte[] la = Bytes.toBytes("la");
    private byte[] zero = Bytes.toBytes("0");

    @Override
      protected void setup(Context context) throws IOException,
        InterruptedException {
      // TODO Auto-generated method stub
      int timestamp = Integer.parseInt(context.getConfiguration().get(
          "serialNumber"));
      ts = Bytes.toBytes(timestamp);
    }

    @Override
      public void map(ImmutableBytesWritable key,
                      ImmutableBytesWritable value, Context context)
        throws IOException, InterruptedException {
      KeyValue kv = new KeyValue(key.get(), P, la, zero);
      context.write(key, kv);
      context.getCounter("hfile_load", "modify").increment(1);
    }
  }

  /**
   * Describe class <code>LoadListMapper</code> here. update the la column
   * with the value of serialNumber for webpages which are eligible to load
   */
  static class LoadListMapper
      extends
      Mapper<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, KeyValue> {

    private byte[] ts;
    private byte[] P = Bytes.toBytes("P");
    private byte[] la = Bytes.toBytes("la");

    @Override
      public void setup(Context context) throws IOException,
        InterruptedException {
      int timestamp = Integer.parseInt(context.getConfiguration().get(
          "serialNumber"));
      ts = Bytes.toBytes(timestamp);
    }

    @Override
      public void map(ImmutableBytesWritable key, NullWritable value,
                      Context context) throws IOException, InterruptedException {
      KeyValue kv = new KeyValue(key.get(), P, la, ts);
      context.write(key, kv);
      context.getCounter("hfile_load", "load").increment(1);
    }
  }

  /**
   *
   * 新的指纹加载map
   *
   * @author dape
   *
   */

  static class LoadFingerMapper
      extends
      Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {

    private byte[] P = Bytes.toBytes("P");
    private byte[] f = Bytes.toBytes("f");

    @Override
      public void setup(Context context) throws IOException,
        InterruptedException {

    }

    @Override
      public void map(ImmutableBytesWritable key,
                      ImmutableBytesWritable value, Context context)
        throws IOException, InterruptedException {
      KeyValue kv = new KeyValue(key.get(), P, f, value.get());
      context.write(key, kv);
      context.getCounter("hfile_load", "finger").increment(1);
    }
  }

  static class HFileGenerateReducer
      extends
      Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {
    List<KeyValue> kvList = new LinkedList<KeyValue>();

    @Override
      protected void reduce(ImmutableBytesWritable key,
                            Iterable<KeyValue> values, Context context) throws IOException,
        InterruptedException {
      kvList.clear();
      for (KeyValue value : values) {
        // context.write(key, value);
        kvList.add(value.clone());
      }
      Collections.sort(kvList, KeyValue.COMPARATOR);
      for (KeyValue kv : kvList) {
        context.write(key, kv);
      }
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

  /**
   * Return the start keys of all of the regions in this table, as a list of
   * ImmutableBytesWritable.
   */
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

  /**
   * Write out a SequenceFile that can be read by TotalOrderPartitioner that
   * contains the split points in startKeys.
   *
   * @param partitionsPath
   *            output path for SequenceFile
   * @param startKeys
   *            the region start keys
   */
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

  public static Job createJob(Configuration conf, String args[])
      throws Exception {
    Job job = new Job(conf);
    job.setJarByClass(HBLoadFlagUpdater.class);
    MultipleInputs.addInputPath(job, new Path(args[0]),
                                SequenceFileInputFormat.class, ModifyListMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]),
                                SequenceFileInputFormat.class, LoadListMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[2]),
                                SequenceFileInputFormat.class, LoadFingerMapper.class);
    job.setReducerClass(HFileGenerateReducer.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);
    job.setOutputFormatClass(HFileOutputFormat.class);
    HFileOutputFormat.setOutputPath(job, new Path(args[3]));

    String tableName = conf.get("tableName", "webDB");
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
    // TableMapReduceUtil.addDependencyJars(job);
    fs.deleteOnExit(partitionsPath);

    // Load generated HFiles into table
    // LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    // loader.doBulkLoad(new Path(outDir), table);

    return job;
  }

  @Override
  public Job createRunnableJob(String[] args) {
    // TODO Auto-generated method stub
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs;
    try {
      otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length < 3) {
        System.out.println("Wrong number of arguments: "
                           + otherArgs.length + " modifylist loadlist output");
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

  public static void main(String args[]) {

  }

}
