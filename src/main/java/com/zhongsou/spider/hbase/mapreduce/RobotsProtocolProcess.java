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
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.url.RobotsProtocolParser;
import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.common.util.URLUtil;

public class RobotsProtocolProcess {
  static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

  public static class RobotsProtocolMapper extends
                                           Mapper<Text, Text, ImmutableBytesWritable, KeyValue> {
    RobotsProtocolParser robotsTxtParser = new RobotsProtocolParser();
    byte R[] = Bytes.toBytes("R");
    byte s[] = Bytes.toBytes("s");
    byte r[] = Bytes.toBytes("r");
    byte v[];

    ImmutableBytesWritable k = new ImmutableBytesWritable();

    @Override
      protected void setup(Context context) throws IOException,
        InterruptedException {

    }

    @Override
      protected void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      Text v = value;
      byte[] b = v.getBytes();
      // 下载成功
      if (b[0] == '1') {
        int packetNum = NumberUtil.readInt(b, 1);
        int packetLen = 0, offset = 5;
        int headerLen = 0, contentLen = 0;
        int urlLen = 0;
        int urloffset = 0;
        String content = null;
        String url = null;
        if (packetNum == 3) {
          for (int i = 0; i < 2; i++) {
            if (i == 1) {
              urlLen = NumberUtil.readInt(b, offset);
              url = new String(b, offset + 4, urlLen);
              urloffset = offset + 4;
            }
            packetLen = NumberUtil.readInt(b, offset);
            offset += packetLen + 4;
          }
          contentLen = NumberUtil.readInt(b, offset);
          if (contentLen < 0) {
            Log.info("error packet");
            return;
          }
          content = new String(b, offset + 4, contentLen);
          // this.clawerConvert.reset(k.getBytes(), b, headerLen,
          // contentLen, offset + 4, offset + 4);

        } else if (packetNum == 4) {
          for (int i = 0; i < 2; i++) {
            if (i == 1) {
              urlLen = NumberUtil.readInt(b, offset);
              urloffset = offset + 4;
              url = new String(b, offset + 4, urlLen);
            }
            packetLen = NumberUtil.readInt(b, offset);
            offset += packetLen + 4;
          }

          headerLen = NumberUtil.readInt(b, offset);
          contentLen = NumberUtil.readInt(b, offset + 4 + headerLen);
          content = new String(b, offset + 4 + headerLen + 4,
                               contentLen);
        } else {
          return;
        }

        if (url != null && content != null
            && content.indexOf("User-agent") != -1) {
          long t1 = System.currentTimeMillis();
          String robots = robotsTxtParser.parserRobotsText(content);
          String host = URLUtil.getHost(url);
          byte h[] = MD5.digest8(host.getBytes()).getDigest();
          k.set(h);

          if (robots != null && !robots.equals("")) {
            KeyValue rkv = new KeyValue(h, R, r,
                                        robots.getBytes());
            context.write(k, rkv);
            context.getCounter("spider", "parse_succeed")
                .increment(1);
          } else {
            Log.info("host " + host + " empty robots" + content);
          }
          KeyValue kv = new KeyValue(h, R, s,
                                     content.getBytes());
          context.getCounter("spider", "robots").increment(1);
          context.write(k, kv);
          long t2 = System.currentTimeMillis();
          if (t2 - t1 > 1000) {
            System.out.println("time consume too long host=" + host
                               + " time=" + (t2 - t1));
          }
        }

        return;
      }
      // 下载失败 403禁止下载
      // TODO 403的响应码禁止下载所有的URL
      else {

      }

    }
  }

  static class RobotsProtocolReducer
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
    // JobConf job = new JobConf(conf, ParseResultConverterJob.class);

    int now = (int) (System.currentTimeMillis() / 1000);

    conf.set("serialNum", String.valueOf(now));

    Job job = new Job(conf, "robots_protocol_import");
    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    FileInputFormat.addInputPath(job, in);

    FileOutputFormat.setOutputPath(job, out);

    job.setMapperClass(RobotsProtocolMapper.class);
    job.setReducerClass(RobotsProtocolReducer.class);
    // conf.setBoolean("url_and_host", true);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);
    job.setOutputFormatClass(HFileOutputFormat.class);

    String tableName = conf.get("importTable", "hostDB");
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
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
