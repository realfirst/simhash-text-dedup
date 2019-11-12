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
import org.apache.hadoop.util.StringUtils;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;
import com.zhongsou.spider.hbase.mapreduce.StatisticalURL.OldURLReducer;
import com.zhongsou.spider.hbase.mapreduce.StatisticalURL.SrcURLDocidMapper;
import com.zhongsou.spider.hbase.mapreduce.StatisticalURL.SrcURLMetaMapper;

public class StatisticOldURL extends Configured implements MapreduceV2Job {
  static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

  static class SrcURLMetaMapper
      extends
      Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
    ImmutableBytesWritable k2 = new ImmutableBytesWritable();

    @Override
    protected void map(ImmutableBytesWritable key,
                       ImmutableBytesWritable value, Context context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      context.write(key, value);
      context.getCounter("statisticurl", "urlmeta").increment(1);
      Log.info("url meta length=" + value.getLength() + "\t"
               + StringUtils.byteToHexString(value.get()) + "\tsize="
               + value.getSize());
    }

  }

  static class SrcURLDocidMapper
      extends
      Mapper<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
    ImmutableBytesWritable v2 = new ImmutableBytesWritable(new byte[0]);

    @Override
    protected void map(ImmutableBytesWritable key, NullWritable value,
                       Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      context.write(key, v2);
      context.getCounter("statisticurl", "srcurl").increment(1);
    }

  }

  static class OldURLReducer
      extends
      Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {
    LinkedList<KeyValue> kvList = new LinkedList<KeyValue>();

    byte[] F = Bytes.toBytes("F");
    byte[] a = Bytes.toBytes("a");
    byte[] m = Bytes.toBytes("m");
    byte[] f = Bytes.toBytes("f");
    byte[] e = Bytes.toBytes("e");
    byte[] c = Bytes.toBytes("c");
    byte[] b = Bytes.toBytes("b");
    byte[] g = Bytes.toBytes("g");
    byte[] n = Bytes.toBytes("n");
    byte[] d = Bytes.toBytes("d");
    byte[] s = Bytes.toBytes("s");
    byte[] l = Bytes.toBytes("l");
    byte finger[] = new byte[8];
    Calendar lastCal = new java.util.GregorianCalendar();
    Calendar now = new java.util.GregorianCalendar();
    static long ONE_DAY_SECONDS = 24 * 60 * 60;
    static DateFormat dateFormat = new java.text.SimpleDateFormat(
        "yyyy-MM-dd HH:mm:ss");
    float modifyRation;
    float newurlRation;
    float oneMonthModifyRation;
    float oneMonthNewURLRation;
    float pageRankRation;
    int no_link_max_counts = 4;
    long no_link_max_time = 1L * 60 * 60 * 24 * 30;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      // TODO Auto-generated method stub
      modifyRation = context.getConfiguration().getFloat("modify_ration",
                                                         0.25f);
      newurlRation = context.getConfiguration().getFloat("newurl_ration",
                                                         0.25f);
      oneMonthModifyRation = context.getConfiguration().getFloat(
          "one_month_modify_ration", 0.25f);
      oneMonthNewURLRation = context.getConfiguration().getFloat(
          "one_month_newURL_ration", 0.25f);
      pageRankRation = context.getConfiguration().getFloat(
          "page_rank_ration", 0.5f);
      no_link_max_counts = context.getConfiguration().getInt(
          "no_new_link_times", 5);
      no_link_max_time = context.getConfiguration().getLong(
          "no_link_max_time", 1L * 60 * 60 * 24 * 30);
    }

    @Override
    protected void reduce(ImmutableBytesWritable key,
                          Iterable<ImmutableBytesWritable> values, Context context)
        throws IOException, InterruptedException {
      ImmutableBytesWritable meta = null;
      Iterator<ImmutableBytesWritable> e = values.iterator();
      int sum = 0;
      while (e.hasNext()) {

        ImmutableBytesWritable tmp = e.next();
        if (tmp.getLength() == 0) {
          sum++;
        } else {
          meta = new ImmutableBytesWritable();
          byte[] b = new byte[tmp.getLength()];
          System.arraycopy(tmp.get(), 0, b, 0, tmp.getLength());
          meta.set(b);
        }
      }

      if (meta == null) {
        Log.info("error key ,did not find corresponding key"
                 + StringUtils.byteToHexString(key.get()));
        return;
      } else {
        Log.info("find reduce key="
                 + StringUtils.byteToHexString(key.get()) + "\tsum="
                 + sum + " meta="
                 + StringUtils.byteToHexString(meta.get())
                 + "\tmeta length=" + meta.getLength());
      }

      // 元数据格式 newfinger(8)+oldfinger(8)+newURLToday(int
      // 4)+modifyCount(int 4)+errorCount(int
      // 4)+newURLBitmap(int 4)+modifyBitmap(int
      // 4)+lastModifyTime(long 8 c time second)

      boolean isChanged = false;

      int newURLToday = 0, modifyCount = 0, errorCount = 0, newURLBitmap = 0, modifyBitmap = 0, no_change_count = 0, type = 0;
      long lastModifyTime = 0;
      long nextDownloadTime = 0;
      float pageRank = 0;

      byte metaBytes[] = meta.get();
      isChanged = Bytes.compareTo(metaBytes, 0, 8, metaBytes, 8, 8) == 0 ? false
                  : true;
      lastModifyTime = NumberUtil.readLong(metaBytes, 36);
      no_change_count = NumberUtil.readInt(metaBytes, 44);
      type = NumberUtil.readInt(metaBytes, 48);

      newURLToday = NumberUtil.readInt(metaBytes, 16);
      modifyCount = NumberUtil.readInt(metaBytes, 20);
      errorCount = NumberUtil.readInt(metaBytes, 24);
      newURLBitmap = Bytes.toInt(metaBytes, 28);
      modifyBitmap = Bytes.toInt(metaBytes, 32);

      int today = now.get(Calendar.DAY_OF_MONTH);
      boolean isNewURL = false;
      if (lastModifyTime != 0) {
        // c time_t is seconds
        lastCal.setTimeInMillis(lastModifyTime * 1000);
        // 同一天
        if (lastCal.get(Calendar.DAY_OF_MONTH) == now
            .get(Calendar.DAY_OF_MONTH)
            && lastCal.get(Calendar.MONTH) == now
            .get(Calendar.MONTH)) {
          if (sum > 0) {
            newURLToday += sum;
            newURLBitmap = newURLBitmap | (1 << (32 - today));
          }
          if (isChanged) {
            modifyCount++;
            modifyBitmap = modifyBitmap | (1 << (32 - today));
          }
        } else {
          lastCal.add(Calendar.DAY_OF_MONTH, 1);
          while (lastCal.before(now)) {
            int day = lastCal.get(Calendar.DAY_OF_MONTH);
            newURLBitmap = newURLBitmap & ~(1 << (32 - day));
            modifyBitmap = modifyBitmap & ~(1 << (32 - day));
            lastCal.add(Calendar.DAY_OF_MONTH, 1);
          }

          if (sum > 0) {
            newURLToday = sum;
            newURLBitmap = newURLBitmap | (1 << (32 - today));
          } else {
            newURLToday = 0;
          }
          if (isChanged) {
            modifyCount = 1;
            modifyBitmap = modifyBitmap | (1 << (32 - today));
          } else {
            modifyCount = 0;
          }

        }

      }
      // 新的url
      else {
        isNewURL = true;
        if (sum > 0) {
          newURLToday = sum;
          newURLBitmap = newURLBitmap | (1 << (32 - today));
        } else {
          newURLToday = 0;
          newURLBitmap = 0;
        }
        if (isChanged) {
          modifyCount = 1;
          modifyBitmap = modifyBitmap | (1 << (32 - today));
        } else {
          modifyCount = 0;
          modifyBitmap = 0;
        }

      }

      if (isChanged) {
        lastModifyTime = System.currentTimeMillis() / 1000;
      }

      kvList.clear();
      // access time
      KeyValue kv = new KeyValue(
          key.get(),
          F,
          a,
          NumberUtil.convertIntToC((int) (System.currentTimeMillis() / 1000)));
      kvList.add(kv);
      if (isChanged) {
        // modify time
        kv = new KeyValue(key.get(), F, m,
                          NumberUtil.convertIntToC((int) (lastModifyTime)));
        kvList.add(kv);
        // simhash，如果发生修改，进行更新
        System.arraycopy(metaBytes, 0, finger, 0, 8);
        kv = new KeyValue(key.get(), F, f, finger);
        kvList.add(kv);

      }
      // 如果旧的errorCount大于0，这次清零

      if (errorCount > 0) {
        kv = new KeyValue(key.get(), F, this.e, Bytes.toBytes(String
                                                              .valueOf(0)));
        kvList.add(kv);
      }
      // modify count one day
      kv = new KeyValue(key.get(), F, c, Bytes.toBytes(String
                                                       .valueOf(modifyCount)));
      kvList.add(kv);
      // one month bitmap modify
      kv = new KeyValue(key.get(), F, b, Bytes.toBytes(modifyBitmap));
      kvList.add(kv);
      // one month bitmap new url
      kv = new KeyValue(key.get(), F, g, Bytes.toBytes(newURLBitmap));
      kvList.add(kv);
      // new new url one day
      kv = new KeyValue(key.get(), F, n, Bytes.toBytes(String
                                                       .valueOf(newURLToday)));
      kvList.add(kv);
      // calculate next time download time

      // status ,将新的url的标记更改为2，即变成下载过的标记
      if (type == 0) {
        KeyValue skv = new KeyValue(key.get(), F, s,
                                    Bytes.toBytes(String.valueOf("2")));
        kvList.add(skv);
      }
      // 目录页
      else if (type == 2) {
        boolean hasChange = false;
        if (newURLToday == 0) {
          no_change_count++;
          hasChange = true;
        }

        if (no_change_count > 0 && newURLToday > 0) {
          no_change_count = 0;
          hasChange = true;
        }

        // 当没有新链接的次数大于最大上限的次数，将其变成内容页
        if (no_change_count > this.no_link_max_counts
            || (System.currentTimeMillis() / 1000 - lastModifyTime) > this.no_link_max_time) {
          KeyValue skv = new KeyValue(key.get(), F, s,
                                      Bytes.toBytes(String.valueOf("1")));
          kvList.add(skv);
          KeyValue lkv = new KeyValue(key.get(), F, l,
                                      Bytes.toBytes(String.valueOf("0")));
          kvList.add(lkv);
          Log.info("url row key "
                   + NumberUtil.getHexString(key.get())
                   + "change status from link page to content page; no_change_count="
                   + no_change_count
                   + "\t last modify time="
                   + dateFormat.format(new java.util.Date(
                       lastModifyTime * 1000)));
        } else if (newURLToday > 0 && no_change_count > 0) {
          KeyValue lkv = new KeyValue(key.get(), F, l,
                                      Bytes.toBytes(String.valueOf("0")));
          kvList.add(lkv);

          Log.info("url row key "
                   + NumberUtil.getHexString(key.get())
                   + "change status from content page to link page ,new url today="
                   + newURLToday);
        } else {
          if (hasChange) {
            KeyValue lkv = new KeyValue(key.get(), F, l,
                                        Bytes.toBytes(String.valueOf(no_change_count)));
            kvList.add(lkv);
          }
        }

      }// 内容页
      else if (type == 1) {
        // 此页面发现新的url，将其变更为目录页
        if (newURLToday > 0) {
          KeyValue skv = new KeyValue(key.get(), F, s,
                                      Bytes.toBytes(String.valueOf("2")));
          kvList.add(skv);
          KeyValue lkv = new KeyValue(key.get(), F, l,
                                      Bytes.toBytes(String.valueOf("0")));
          kvList.add(lkv);
        } else {
          if (!isChanged) {
            no_change_count++;
          } else {
            no_change_count = 0;
          }
          KeyValue lkv = new KeyValue(key.get(), F, l,
                                      Bytes.toBytes(String.valueOf(no_change_count)));
          kvList.add(lkv);

        }

      } else {
        Log.info("error type " + type + " for key="
                 + NumberUtil.getHexString(key.get()));
      }

      if (isChanged) {
        float sumRate = 0;
        if (modifyCount > 0) {
          sumRate += this.modifyRation * Math.log(modifyCount + 1);
        }
        int oneMonthModifyCount = NumberUtil
                                  .bit_count_sparse(modifyBitmap);
        if (oneMonthModifyCount > 0) {
          sumRate += this.oneMonthModifyRation
                     * (oneMonthModifyCount / 30);
        }

        int oneMonthNewURLCount = NumberUtil
                                  .bit_count_sparse(newURLBitmap);
        if (oneMonthNewURLCount > 0) {
          sumRate += this.oneMonthNewURLRation
                     * (oneMonthNewURLCount / 30);
        }

        if (newURLToday > 0) {
          sumRate += this.newurlRation * (Math.log(newURLToday + 1));
        }

        if (pageRank > 0) {
          sumRate += this.pageRankRation * pageRank;
        }

        nextDownloadTime = System.currentTimeMillis() / 1000
                           + (long) ((ONE_DAY_SECONDS / sumRate));

      } else {

        float interval = (System.currentTimeMillis() / 1000 - lastModifyTime)
                         / ONE_DAY_SECONDS;
        nextDownloadTime = System.currentTimeMillis() / 1000 + 
                           (int) ((ONE_DAY_SECONDS) * (1 + Math.log(1 + interval)));

      }

      // 下一次下载的时间
      kv = new KeyValue(key.get(), F, d,
                        Bytes.toBytes((int) nextDownloadTime));
      kvList.add(kv);

      Log.info("final result:isChanged:="
               + isChanged
               + "\tlastModifyTime:="
               + dateFormat.format(new java.util.Date(
                   lastModifyTime * 1000))
               + "\tmodifyCount="
               + String.valueOf(modifyCount)
               + "\tmodifybitmap="
               + NumberUtil.getHexString(Bytes.toBytes(modifyBitmap))
               + "\tnewurlbitmap="
               + NumberUtil.getHexString(Bytes.toBytes(newURLBitmap))
               + "\tnewurltoday="
               + String.valueOf(newURLToday)
               + "\t nextdownloadtime="
               + dateFormat.format(new java.util.Date(
                   nextDownloadTime * 1000)));
      Collections.sort(kvList, KeyValue.COMPARATOR);
      for (KeyValue keyvalue : kvList) {
        context.write(key, keyvalue);
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

  @Override
  public Job createRunnableJob(String[] args) {
    // TODO Auto-generated method stub
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs;
    try {
      otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length < 3) {
        System.out.println("Wrong number of arguments: "
                           + otherArgs.length + " usage:");
        System.exit(-1);
      }
      Job job = createOldURLJob(conf, otherArgs);
      return job;
    } catch (IOException e) { // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) { // TODO Auto-generated
      e.printStackTrace();
    }
    return null;
  }

  static Job createOldURLJob(Configuration conf, String args[]) {
    Job job = null;
    try {
      job = new Job(conf);
      MultipleInputs.addInputPath(job, new Path(args[0]),
                                  SequenceFileInputFormat.class, SrcURLMetaMapper.class);
      MultipleInputs.addInputPath(job, new Path(args[1]),
                                  SequenceFileInputFormat.class, SrcURLDocidMapper.class);
      FileOutputFormat.setOutputPath(job, new Path(args[2]));

      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(ImmutableBytesWritable.class);
      job.setReducerClass(OldURLReducer.class);
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

}
