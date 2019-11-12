package com.zhongsou.spider.hbase.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.common.util.URLUtil;
import com.zhongsou.spider.hadoop.HostInfo;

class HostInfoGroupComparator extends WritableComparator {

  protected HostInfoGroupComparator() {
    super(HostInfo.class, true);
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int i = compareBytes(b1, s1, 8, b2, s2, 8);
    return i;
  }

}

class HostInfoPartition extends Partitioner<HostInfo, Text> {

  @Override
    public int getPartition(HostInfo key, Text value, int numPartitions) {
    return Math.abs(Bytes.toString(key.getDomain()).hashCode() * 127) % numPartitions;
  }

}

public class HostStatistic {
  public static class ScanHostMapper extends TableMapper<HostInfo, Text> {
    byte[] H = Bytes.toBytes("H");
    byte[] h = Bytes.toBytes("h");

    HostInfo info = new HostInfo();

    @Override
      protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

      byte hostbytes[] = value.getValue(H, h);
      String host = new String(hostbytes);
      if (host.length() == 0 || host.getBytes().length != hostbytes.length) {
        Log.info("error host" + host + "\t src bytes=" + NumberUtil.getHexString(hostbytes));
        context.getCounter("hostDB", "errorHost").increment(1);
        return;
      }
      String domain = URLUtil.getDomainByHost(host);
      if (domain == null) {
        Log.info("domain is null" + host);
        return;
      }
      byte digest[] = MD5.digest8(domain.getBytes()).getDigest();
      info.setDomain(digest);
      String reverseHost = URLUtil.hostReverse(host);
      info.setHost(reverseHost.getBytes());
      context.write(info, new Text(info.getHost()));

    }
  }

  public static class ScanHostReducer extends Reducer<HostInfo, Text, Text, NullWritable> {

    private MultipleOutputs mos;

    int maxLevelOneHost = 1000;
    int maxLevelTwoHost = 100;

    HashSet<String> blcakHost = new HashSet<String>();

    @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.cleanup(context);
      mos.close();
    }

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      mos = new MultipleOutputs(context);
      this.maxLevelOneHost = context.getConfiguration().getInt("max_level_one_host", 2500);
      this.maxLevelTwoHost = context.getConfiguration().getInt("max_level_two_host", 500);

    }

    @Override
      protected void reduce(HostInfo key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      this.blcakHost.clear();
      Iterator<Text> it = values.iterator();
      String domain = "";
      String reverseDomain = "";
      String host = "";
      boolean first = true;
      String lastLevelOneHost = "";
      String lastLevelTwoHost = "";
      int levelOneHostNum = 0;
      int levelTwoHostNum = 0;

      while (it.hasNext()) {
        Text o = it.next();
        host = o.toString();
        if (first) {
          domain = URLUtil.getDomainByHost(URLUtil.hostReverse(o.toString()));
          reverseDomain = URLUtil.hostReverse(domain);
          first = false;
        }

        if (host.length() == reverseDomain.length() && host.equals(reverseDomain)) {
          if (!lastLevelOneHost.equals(host)) {
            lastLevelOneHost = host;
            levelOneHostNum++;
            levelTwoHostNum = 0;
            Log.info("find level one host " + host + "\t" + levelOneHostNum + "\t" + domain);
            if (levelOneHostNum > this.maxLevelOneHost) {
              Log.info("exceed max num of level one host" + domain + "\t" + this.maxLevelOneHost + "\t" + levelOneHostNum);
              mos.write("blackdomain", new Text(domain), NullWritable.get());
              return;
            }
          }

          continue;
        }

        int t = host.indexOf(reverseDomain);
        if (t == -1) {
          Log.info("error host " + host + "\t" + reverseDomain);
          context.getCounter("hoststat", "error_host").increment(1);
          continue;

        } else {
          if (host.length() <= reverseDomain.length() + 1) {
            continue;
          }

          int levelOneDot = host.indexOf(".", reverseDomain.length() + 1);
          // 找到是否有3二级域名

          if (levelOneDot == -1) {
            // 没有，此host是一个二级域名
            if (this.blcakHost.contains(host))
              continue;
            if (!lastLevelOneHost.equals(host)) {
              lastLevelOneHost = host;
              levelOneHostNum++;
              Log.info("find level one host " + lastLevelOneHost + "\tlevel one num=" + levelOneHostNum + "\tdomain=" + domain + "\tlastLeveltwoNum" + levelTwoHostNum);
              levelTwoHostNum = 0;
              if (levelOneHostNum > this.maxLevelOneHost) {
                Log.info("exceed max num of level one host" + domain + "\t" + this.maxLevelOneHost + "\t" + levelOneHostNum);
                mos.write("blackdomain", new Text(domain), NullWritable.get());
                return;
              }
            }

          } else {
            String levelOneHost = host.substring(0, levelOneDot);
            // 看二级域名是否已经在black的host之中
            if (this.blcakHost.contains(levelOneHost))
              continue;
            if (!levelOneHost.equals(lastLevelOneHost)) {
              lastLevelOneHost = levelOneHost;
              levelOneHostNum++;
              Log.info("find level one host " + lastLevelOneHost + "host=" + host + "\tlevel one num=" + levelOneHostNum + "\tdomain=" + domain + "\tlastLeveltwoNum" + levelTwoHostNum);
              levelTwoHostNum = 0;
              if (levelOneHostNum > this.maxLevelOneHost) {
                Log.info("exceed max num of level one host" + domain + "\t" + this.maxLevelOneHost + "\t" + levelOneHostNum);
                mos.write("blackdomain", new Text(domain), NullWritable.get());
                return;
              }

            }
            if (host.length() > levelOneHost.length() + 1) {
              int levelTwoDot = host.indexOf(".", levelOneHost.length() + 1);
              String levelTwoHost = "";
              if (levelTwoDot == -1) {
                levelTwoHost = host;
              } else {
                levelTwoHost = host.substring(0, levelTwoDot);
              }

              levelTwoHostNum++;
              Log.info("find level two host " + levelTwoHost + " host "  + host + "\t level one host=" + levelOneHost + " level one num=" + levelOneHostNum + "\tlevel two num="
                       + levelTwoHostNum + "\tdomain=" + domain);
              if (levelTwoHostNum > this.maxLevelTwoHost) {
                blcakHost.add(levelOneHost);
                Log.info("exceed max num of level two host " + levelOneHost + "\tlevel one num=" + levelOneHostNum + "\tlevel two num=" + levelTwoHostNum);
                context.write(new Text(URLUtil.hostReverse(lastLevelOneHost)), NullWritable.get());
                continue;
              }
            }
          }

        }
      }
      if (levelOneHostNum > this.maxLevelOneHost) {
        Log.info("exceed max num of level one host" + domain + "\t" + this.maxLevelOneHost + "\t" + levelOneHostNum);
        mos.write("blackdomain", new Text(domain), NullWritable.get());
        return;
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

  static Job createSubmitJob(Configuration conf, String args[]) {
    try {

      Scan scan = new Scan();
      String tableName = conf.get("tableName", "hostDB");
      scan.setCaching(500);
      // scan.addColumn(Bytes.toBytes("P"), Bytes.toBytes("F"));
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
          e1.printStackTrace();
        }
      }
      if (conf.get(TableInputFormat.SCAN_COLUMNS) != null) {
        addColumns(scan, conf.get(TableInputFormat.SCAN_COLUMNS));
      } else {
        addColumns(scan, "H:h");
      }

      if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
        scan.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
      }
      if (conf.get(TableInputFormat.SCAN_MAXVERSIONS) != null) {
        scan.setMaxVersions(Integer.parseInt(conf.get(TableInputFormat.SCAN_MAXVERSIONS)));
      }

      // scan.setMaxVersions(1);
      conf.setBoolean("mapred.compress.map.output", true);
      conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
      conf.setBoolean("mapred.output.compress", false);
      conf.set("mapred.output.compression.type", "BLOCK");
      conf.set("mapred.output.compression", "org.apache.hadoop.io.compress.SnappyCodec");

      conf.set(TableInputFormat.SCAN, convertScanToString(scan));
      Job job = new Job(conf);

      TableMapReduceUtil.initTableMapperJob(tableName, scan, ScanHostMapper.class, HostInfo.class, Text.class, job);

      FileOutputFormat.setOutputPath(job, new Path(args[0]));
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setMapOutputKeyClass(HostInfo.class);
      job.setMapOutputValueClass(Text.class);

      MultipleOutputs.addNamedOutput(job, "blackdomain", TextOutputFormat.class, Text.class, NullWritable.class);
      job.setGroupingComparatorClass(HostInfoGroupComparator.class);
      job.setPartitionerClass(HostInfoPartition.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);

      job.setReducerClass(ScanHostReducer.class);
      int numreduce = conf.getInt("mapred.reduce.tasks", 0);
      job.setNumReduceTasks(numreduce);
      return job;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void main(String args[]) {


    Configuration conf = HBaseConfiguration.create();
    try {
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      Job job = HostStatistic.createSubmitJob(conf, otherArgs);
      if (otherArgs.length < 1) {
        System.out.println("Wrong number of arguments: " +
                           otherArgs.length +
                           " usage: outputpath SingleColumnValueFilter('F','s',=,'binary:5',true,true)");
        System.exit(-1);
      }
      job.waitForCompletion(true);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

}
