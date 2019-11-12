package com.zhongsou.spider.hbase.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.NumberUtil;

public class RobotFileGenerate {

  public static class HostMapper
      extends TableMapper<Text, Text> {
    byte[] H = Bytes.toBytes("H");
    byte[] h = Bytes.toBytes("h");
    byte[] R = Bytes.toBytes("R");
    byte[] r = Bytes.toBytes("r");

    @Override
      protected void map(ImmutableBytesWritable key,
                         Result value,
                         Context context)
        throws IOException, InterruptedException {
      byte bhost[] = value.getValue(H, h);
      byte robots[] = value.getValue(R, r);
      if (bhost != null && robots != null && robots.length > 0) {
        context.write(new Text(bhost), new Text(robots));
      }
    }
  }

  public static class HostReducer
      extends Reducer<Text, Text, ImmutableBytesWritable, NullWritable> {
    byte[] buffer = new byte[2048];
    ImmutableBytesWritable o = new ImmutableBytesWritable();
    byte[] start = "########".getBytes();
    byte[] end = "@@@@@@@@".getBytes();
    private MultipleOutputs mos;
    long offset = 0;
    int len = 0;
    int currOffset = 0;
    byte[] idxbuffer = new byte[20];
    byte[] robotsbuffer = new byte[1024 * 1024 * 2];
    ImmutableBytesWritable kidx = new ImmutableBytesWritable();
    ImmutableBytesWritable kcontent = new ImmutableBytesWritable();

    @Override
      protected void setup(Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      mos = new MultipleOutputs(context);
      System.arraycopy(start, 0, robotsbuffer, 0, 8);
    }

    @Override
      protected void cleanup(Context context) throws IOException,
        InterruptedException {
      super.cleanup(context);
      mos.close();
    }

    /**
     * <pre>
     * 写robots协议到两个文件之中，一个idx文件，格式为：host_md5(8) + offset(8) + len(4) = 20
     * 另一个是内容文件：格式为：######## + host_md5(8) + host_len(4) + host + robots_len(4) + robots + @@@@@@@@
     * </pre>
     */

    @Override
      protected void reduce(Text key,
                            Iterable<Text> values,
                            Context context)
        throws IOException, InterruptedException {
      len = 8;
      String host = key.toString();
      String robots = values.iterator().next().toString();
      byte digest[] = MD5.digest8(host.getBytes()).getDigest();
      if (robots.getBytes().length > robotsbuffer.length) {
        context.getCounter("robots", "error_record").increment(1);
        return;
      }

      byte[] hostbyte = host.getBytes();
      byte[] robotsbyte = robots.getBytes();

      byte hostlen[] = NumberUtil.convertIntToC(hostbyte.length);
      System.arraycopy(digest, 0, robotsbuffer, len, 8);
      len += 8;
      System.arraycopy(hostlen, 0, robotsbuffer, len, 4);
      len += 4;
      System.arraycopy(hostbyte, 0, robotsbuffer, len, hostbyte.length);
      len += hostbyte.length;
      byte robotslen[] = NumberUtil.convertIntToC(robotsbyte.length);
      System.arraycopy(robotslen, 0, robotsbuffer, len, 4);
      len += 4;
      System.arraycopy(robotsbyte, 0, robotsbuffer, len, robotsbyte.length);
      len += robotsbyte.length;
      System.arraycopy(end, 0, robotsbuffer, len, 8);
      len += 8;

      System.arraycopy(digest, 0, idxbuffer, 0, 8);
      byte off[] = NumberUtil.convertLongToC(offset);
      System.arraycopy(off, 0, idxbuffer, 8, 8);
      byte lenbyte[] = NumberUtil.convertIntToC(len);
      System.arraycopy(lenbyte, 0, idxbuffer, 16, 4);

      offset += len;
      kcontent.set(robotsbuffer, 0, len);
      context.write(kcontent, NullWritable.get());
      kidx.set(idxbuffer);
      mos.write("robotidx", kidx, NullWritable.get());
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
      } else {
        SingleColumnValueFilter lff = new SingleColumnValueFilter(
            Bytes.toBytes("R"), Bytes.toBytes("r"),
            CompareFilter.CompareOp.NOT_EQUAL, "0".getBytes());
        lff.setFilterIfMissing(true);
        lff.setLatestVersionOnly(true);
        scan.setFilter(lff);
      }
      
      if (conf.get(TableInputFormat.SCAN_COLUMNS) != null) {
        addColumns(scan, conf.get(TableInputFormat.SCAN_COLUMNS));
      } else {
        scan.addColumn(Bytes.toBytes("H"), Bytes.toBytes("h"));
        scan.addColumn(Bytes.toBytes("R"), Bytes.toBytes("r"));
      }

      if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
        scan.addFamily(Bytes.toBytes(conf
                                     .get(TableInputFormat.SCAN_COLUMN_FAMILY)));
      }
      if (conf.get(TableInputFormat.SCAN_MAXVERSIONS) != null) {
        scan.setMaxVersions(Integer.parseInt(conf
                                             .get(TableInputFormat.SCAN_MAXVERSIONS)));
      } else {
        scan.setMaxVersions(1);
      }

      // URI hosturl = new URI("file:///home/kaifa/xiqi/blackhost.txt"
      // + "#blackhost.txt");
      // URI domainurl = new URI("file:///home/kaifa/xiqi/blackdomain.txt"
      // + "#blackhost.txt");
      // DistributedCache.addCacheFile(hosturl, conf);
      // DistributedCache.addCacheFile(domainurl, conf);
      // DistributedCache.createSymlink(conf);

      // scan.setMaxVersions(1);
      conf.setBoolean("mapred.compress.map.output", true);
      conf.set("mapred.map.output.compression.codec",
               "org.apache.hadoop.io.compress.SnappyCodec");
      conf.setBoolean("mapred.output.compress", false);

      conf.set(TableInputFormat.SCAN, convertScanToString(scan));
      Job job = new Job(conf);

      TableMapReduceUtil.initTableMapperJob(tableName, scan,
                                            HostMapper.class, Text.class, NullWritable.class, job);

      FileOutputFormat.setOutputPath(job, new Path(args[0]));
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      job.setOutputFormatClass(BinaryOutputFormat.class);
      MultipleOutputs.addNamedOutput(job, "robotidx",
                                     BinaryOutputFormat.class, ImmutableBytesWritable.class,
                                     NullWritable.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(ImmutableBytesWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setReducerClass(HostReducer.class);

      return job;
    } catch (IOException e) {
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
      Job job = RobotFileGenerate.createSubmitJob(conf, otherArgs);
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
