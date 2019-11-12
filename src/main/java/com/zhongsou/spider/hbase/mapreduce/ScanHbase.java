package com.zhongsou.spider.hbase.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zhongsou.spider.common.url.RobotsFilter;
import com.zhongsou.spider.common.url.URLFilter;
import com.zhongsou.spider.common.util.NumberUtil;

enum Duplidate {
  url;
}

public class ScanHbase {
  public static class ScanMapper extends TableMapper<Text, Text> {
    byte[] f = Bytes.toBytes("F");
    byte[] u = Bytes.toBytes("u");
    byte[] h = Bytes.toBytes("H");
    byte[] s = Bytes.toBytes("s");
    byte[] p = Bytes.toBytes("P");
    byte[] finger = Bytes.toBytes("f");
    // URLFilters filters = null;
    URLFilter filter = null;

    @Override
      protected void setup(Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      // filters = new URLFilters(context.getConfiguration());
      filter = new RobotsFilter();
      filter.setConf(context.getConfiguration());
    }
    
    @Override
      protected void map(ImmutableBytesWritable key, Result value,
                         Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      //
      // byte a[] = value.getValue(p, finger);
      // ImmutableBytesWritable v = new ImmutableBytesWritable();
      // v.set(a);
      // context.write(key, v);
      // ImmutableBytesWritable url = new ImmutableBytesWritable();
      // ImmutableBytesWritable html = new ImmutableBytesWritable();
      // int i = 0;
      // for (KeyValue kv : value.raw()) {
      // byte a[] = kv.getBuffer();
      // if (new String(kv.getFamily()).equals("F") && new
      // String(kv.getQualifier()).equals("u")) {
      // url.set(a, kv.getValueOffset(), kv.getValueLength());
      // i++;
      // } else if (new String(kv.getFamily()).equals("H") && new
      // String(kv.getQualifier()).equals("s")) {
      // if (kv.getValueLength() < 10240) {
      // html.set(a, kv.getValueOffset(), kv.getValueLength());
      // i++;
      // } else {
      // context.getCounter("scanhbase", "count").increment(1);
      // }
      // }
      // }
      // if (i == 2) {
      // context.write(url, html);
      // context.getCounter("scanhbase", "small_count").increment(1);
      // Log.info("url=" + new String(url.get(), url.getOffset(),
      // url.getLength()));
      // }
      // context.write(key, NullWritable.get());
      context.getCounter("scanhbase", "url_count").increment(1);

      byte a[] = value.getValue(f, u);
      try {
        if (a == null) {
          context.getCounter("scan_hbase", "empty_url").increment(1);
          return;
        }
        String url = new String(a);
        try {
          String m = filter.filter(url);
          if (m == null) {
            context.write(
                new Text(NumberUtil.getHexString(key.get())),
                new Text(url));
          }
        } catch (Exception e) {
          // context.write(key, NullWritable.get());
          context.getCounter("scan_hbase", "error_url").increment(1);
        }
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      // if (key.getLength() > 8 || key.get().length > 8) {
      // context.getCounter("scanhbase", "invalide_docid").increment(1);
      // context.write(key, NullWritable.get());
      // Log.info("key array length" + key.get().length +
      // "\tkey actual length=" + key.getLength() + "\t" +
      // NumberUtil.getHexString(key.get()));
      // return;
      // }

    }

  }

  public static class ScanReducer extends Reducer<Text, Text, Text, Text> {

    @Override
      protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text v : values) {
        context.write(key, v);
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
      String tableName = conf.get("tableName", "webDB");
      scan.setCaching(500);
      scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("u"));
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
      conf.setBoolean("mapred.compress.map.output", true);
      conf.set("mapred.map.output.compression.codec",
               "org.apache.hadoop.io.compress.SnappyCodec");
      // conf.setBoolean("mapred.output.compress", true);
      // conf.set("mapred.output.compression.type", "BLOCK");
      // conf.set("mapred.output.compression",
      // "org.apache.hadoop.io.compress.SnappyCodec");
      URI hosturl = new URI(
          "hdfs://hadoop-master-83:9900/user/kaifa/robots_output/part-r-00000"
          + "#part-r-00000");
      URI domainurl = new URI(
          "hdfs://hadoop-master-83:9900/user/kaifa/robots_output/robotidx-r-00000"
          + "#robotidx-r-00000");
      DistributedCache.addCacheFile(hosturl, conf);
      DistributedCache.addCacheFile(domainurl, conf);
      DistributedCache.createSymlink(conf);
      conf.setBoolean("useDistributeCache", true);
      conf.set("robot_idx_name", "robotidx-r-00000");
      conf.set("robot_file_name", "part-r-00000");
      conf.set(TableInputFormat.SCAN, convertScanToString(scan));
      Job job = new Job(conf);

      TableMapReduceUtil.initTableMapperJob(tableName, scan,
                                            ScanMapper.class, Text.class, NullWritable.class, job);

      FileOutputFormat.setOutputPath(job, new Path(args[0]));
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setMapSpeculativeExecution(false);
      job.setReducerClass(ScanReducer.class);

      // int numreduce = conf.getInt("mapred.reduce.tasks", 0);
      // job.setNumReduceTasks(numreduce);
      return job;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void main(String args[]) {
    Configuration conf = HBaseConfiguration.create();
    try {
      String[] otherArgs = new GenericOptionsParser(conf, args)
                           .getRemainingArgs();
      Job job = ScanHbase.createSubmitJob(conf, otherArgs);
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
