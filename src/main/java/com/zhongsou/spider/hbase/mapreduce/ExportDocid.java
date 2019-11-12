package com.zhongsou.spider.hbase.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;

public class ExportDocid extends Configured implements MapreduceV2Job {

  public ExportDocid() {
    Configuration conf = HBaseConfiguration.create();
    this.setConf(conf);
  }

  public static class ExportDocidMapper
      extends TableMapper<ImmutableBytesWritable, NullWritable> {
    byte[] f = Bytes.toBytes("F");
    byte[] u = Bytes.toBytes("u");
    byte[] h = Bytes.toBytes("H");
    byte[] s = Bytes.toBytes("s");
    byte[] p = Bytes.toBytes("P");
    byte[] finger = Bytes.toBytes("f");

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
      context.getCounter("scanhbase", "url_count").increment(1);

    }
  }

  public static class ExportDocidReducer
      extends Reducer<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, NullWritable> {

    @Override
    protected void reduce(ImmutableBytesWritable key,
                          Iterable<NullWritable> values,
                          Context context)
        throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }

  }

  static String convertScanToString(Scan scan)
      throws IOException {
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
      scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("s"));

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
      conf.setBoolean("mapred.output.compress", true);
      conf.set("mapred.output.compression.type", "BLOCK");
      conf.set("mapred.output.compression", "org.apache.hadoop.io.compress.SnappyCodec");

      conf.set(TableInputFormat.SCAN, convertScanToString(scan));
      Job job = new Job(conf);

      TableMapReduceUtil.initTableMapperJob(tableName,
                                            scan,
                                            ExportDocidMapper.class,
                                            ImmutableBytesWritable.class,
                                            NullWritable.class, job);

      FileOutputFormat.setOutputPath(job, new Path(args[0]));
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      job.setMapSpeculativeExecution(false);
      job.setReduceSpeculativeExecution(true);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setOutputKeyClass(ImmutableBytesWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setReducerClass(ExportDocidReducer.class);
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
      Job job = ExportDocid.createSubmitJob(conf, otherArgs);
      if (otherArgs.length < 1) {
        System.out.println("Wrong number of arguments: " + otherArgs.length +
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

  @Override
  public Job createRunnableJob(String[] args) {
    String[] otherArgs;
    try {
      otherArgs = new GenericOptionsParser(this.getConf(), args).getRemainingArgs();
      Job job = ExportDocid.createSubmitJob(this.getConf(), otherArgs);
      return job;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

}
