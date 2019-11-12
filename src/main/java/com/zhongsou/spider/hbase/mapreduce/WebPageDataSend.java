package com.zhongsou.spider.hbase.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.NumberUtil;

/**
 * 
 * 读取hbase的数据和删除列表的数据，输出二种数据文件
 * 
 * <pre>
 * 1 索引需要的加载的数据文件，按照docid取模，分成1024的槽,二进制文件 
 * 2 删除文件也要按照docid取模，分成1024个文件,二进制文件
 * </pre>
 * 
 * @author Xi Qi
 * 
 */

public class WebPageDataSend {
  /**
   * 
   * 读取hase的数据，转成索引需要的二进制格式
   * 
   * @author Xi Qi
   * 
   *         要读取的列： 1 从数据库之中取出数据，按照docid取模
   * 
   */
  public static class WebPageDataSendMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
    byte[] f = Bytes.toBytes("F");
    byte[] u = Bytes.toBytes("u");
    byte[] p = Bytes.toBytes("P");
    byte[] finger = Bytes.toBytes("f");
    byte[] sc = Bytes.toBytes("sc");
    byte[] srt = Bytes.toBytes("srt");
    byte[] sst = Bytes.toBytes("sst");
    byte[] t = Bytes.toBytes("t");
    byte[] ac = Bytes.toBytes("ac");
    byte[] cl = Bytes.toBytes("cl");
    byte[] su = Bytes.toBytes("su");
    byte[] la = Bytes.toBytes("la");
    byte[] one = NumberUtil.convertIntToC(3);
    byte[] two = NumberUtil.convertIntToC(4);
    byte[] three = NumberUtil.convertIntToC(5);

    byte[] buffer = new byte[1024 * 1024 * 2];

    ImmutableBytesWritable v = new ImmutableBytesWritable();

    byte[] namebuffer = new byte[8];
    int offset = 12;
    int process = 0;

    @Override
      protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      Arrays.fill(buffer, (byte) '0');
      KeyValue[] kvs = value.raw();

      process = 0;

      System.arraycopy(key.get(), 0, buffer, 4, 8);
      offset = 16;
      for (KeyValue kv : kvs) {
        // F 族
        if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(), f, 0, 1) == 0) {
          // F:u
          if (kv.getQualifierLength() == 1 && Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(), u, 0, 1) == 0) {
            if (offset + kv.getValueLength() + 12 > buffer.length) {
              Log.info("buffer to long docid=" + StringUtils.byteToHexString(kv.getKey()) + "\tlength=" + (offset + kv.getValueLength()));
              continue;
            }
            System.arraycopy(kv.getBuffer(), kv.getQualifierOffset(), buffer, offset, kv.getQualifierLength());
            offset += 8;
            int length = kv.getValueLength();
            byte valueLen[] = NumberUtil.convertIntToC(length);
            System.arraycopy(valueLen, 0, buffer, offset, valueLen.length);
            offset += valueLen.length;
            System.arraycopy(kv.getBuffer(), kv.getValueOffset(), buffer, offset, kv.getValueLength());
            offset += kv.getValueLength();
            process++;
          }

        }
        // P族
        else if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(), p, 0, 1) == 0) {
          if (offset + kv.getValueLength() + 12 > buffer.length) {
            Log.info("buffer to long docid=" + StringUtils.byteToHexString(kv.getKey()) + "\tlength=" + (offset + kv.getValueLength()));
            continue;
          }
          if (Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(), la, 0, la.length) == 0) {
            continue;
          }
          System.arraycopy(kv.getBuffer(), kv.getQualifierOffset(), buffer, offset, kv.getQualifierLength());
          offset += 8;
          int length = kv.getValueLength();
          byte valueLen[] = NumberUtil.convertIntToC(length);
          System.arraycopy(valueLen, 0, buffer, offset, valueLen.length);
          offset += valueLen.length;

          System.arraycopy(kv.getBuffer(), kv.getValueOffset(), buffer, offset, kv.getValueLength());
          offset += kv.getValueLength();
          process++;
        }
      }
      byte len[] = NumberUtil.convertIntToC(process);
      System.arraycopy(len, 0, buffer, 12, 4);

      byte sumlen[] = NumberUtil.convertIntToC(offset - 4);
      System.arraycopy(sumlen, 0, buffer, 0, 4);
      v.set(buffer, 0, offset);
      context.write(key, v);

    }
  }

  /**
   * 删除列表的文件处理文件
   * 
   * @author Xi Qi
   * 
   */
  public static class WebPageDeleteMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

    byte one[] = new byte[1];
    ImmutableBytesWritable v = new ImmutableBytesWritable();

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      v.set(one, 0, 1);
    }

    protected void map(ImmutableBytesWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      context.write(key, v);
    }
  }

  public static class DocidPartioner<K, V> extends Partitioner<K, V> implements Configurable {
    Configuration conf;
    int numOfreduce;

    @Override
      public Configuration getConf() {
      // TODO Auto-generated method stub
      return this.conf;
    }

    @Override
      public void setConf(Configuration arg0) {
      // TODO Auto-generated method stub
      this.conf = arg0;
      this.numOfreduce = conf.getInt("mapred.reduce.tasks", 1024);
    }

    // 1024个槽，取模
    @Override
      public int getPartition(K arg0, V arg1, int arg2) {
      ImmutableBytesWritable k = (ImmutableBytesWritable) arg0;
      // TODO Auto-generated method stub
      byte ac[] = k.get();
      byte a = ac[k.getOffset() + k.getLength() - 1];
      byte b = ac[k.getOffset() + k.getLength() - 2];
      int t = a & 0x000000ff;
      int t2 = b & 0x00000003;
      int part = t2 * 256 + t;
      if (part < 0 || part >= 1024) {
        Log.info("error " + t + " t2=" + t2 + "\t" + part);
      }
      return part;
    }

  }

  public static class WebPageDataSendReducer extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, NullWritable> {
    private MultipleOutputs mos;

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      mos = new MultipleOutputs(context);
    }

    @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.cleanup(context);
      mos.close();
    }

    @Override
      protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub

      ImmutableBytesWritable v = null;
      Iterator<ImmutableBytesWritable> it = values.iterator();
      while (it.hasNext()) {
        v = it.next();
        if (v.getLength() == 1) {
          mos.write("deletelist", key, NullWritable.get());
        } else {
          context.write(v, NullWritable.get());
        }
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
			
      if (conf.get("serialNum") != null) {
        int serialNum = Integer.parseInt(conf.get("serialNum"));
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("P"), Bytes.toBytes("la"), CompareOp.EQUAL, Bytes.toBytes(serialNum));
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        scan.setFilter(filter);
      } else {
        System.out.println("must give serialNum ");
        return null;
      }
      if (conf.get(TableInputFormat.SCAN_COLUMNS) != null) {
        addColumns(scan, conf.get(TableInputFormat.SCAN_COLUMNS));
      } else {
        addColumns(scan, "F:u P:su P:sc P:sst P:f P:t P:srt P:ac P:cl P:la");
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

      conf.set(TableInputFormat.SCAN, convertScanToString(scan));
      scan.setCacheBlocks(false);

      Job job = new Job(conf);

      job.setJobName("exportLoadPageDataFromHbase");
      // 删除列表,多个删除列表，一个是选择逻辑的删除列表
      if (args.length > 1) {
        int len = args.length;
        for (int i = 1; i < len; i++) {
          MultipleInputs.addInputPath(job, new Path(args[i]), SequenceFileInputFormat.class, WebPageDeleteMapper.class);
        }

      }
      TableMapReduceUtil.initTableMapperJob(tableName, scan, WebPageDataSendMapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
      FileOutputFormat.setOutputPath(job, new Path(args[0]));
      job.setOutputFormatClass(BinaryOutputFormat.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(ImmutableBytesWritable.class);
      job.setPartitionerClass(DocidPartioner.class);
      job.setOutputKeyClass(ImmutableBytesWritable.class);
      job.setOutputValueClass(NullWritable.class);
      MultipleOutputs.addNamedOutput(job, "deletelist", BinaryOutputFormat.class, ImmutableBytesWritable.class, NullWritable.class);
      job.setReducerClass(WebPageDataSendReducer.class);
      job.setNumReduceTasks(1024);
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
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      Job job = WebPageDataSend.createSubmitJob(conf, otherArgs);
      if (otherArgs.length < 1) {
        System.out.println("Wrong number of arguments: " + otherArgs.length + " usage: outputpath ");
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
