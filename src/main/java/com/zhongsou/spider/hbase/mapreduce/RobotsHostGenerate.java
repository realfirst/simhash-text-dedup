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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.NumberUtil;

public class RobotsHostGenerate {
  public static class HostMapper
      extends TableMapper<Text, NullWritable> {
    byte [] H = Bytes.toBytes("H");
    byte [] h = Bytes.toBytes("h");
    byte [] o = Bytes.toBytes("o");
    //          HashSet<String> domainSet = new HashSet<String>();
    //          HashSet<String> hostSet = new HashSet<String>();
    int robotsTheold;
    
    @Override
      protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);

      //                        File hostfile = new File("blackhost.txt");
      //                        File domainfile = new File("blackdomain.txt");
      //                        BufferedReader reader = null;
      //                        if (hostfile.exists()) {
      //                                reader = new BufferedReader(new InputStreamReader(
      //                                                new FileInputStream(hostfile)));
      //                                String line = null;
      //                                while ((line = reader.readLine()) != null) {
      //                                        hostSet.add(line.trim());
      //                                }
      //                                reader.close();
      //                        }
      //
      //                        if (domainfile.exists()) {
      //                                reader = new BufferedReader(new InputStreamReader(
      //                                                new FileInputStream(domainfile)));
      //                                String line = null;
      //                                while ((line = reader.readLine()) != null) {
      //                                        domainSet.add(line.trim());
      //                                }
      //                                reader.close();
      //                        }
      //
      //                        Log.info("load host size=" + hostSet.size() + "\tdomain size="
      //                                        + domainSet.size());
      robotsTheold = context.getConfiguration().getInt("robotsTheold", 10);
    }

    @Override
      protected void map(ImmutableBytesWritable key,
                         Result value,
                         Context context)
        throws IOException, InterruptedException {
      byte bhost[] = value.getValue(H, h);

      byte urlnum[]=value.getValue(H, o);

      int m=NumberUtil.readInt(urlnum,0);
      if(m<robotsTheold)
        return ;
      String host = new String(bhost);
      context.write(new Text(host), NullWritable.get());
      //                        String host = new String(bhost);
      //                        String domain = URLUtil.getDomainByHost(host);
      //                        int t = host.lastIndexOf(domain);
      //                        String levelOneHost = null;
      //                        if (t > 2) {
      //                                int dotpos = host.lastIndexOf(".", t - 2);
      //                                if (dotpos != -1) {
      //                                        levelOneHost = host.substring(dotpos + 1);
      //                                } else {
      //                                        levelOneHost = host;
      //                                }
      //                        }
      //
      //                        if (domainSet.contains(domain)) {
      //                                if (levelOneHost != null) {
      //                                        context.write(new Text(levelOneHost), NullWritable.get());
      //                                }
      //
      //                        } else if (levelOneHost != null && hostSet.contains(levelOneHost)) {
      //                                context.write(new Text(levelOneHost), NullWritable.get());
      //                        } else {
      //                                if (levelOneHost != null) {
      //                                        context.write(new Text(levelOneHost), NullWritable.get());
      //                                } else {
      //                                        context.write(new Text(host), NullWritable.get());
      //                                }
      //                        }

    }
  }

  public static class HostReducer
      extends Reducer<Text, NullWritable, ImmutableBytesWritable, Text> {
    byte [] buffer = new byte[2048];
    ImmutableBytesWritable o = new ImmutableBytesWritable();

    @Override
      protected void reduce(Text key,
                            Iterable<NullWritable> values, Context context)
        throws IOException, InterruptedException {
      String host=key.toString();
      //
      //                        int urlLen = *(int *) (value.c_str() + 48 + sizeof(time_t));
      //                        url = value.substr(52 + sizeof(time_t), urlLen);

      String url="http://"+host+"/robots.txt";
      byte [] urlbytes = url.getBytes();
      byte [] md5 = MD5.digest8(urlbytes, 0, urlbytes.length).getDigest();
      o.set(md5);
      byte lenbytes[]=NumberUtil.convertIntToC(urlbytes.length);
      System.arraycopy(lenbytes, 0, buffer, 44, 4);
      System.arraycopy(urlbytes, 0, buffer, 48, urlbytes.length);
      context.write(o, new Text(new String(buffer, 0, 48 + urlbytes.length)));
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
        scan.addColumn(Bytes.toBytes("H"), Bytes.toBytes("h"));
        scan.addColumn(Bytes.toBytes("H"), Bytes.toBytes("o"));
      }

      if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
        scan.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
      }
      if (conf.get(TableInputFormat.SCAN_MAXVERSIONS) != null) {
        scan.setMaxVersions(Integer.parseInt(conf.get(TableInputFormat.SCAN_MAXVERSIONS)));
      } else {
        scan.setMaxVersions(1);
      }

      //                        URI hosturl = new URI("file:///home/kaifa/xiqi/blackhost.txt"
      //                                        + "#blackhost.txt");
      //                        URI domainurl = new URI("file:///home/kaifa/xiqi/blackdomain.txt"
      //                                        + "#blackhost.txt");
      //                        DistributedCache.addCacheFile(hosturl, conf);
      //                        DistributedCache.addCacheFile(domainurl, conf);
      //                        DistributedCache.createSymlink(conf);

      // scan.setMaxVersions(1);
      conf.setBoolean("mapred.compress.map.output", true);
      conf.set("mapred.map.output.compression.codec",
               "org.apache.hadoop.io.compress.SnappyCodec");
      conf.setBoolean("mapred.output.compress", false);


      conf.set(TableInputFormat.SCAN, convertScanToString(scan));
      Job job = new Job(conf);

      TableMapReduceUtil.initTableMapperJob(tableName, scan,
                                            HostMapper.class,
                                            Text.class,
                                            NullWritable.class, job);

      FileOutputFormat.setOutputPath(job, new Path(args[0]));
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      //                        job.setOutputFormatClass(TextOutputFormat.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setOutputKeyClass(ImmutableBytesWritable.class);
      job.setOutputValueClass(Text.class);
      job.setReducerClass(HostReducer.class);

      return job;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void main(String args[]) {
    Configuration conf = HBaseConfiguration.create();
    try {
      String[] otherArgs = new GenericOptionsParser(conf, args)
                           .getRemainingArgs();
      Job job = RobotsHostGenerate.createSubmitJob(conf, otherArgs);
      if (otherArgs.length < 1) {
        System.out
            .println("Wrong number of arguments: "
                     + otherArgs.length
                     + " usage: outputpath SingleColumnValueFilter('F','s',=,'binary:5',true,true)");
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
