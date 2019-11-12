package com.zhongsou.incload;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

public class HFileCreatorMapper
    extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {
  private static final byte [] cf = Bytes.toBytes("P");
  private static final byte [] nf = Bytes.toBytes("n");
  private static int ts;
  private byte [] nfv;
  private KeyValue kvNf;
  
  @Override protected void setup(Context context) {
    ts = context.getConfiguration().getInt("timestamp", 0);
    nfv = Bytes.toBytes(ts);
    Log.info("value of ts:" + ts);
  }
  
  @Override protected void map(ImmutableBytesWritable key,
                               ImmutableBytesWritable value,
                               Context context)
      throws IOException, InterruptedException {
    kvNf = new KeyValue(key.get(), cf, nf, nfv);
    context.write(key, kvNf);
  }

  public static void main(String[] args) throws Exception {
    int i = 0;
    for (String arg : args) {
      System.out.println("arg " + i + ":" + arg);
      i++;
    }
    Configuration conf = new Configuration();

    conf.set("hbase.table.name", args[3]);
    int ts = (int) (System.currentTimeMillis() / 1000);
    conf.setInt("timestamp", ts);
    // Load hbase-site.xml 
    HBaseConfiguration.addHbaseResources(conf);

    Job job = new Job(conf, "HBase-Bulk-Import-" + ts);
    job.setJarByClass(HFileCreatorMapper.class);

    job.setMapperClass(HFileCreatorMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);

    HTable hTable = new HTable(conf, args[3]);
    
    // Auto configure partitioner and reducer
    HFileOutputFormat.configureIncrementalLoad(job, hTable);

    Path indir = new Path(args[1]);
    Path outdir = new Path(args[2]);

    FileInputFormat.addInputPath(job, indir);
    FileOutputFormat.setOutputPath(job, outdir);

    FileSystem fs = FileSystem.get(conf);
    fs.delete(outdir, true);
    
    job.waitForCompletion(true);

    // Load generated HFiles into table
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(new Path(args[2]), hTable);
  }
}
