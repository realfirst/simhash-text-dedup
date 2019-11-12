package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.net.InetAddress;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.hadoop.HostSequenceFileInputFormat;
import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;
import com.zhongsou.spider.hadoop.jobcontrol.SelectAndSendJob;
import com.zhongsou.spider.hadoop.jobcontrol.SpiderJob;

/**
 *
 * 发送网页数据
 *
 * @author Xi Qi
 *
 */
public class SendWebPage extends Configured implements MapreduceV2Job {
  static Logger logger = SelectAndSendJob.logger;
  DateFormat format = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
  HTable table;
  HashMap<String, SequenceFile.Writer> writerMap;
  NavigableMap<HRegionInfo, ServerName> regionMap;

  byte[] lastStartKey;
  byte[] lastEndKey;
  SequenceFile.Writer lastWriter;

  public SendWebPage() {
    try {
      if (this.getConf() == null) {
        Configuration conf = HBaseConfiguration.create();
        this.table = new HTable(conf, "webDB");
      } else {
        this.table = new HTable(this.getConf(), "webDB");
      }
      regionMap = table.getRegionLocations();
      writerMap = new HashMap<String, SequenceFile.Writer>();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   *
   * 根据regionserver的位置，生成加载的输入的文件
   *
   * @return
   */
  private boolean createWriter() {
    try {
      FileSystem fs;
      fs = FileSystem.get(this.getConf());
      NavigableMap<HRegionInfo, ServerName> regionMap = table.getRegionLocations();
      int i = 0;
      String st = this.getConf().get("seqTimestamp");
      if (st == null || st.equals(""))
        return false;
      String loadlistDir = SpiderJob.ROOT_DIR + "/" + SpiderJob.SELECT_LOAD_DOCID_DIR + "/" + st;
      for (ServerName serverName : regionMap.values()) {
        if (!this.writerMap.containsKey(serverName.getHostname())) {
          String fileName = loadlistDir + "/" + String.valueOf(serverName.getHostname()) + "___" + (i++);
          Path lpath = new Path(fileName);
          logger.info("writer path " + fileName);
          SequenceFile.Writer loadWriter = SequenceFile.createWriter(fs, this.getConf(), lpath, ImmutableBytesWritable.class, ImmutableBytesWritable.class);
          this.writerMap.put(serverName.getHostname(), loadWriter);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public void setConf(Configuration conf) {
    // TODO Auto-generated method stub
    super.setConf(conf);

  }

  private HRegionInfo getDocidRegion(byte[] docid) {
    for (Map.Entry<HRegionInfo, ServerName> entry : this.regionMap.entrySet()) {
      HRegionInfo region = entry.getKey();
      byte[] start = region.getStartKey();
      byte[] end = region.getEndKey();

      if ((start.length == 0 || Bytes.compareTo(docid, start) >= 0) && (end.length == 0 || Bytes.compareTo(docid, end) < 0)) {
        return region;
      }

    }
    logger.info("error can not find appropriate region for docid " + NumberUtil.getHexString(docid));
    return null;
  }

  private void closeWriter() {

    for (Map.Entry<String, SequenceFile.Writer> entry : this.writerMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private SequenceFile.Writer getWriter(byte[] docid) {
    if (this.lastEndKey != null && this.lastStartKey != null) {
      if ((this.lastStartKey.length == 0 || Bytes.compareTo(docid, this.lastStartKey) >= 0) && (this.lastEndKey.length == 0 || Bytes.compareTo(docid, this.lastEndKey) < 0)) {
        return this.lastWriter;
      }
    }
    HRegionInfo info;
    info = this.getDocidRegion(docid);
    if (info != null) {
      this.lastEndKey = info.getEndKey();
      this.lastStartKey = info.getStartKey();
      ServerName name = this.regionMap.get(info);
      this.lastWriter = this.writerMap.get(name.getHostname());
      return this.lastWriter;
    }
    return null;
  }

  static public boolean isInRegion(HRegionInfo region, byte[] docid) {
    byte[] start = region.getStartKey();
    byte[] end = region.getEndKey();

    if ((start.length == 0 && docid.length == 0) || (end.length == 0 && docid.length == 0))
      return true;
    if ((start.length == 0 || Bytes.compareTo(docid, start) >= 0) && (end.length == 0 || Bytes.compareTo(docid, end) < 0)) {
      return true;
    }
    return false;
  }

  private byte[] findNextDocid(byte[] docid, HRegionInfo region) {
    byte[] nextDocid = Arrays.copyOf(docid, docid.length);
    for (int i = 7; i >= 0; i--) {
      if (nextDocid[i] == (byte) 0xff)
        continue;
      nextDocid[i] = (byte) 0xff;
      if (region.getEndKey().length != 0 && Bytes.compareTo(nextDocid, region.getEndKey()) >= 0) {
        logger.info("docid " + NumberUtil.getHexString(docid) + " find region end docid " + NumberUtil.getHexString(region.getEndKey()));
        return region.getEndKey();
      } else {
        logger.info("docid " + NumberUtil.getHexString(docid) + " find a next end docid " + NumberUtil.getHexString(nextDocid) + " region end docid "
                    + NumberUtil.getHexString(region.getEndKey()));
        return nextDocid;
      }
    }
    return region.getEndKey();
  }

  static class SendPageDataMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

    private int caching = 500;
    List<Row> rowlist = new ArrayList<Row>(100);
    Object result[] = null;
    HTable webDbTable;
    byte F[] = Bytes.toBytes("F");
    byte P[] = Bytes.toBytes("P");
    byte[] u = Bytes.toBytes("u");
    byte[] finger = Bytes.toBytes("f");
    byte[] sc = Bytes.toBytes("sc");
    byte[] srt = Bytes.toBytes("srt");
    byte[] sst = Bytes.toBytes("sst");
    byte[] t = Bytes.toBytes("t");
    byte[] ac = Bytes.toBytes("ac");
    byte[] cl = Bytes.toBytes("cl");
    byte[] su = Bytes.toBytes("su");
    byte[] la = Bytes.toBytes("la");
    byte[] buffer = new byte[1024 * 1024 * 2];

    ImmutableBytesWritable v = new ImmutableBytesWritable();
    Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>();

    byte[] namebuffer = new byte[8];
    int offset = 12;
    int process = 0;

    String hostName;

    @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
    }

    private void writeData(byte[] key, Result value, Context context) throws IOException, InterruptedException {
      Arrays.fill(buffer, (byte) '0');
      KeyValue[] kvs = value.raw();

      process = 0;

      System.arraycopy(key, 0, buffer, 4, 8);
      offset = 16;
      for (KeyValue kv : kvs) {
        // F 族
        if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(), F, 0, 1) == 0) {
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
        else if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(), P, 0, 1) == 0) {
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
      context.write(new ImmutableBytesWritable(key), v);
      context.getCounter("senddata", "load").increment(1);
    }

    @Override
      protected void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
      boolean find = false;
      for (HRegionInfo info : this.regionSet) {
        if (SendWebPage.isInRegion(info, key.get()) && SendWebPage.isInRegion(info, value.get())) {
          Log.info("get a region" + this.hostName + "\tregion start" + NumberUtil.getHexString(info.getStartKey()) + "\tregion end=" + NumberUtil.getHexString(info.getEndKey())
                   + "\t start=" + NumberUtil.getHexString(key.get()) + "\t end=" + NumberUtil.getHexString(value.get()));
          context.getCounter("hbase_region", this.hostName + "_match").increment(1);
          find = true;
          break;
        }

      }
      if (!find) {
        HRegionLocation location = this.webDbTable.getRegionLocation(key.get(), false);
        Log.info("did not find region" + "\tstart=" + NumberUtil.getHexString(key.get()) + "\t end=" + NumberUtil.getHexString(value.get()) + "\t region location " + location.getHostname());
        context.getCounter("hbase_region", this.hostName + "not_match").increment(1);
        NavigableMap<HRegionInfo, ServerName> tmap = this.webDbTable.getRegionLocations();

      }
      context.getCounter("hbase_region", this.hostName).increment(1);
      context.getCounter("hbase_region", "region").increment(1);
      scan.setStartRow(key.get());
      scan.setStopRow(value.get());
      ResultScanner scanner = this.webDbTable.getScanner(scan);
      scan.setCaching(this.caching);
      context.getCounter("scanCounter", "scanTime").increment(1);
      try {
     outer: for (Result result : scanner) {
          this.writeData(result.getRow(), result, context);
        }

      } finally {
        scanner.close();
      }

    }

    private Scan scan = new Scan();
    SingleColumnValueFilter filter;

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {

      super.setup(context);
      Configuration conf = context.getConfiguration();
      this.caching = conf.getInt("get_data_caching", 500);
      this.result = new Object[this.caching];
      String tableName = conf.get("LoadTableName", "webDB");
      this.webDbTable = new HTable(conf, tableName);
      int serialNum = Integer.parseInt(conf.get("serialNumber"));
      filter = new SingleColumnValueFilter(Bytes.toBytes("P"), Bytes.toBytes("la"), CompareOp.EQUAL, Bytes.toBytes(serialNum));
      filter.setFilterIfMissing(true);
      filter.setLatestVersionOnly(true);
      scan.addColumn(P, finger);
      scan.addColumn(P, sc);
      scan.addColumn(P, srt);
      scan.addColumn(P, sst);
      scan.addColumn(P, t);
      scan.addColumn(P, ac);
      scan.addColumn(P, su);
      scan.addColumn(P, la);
      scan.addColumn(P, cl);
      scan.addColumn(F, u);
      scan.setFilter(filter);

      NavigableMap<HRegionInfo, ServerName> tmap = this.webDbTable.getRegionLocations();

      InetAddress address = InetAddress.getLocalHost();
      hostName = address.getHostName();
      Log.info("map start ,host name " + hostName);
      for (Map.Entry<HRegionInfo, ServerName> entry : tmap.entrySet()) {
        if (entry.getValue().getHostname().equals(hostName)) {
          this.regionSet.add(entry.getKey());
        }
      }
      Log.info("host " + hostName + "\t get region " + this.regionSet.size());
    }

  }

  public static class DocidPartioner<K, V> extends Partitioner<K, V> implements Configurable {
    Configuration conf;
    int numOfreduce;

    @Override
      public Configuration getConf() {

      return this.conf;
    }

    @Override
      public void setConf(Configuration arg0) {

      this.conf = arg0;
      this.numOfreduce = conf.getInt("mapred.reduce.tasks", 1024);
    }

    // 1024个槽，取模
    @Override
      public int getPartition(K arg0, V arg1, int arg2) {
      ImmutableBytesWritable k = (ImmutableBytesWritable) arg0;

      byte ac[] = k.get();
      byte a = ac[k.getOffset() + k.getLength() - 1];
      byte b = ac[k.getOffset() + k.getLength() - 2];
      int t = a & 0x000000ff;
      int t2 = b & 0x00000003;
      int part = t2 * 256 + t;
      if (part < 0 || part > 1024) {
        Log.info("t2 =" + t2 + " t=" + t + " part=" + part);
      }
      return part;
    }

  }

  public static class SendPageDeleteMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

    byte one[] = new byte[1];
    ImmutableBytesWritable v = new ImmutableBytesWritable();

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {

      super.setup(context);
      v.set(one, 0, 1);
    }

    @Override
      protected void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
      context.write(key, v);
      context.getCounter("senddata", "delete").increment(1);
    }
  }

  public static class SendPageDataSendReducer extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, NullWritable> {
    private MultipleOutputs mos;

    @Override
      protected void setup(Context context) throws IOException, InterruptedException {

      super.setup(context);
      mos = new MultipleOutputs(context);
    }

    @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {

      super.cleanup(context);
      mos.close();
    }

    @Override
      protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {

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

  static Job createSendPageJob(Configuration conf, String args[]) {
    Job job = null;
    try {
      conf.setBoolean("mapred.compress.map.output", true);
      conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
      conf.setBoolean("mapred.output.compress", false);
      job = new Job(conf);
      String st = conf.get("seqTimestamp");
      if (st == null || st.equals(""))
        return null;
      String loadlistDir = SpiderJob.ROOT_DIR + "/" + SpiderJob.SELECT_LOAD_DOCID_DIR + "/" + st;
      // 根据加载列表的docid生成的，加载目录，目录之中按照regionsever的机器来写文件，一个文件包括这个regionserver之中所有region的起始docid，key是start
      // docid,value是end docid
      // 一对kv是一个region
      MultipleInputs.addInputPath(job, new Path(loadlistDir), HostSequenceFileInputFormat.class, SendPageDataMapper.class);
      MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, SendPageDeleteMapper.class);
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      job.setOutputFormatClass(BinaryOutputFormat.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(ImmutableBytesWritable.class);
      job.setMapSpeculativeExecution(false);
      job.setPartitionerClass(DocidPartioner.class);
      job.setOutputKeyClass(ImmutableBytesWritable.class);
      job.setOutputValueClass(NullWritable.class);
      MultipleOutputs.addNamedOutput(job, "deletelist", BinaryOutputFormat.class, ImmutableBytesWritable.class, NullWritable.class);
      job.setReducerClass(SendPageDataSendReducer.class);
      job.setNumReduceTasks(1024);
      return job;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  /**
   *
   * 生成加载的文件,一个regionserver进应于一个文件，k 是一个region的起的位置 ，v是这个region末尾的docid，
   * 满足如下关系region start key<=k<v<region end key
   *
   * @param loadFile
   * @return
   */
  boolean generateLoadListFile(String loadFile) {
    boolean flag = false;
    SequenceFile.Reader allReader = null;

    try {
      FileSystem fs;
      fs = FileSystem.get(this.getConf());
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      NullWritable v = NullWritable.get();
      allReader = new SequenceFile.Reader(fs, new Path(loadFile), this.getConf());

      // 将新加载的数据排序，将并且写入新的文件
      int numMap = this.getConf().getInt("mapred.map.tasks", 1);
      Log.info(" num of map task " + numMap);
      byte[] lastDocid = null;
      HRegionInfo lastRegion = null;
      byte[] lastStartKey = null;
      SequenceFile.Writer writer = null;
      ImmutableBytesWritable start = new ImmutableBytesWritable();
      ImmutableBytesWritable end = new ImmutableBytesWritable();
      int sum = 0;
      int separateSameRegionCount = this.getConf().getInt("sameRegionMax", 0);
      try {
        while (allReader.next(key, v)) {
          sum++;
          if (lastDocid == null) {
            lastDocid = key.copyBytes();
            lastStartKey = lastDocid;
            lastRegion = this.getDocidRegion(lastDocid);
            continue;
          }

          byte[] docid = key.get();
          if (isInRegion(lastRegion, docid)) {
            lastDocid = key.copyBytes();
            if (separateSameRegionCount > 0 && sum > separateSameRegionCount) {
              writer = this.getWriter(lastStartKey);
              start.set(lastStartKey);
              end.set(lastDocid);
              writer.append(start, end);
              lastStartKey = lastDocid;
              lastRegion = this.getDocidRegion(docid);
              sum = 0;
            }
            continue;
          } else {
            byte regionStopDocid[] = this.findNextDocid(lastDocid, lastRegion);
            logger.info("docid get a new writer " + NumberUtil.getHexString(docid) + " last region start=" + NumberUtil.getHexString(lastRegion.getStartKey()) + "\tlast region end="
                        + NumberUtil.getHexString(lastRegion.getEndKey()) + "\t part start key=" + NumberUtil.getHexString(lastStartKey) + "\tend key="
                        + NumberUtil.getHexString(regionStopDocid) + "\tsum=" + sum);
            writer = this.getWriter(lastStartKey);
            start.set(lastStartKey);
            end.set(regionStopDocid);
            writer.append(start, end);
            lastStartKey = key.copyBytes();
            lastDocid = lastStartKey;
            lastRegion = this.getDocidRegion(docid);
            sum = 0;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      if (sum > 0) {
        // at the end
        byte regionStopDocid[] = this.findNextDocid(lastDocid, lastRegion);

        logger.info("docid get a new writer " + NumberUtil.getHexString(lastDocid) + " last region start=" + NumberUtil.getHexString(lastRegion.getStartKey()) + "\tlast region end="
                    + NumberUtil.getHexString(lastRegion.getEndKey()) + "\t part start key=" + NumberUtil.getHexString(lastStartKey) + "\tend key=" + NumberUtil.getHexString(regionStopDocid));

        writer = this.getWriter(lastStartKey);
        start.set(lastStartKey);
        end.set(regionStopDocid);
        writer.append(start, end);
      }

      this.closeWriter();
      flag = true;

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return flag;
  }

  public static void main(String args[]) {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs;
    try {
      otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length < 3) {
        System.out.println("Wrong number of arguments: " + otherArgs.length + " modifylist loadlist output");
        System.exit(-1);
      }
      SendWebPage pa = new SendWebPage();
      pa.setConf(conf);
      pa.createWriter();
      boolean res = pa.generateLoadListFile(otherArgs[0]);
      if (!res)
        return;
      Job job = createSendPageJob(conf, otherArgs);
      job.waitForCompletion(true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Job createRunnableJob(String[] args) {
    // TODO Auto-generated method stub
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs;
    try {
      otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length < 3) {
        System.out.println("Wrong number of arguments: " + otherArgs.length + " modifylist loadlist output");
        System.exit(-1);
      }
      this.setConf(conf);
      this.createWriter();
      boolean res = this.generateLoadListFile(otherArgs[0]);
      if (!res)
        return null;

      Job job = createSendPageJob(conf, otherArgs);
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

}
