package com.zhongsou.spider.hadoop.jobcontrol;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.mortbay.log.Log;

import com.zhongsou.incload.SpamPageGenerate;
import com.zhongsou.spider.bean.JobInfo;
import com.zhongsou.spider.bean.ParsedFingerInfo;
import com.zhongsou.spider.exception.EmptyParamValueException;
import com.zhongsou.spider.hadoop.FileInfo;
import com.zhongsou.spider.hadoop.HDFSUtil;

/**
 *
 * 选择逻辑与生成发件的任务 主要有以下的功能：
 *
 * <pre>
 * 1  将分析完成的hfile写入到hbase之中
 * 2  进行url指纹的排重
 * 3  进行选择,生成删除列表
 * 4  生成发送给索引的文件
 *
 * <pre>
 *
 *
 *
 *
 * @author Xi Qi
 *
 */

public class SelectAndSendJob extends SpiderJob {
  PriorityBlockingQueue<ParsedFingerInfo> finishedParsedQueue =
      new PriorityBlockingQueue<ParsedFingerInfo>();
  PriorityBlockingQueue<ParsedFingerInfo> finishedGenerateQueue =
      new PriorityBlockingQueue<ParsedFingerInfo>();
  public static Logger logger = Logger.getLogger(SelectAndSendJob.class);
  final static String URL_AND_FINGER_OUTPUT = ROOT_DIR + "/"
                                              + URL_AND_FINGER_DIR;
  final static String PARSED_CONVERT_OUTPUT = ROOT_DIR + "/"
                                              + PARSE_CONVERT_DIR;
  private List<FileInfo> fileList = new LinkedList<FileInfo>();
  int scanFolderInterval = conf.getInt("scan_interval", 1000 * 10);
  HTable webDBTable;
  boolean deleteHFile;
  int loadFailedSleepTime = conf.getInt("loadFailedSleep", 1000 * 60);
  int retryTimes = conf.getInt("failedRetryTimes", 30);
  HashMap<String, JobInfo> jobInfoMap;
  GenerateSendList generateLoadlist;
  SendContent sendContent;
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  TreeMap<ImmutableBytesWritable, ImmutableBytesWritable> newLoadMap
      = new TreeMap<ImmutableBytesWritable, ImmutableBytesWritable>(
          new ImmutableBytesWritable.Comparator());

  AtomicBoolean finishGetDataFromHbase = new AtomicBoolean(true);

  public SelectAndSendJob(Configuration conf) throws IOException {
    super(conf);
    webDBTable = new HTable(conf, "webDB");
    this.deleteHFile = conf.getBoolean("delete_hfile", false);
    this.jobInfoMap = jobMap.get(this.getClass().getName());
    generateLoadlist = new GenerateSendList();
    sendContent = new SendContent();
    this.running = true;
    // scanLoadFlagFolder();
    this.scanUrlAndFingerFolder();
    for (ParsedFingerInfo info : this.finishedGenerateQueue) {
      if (this.finishedParsedQueue.contains(info)) {
        this.finishedParsedQueue.remove(info);
      }
    }
    logger.info("initialize finished! sendQueue size "
                + this.finishedGenerateQueue.size() + "\t dup queue "
                + this.finishedParsedQueue.size());
  }

  @Override
  public void runJob() {
    Thread selectThread = new Thread(this.generateLoadlist);
    selectThread.start();
    Thread sendThread = new Thread(this.sendContent);
    sendThread.start();
    while (true) {
      try {
        Thread.sleep(1000 * 10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public int scanUrlAndFingerFolder() {
    lock.readLock().lock();
    this.fileList.clear();
    HDFSUtil.listFile(fs, URL_AND_FINGER_OUTPUT, fileList, false);
    for (FileInfo fileinfo : fileList) {
      try {
        String path = fileinfo.getPath();
        String seqtime = path.substring(path.lastIndexOf("/") + 1);
        String hfileOutput = PARSED_CONVERT_OUTPUT + "/" + seqtime;

        boolean exist = fs.exists(new Path(hfileOutput));
        if (!exist) {
          // String newurlAndfinger = path.replaceFirst(ROOT_DIR,
          // ROOT_DIR_BAK);
          // boolean res = HDFSUtil.renameFile(fs, path,
          // newurlAndfinger);
          logger.warn("url and finger " + path + " seq time "
                      + seqtime
                      + " can not find appropriate hfile output path "
                      + hfileOutput);
          // continue;
        }
        if (seqtime.matches("\\d{14}")) {
          if (path.matches("(?i)hdfs://.*")) {
            path = path.substring(path.indexOf("/", 9));
          }
          if (exist) {
            if (hfileOutput.matches("(?i)hdfs://")) {
              hfileOutput = hfileOutput.substring(hfileOutput
                                                  .indexOf("/", 9));
            }
          } else {
            hfileOutput = null;
          }
          ParsedFingerInfo info = new ParsedFingerInfo();
          info.setHfileOutputPath(hfileOutput);
          info.setSeqTime(seqtime);
          info.setUrlAndFingerPath(path);
          if (!this.finishedGenerateQueue.contains(info)) {
            this.finishedParsedQueue.add(info);
            logger.info("add a download finished folder to upload hfile flag queue,path="
                        + info
                        + "\tqueue size="
                        + this.finishedParsedQueue.size());
          }
        }

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    lock.readLock().unlock();
    return this.finishedParsedQueue.size();
  }

  public int scanLoadFlagFolder() {

    ArrayList<FileInfo> tlist = new ArrayList<FileInfo>();

    HDFSUtil.listFile(fs, ROOT_DIR + "/" + LOAD_FLAG_HFILE_OUTPUT_DIR,
                      tlist, false);

    for (FileInfo fileinfo : tlist) {
      try {
        String path = fileinfo.getPath();
        String seqtime = path.substring(path.lastIndexOf("/") + 1);
        String hfileOutput = PARSED_CONVERT_OUTPUT + "/" + seqtime;

        boolean exist = fs.exists(new Path(hfileOutput));
        if (!exist) {
          // String newurlAndfinger = path.replaceFirst(ROOT_DIR,
          // ROOT_DIR_BAK);
          // boolean res = HDFSUtil.renameFile(fs, path,
          // newurlAndfinger);
          logger.warn("url and finger " + path + " seq time "
                      + seqtime
                      + " can not find appropriate hfile output path "
                      + hfileOutput);
          // continue;
        }
        if (seqtime.matches("\\d{14}")) {
          if (path.matches("(?i)hdfs://.*")) {
            path = path.substring(path.indexOf("/", 9));
          }
          if (exist) {
            if (hfileOutput.matches("(?i)hdfs://")) {
              hfileOutput = hfileOutput.substring(hfileOutput
                                                  .indexOf("/", 9));
            }
          } else {
            hfileOutput = null;
          }
          ParsedFingerInfo info = new ParsedFingerInfo();
          info.setHfileOutputPath(hfileOutput);
          info.setSeqTime(seqtime);
          info.setUrlAndFingerPath(URL_AND_FINGER_OUTPUT + "/"
                                   + seqtime);
          info.setLoadFilePath(SpiderJob.ROOT_DIR + "/"
                               + LOAD_DOCID_DIR + "/" + seqtime);
          info.setNewURLAndFingerFile(SpiderJob.ROOT_DIR + "/"
                                      + NEW_URL_AND_FINGER_DIR + "/" + seqtime);
          this.finishedGenerateQueue.add(info);

          logger.info("add a duplicate succeed path to send queue,path="
                      + info
                      + "\tqueue size="
                      + this.finishedGenerateQueue.size());
        }

      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return this.finishedGenerateQueue.size();

  }

  @Override
  void checkFolderExists() {
  }

  class GenerateSendList implements Runnable {

    @Override
    public void run() {
      while (running) {

        while (finishedParsedQueue.size() == 0) { // indicate page parse is finished
          int m = scanUrlAndFingerFolder();
          if (m > 0)
            break;
          else {
            try {
              Thread.sleep(scanFolderInterval);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }

        ParsedFingerInfo info = finishedParsedQueue.poll();
        if (info == null) {
          logger.error("pop a empty url and finger ");
          continue;
        }

        // 如果数据获取线程还没全部从hbase之中抽取上一轮的数据，不加载的新的数据hbase之中
        while (finishGetDataFromHbase.get() == false) {
          try {
            Thread.sleep(1000 * 1);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        // 将分析的完的结果写入hbase，比如正文内容,标题,P族
        boolean upload = false;
        int failed = 0;
        if (info.getHfileOutputPath() != null) {
          do {
            upload = this.uploadContentHfile(info);
            if (upload)
              break;
            else {
              failed++;
              logger.info("load hfile " + info + " to hbase failed,try again");
              if (failed == retryTimes) {
                logger.error("wait a long time failed load data to hbase,quit");
                System.exit(0);
              }
              try {
                Thread.sleep(loadFailedSleepTime);
              } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              }
            }
          } while (!upload);
        }

        // 根据相同指纹的个数，杀掉相同指纹很多的docid，生成一个新的docid与finger的加载文件
        boolean generateNewLoad = false;
        failed = 0;
        do {
          generateNewLoad = this.generateSameFingerDeleteList(info);
          if (generateNewLoad)
            break;
          else {
            failed++;
            logger.info("generate new load file " + info + " file failed,,try again");
            if (failed == retryTimes) {
              logger.error("wait a long time failed generate spam page file,quit");
              System.exit(0);
            }
            try {
              Thread.sleep(loadFailedSleepTime);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

        } while (!generateNewLoad);

        // 生成近似指纹的重复对
        JobInfo generateInfo = jobInfoMap.get("generate_dup_pair");
        boolean generate_dup_pair = false;
        failed = 0;
        do {
          generate_dup_pair = this.generateSelectPair(generateInfo,
                                                      info);
          if (generate_dup_pair)
            break;
          else {
            failed++;
            logger.info("generate select pair " + info + " file failed, try again");
            if (failed == retryTimes) {
              logger.error("wait a long time failed generate duplicate pair file,quit");
              System.exit(0);
            }
            try {
              Thread.sleep(loadFailedSleepTime);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }

        } while (!generate_dup_pair);

        JobInfo selectInfo = jobInfoMap.get("select_dup_pair");
        boolean select_pair = false;

        // 选择上面的输出结果，生成状态修改列表与选择删除列表
        failed = 0;
        do {
          select_pair = this.selecteDocidPair(selectInfo,
                                              info.getSeqTime());
          if (select_pair)
            break;
          else {
            failed++;
            logger.info(" select pair " + info + " file failed, try again");
            if (failed == retryTimes) {
              logger.error("wait a long time failed for select pair ,quit");
              System.exit(0);
            }
            try {
              Thread.sleep(loadFailedSleepTime);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }

        } while (!select_pair);

        // 根据删除列表，生成加载列表
        boolean generateLoad = generateLoadListFile(info);
        failed = 0;
        while (!generateLoad) {
          logger.info("generate load file failed");
          generateLoad = generateLoadListFile(info);
          failed++;
          if (failed == retryTimes) {
            logger.error("wait a long time failed generate new load file,quit");
            System.exit(0);
          }
        }

        // 根据新生成的删除列表与状态修改列表和新生成的docid+finger的文件，生成hfile，将P:la打上相应的标记
        JobInfo loadInfo = jobInfoMap.get("generate_load_flag_hfile");
        boolean generate_load_file = false;
        failed = 0;
        do {
          generate_load_file = generateLoadFlagFile(loadInfo, info.getSeqTime());
          if (generate_load_file) {
            break;
          } else {
            failed++;
            logger.info("generate load hfile " + info + " file failed,,try again");
            if (failed == retryTimes) {
              logger.error("wait a long time failed generate load flag hfile,quit");
              System.exit(0);
            }
            try {
              Thread.sleep(loadFailedSleepTime);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

        } while (!generate_load_file);

        // 将生成的hfile的队列导入发送的线程
        while (finishedGenerateQueue.size() >= 1) {
          try {
            Thread.sleep(1000 * 10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        finishedGenerateQueue.add(info);
        // 等待上一轮的数据的从hbase取数据完成,
        finishGetDataFromHbase.set(false);
      }

    }

    public boolean uploadContentHfile(ParsedFingerInfo info) {
      boolean flag = false;
      try {
        logger.info("start load hfile to webDb table path=" +
                    info.getHfileOutputPath());
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(info.getHfileOutputPath()), webDBTable);
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("load hfile to hbase failed " + info.getHfileOutputPath());
        return false;
      }
      if (deleteHFile) {
        HDFSUtil.rmdirs(fs, info.getHfileOutputPath());
        logger.info("load succed delete hfile" + info.getHfileOutputPath());
      } else {
        String bak = info.getHfileOutputPath().replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
        boolean res = HDFSUtil.renameFile(fs, info.getHfileOutputPath(), bak);
        logger.info("load succed rename hfile " +
                    info.getHfileOutputPath() + "\t to " + bak + " res=" + res);
      }
      flag = true;
      return flag;
    }

    public boolean generateSameFingerDeleteList(ParsedFingerInfo info) {
      boolean flag = false;
      SpamPageGenerate p = new SpamPageGenerate(
          info.getUrlAndFingerPath(), info.getSeqTime());
      int res = p.process(conf, fs);
      if (res != -1) {
        String newLoadURLAndFingerFile = SpiderJob.ROOT_DIR + "/"
                                         + SpiderJob.NEW_URL_AND_FINGER_DIR + "/"
                                         + info.getSeqTime();
        try {
          if (fs.exists(new Path(newLoadURLAndFingerFile))) {
            logger.info("delete spam page succeed,delete spam"
                        + res + " new load file "
                        + newLoadURLAndFingerFile);
            info.setNewURLAndFingerFile(newLoadURLAndFingerFile);
            flag = true;
          } else {
            logger.error("new load file " + newLoadURLAndFingerFile
                         + " res=" + res);
          }
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } else {
        logger.error("delete spam page failed");
        return false;
      }
      return flag;
    }

    public boolean generateSelectPair(JobInfo jobinfo, ParsedFingerInfo info) {
      boolean flag = false;
      jobinfo.getParamMap().put("$seqTimestamp", info.getSeqTime());
      jobinfo.getParamMap().put("$urlid_finger_file",
                                info.getNewURLAndFingerFile());
      jobinfo.getParamMap().put("$job_name",
                                "generate_dup_pair_" + info.getSeqTime());
      String output = ROOT_DIR + "/" + GENERATE_DUP_PAIR_DIR + "/"
                      + info.getSeqTime();
      String load_finger_did_not_change = ROOT_DIR + "/"
                                          + LOAD_DOCD_FINGER_NOT_CHANGE + "/" + info.getSeqTime();
      jobinfo.getParamMap().put("$pair_output", output);
      try {
        boolean exist = fs.exists(new Path(output));
        if (exist) {
          HDFSUtil.rmdirs(fs, output);
        }
        String comand = jobinfo.generateCommand();
        logger.info("start genereate dup pair ,command:" + comand);
        Job job = (Job) (JobCreateFactory.createJobObject(jobinfo));
        job.waitForCompletion(true);
        if (job.isSuccessful()) {
          flag = true;
          if (HDFSUtil.mergeMultipleSeqFile2One(fs, conf, output
                                                + "/unloadKey*", load_finger_did_not_change,
                                                ImmutableBytesWritable.class, NullWritable.class)) {
            HDFSUtil.rmFiles(fs, output + "/unloadKey*");
          } else {
            logger.info("merge unload key file failed");
            HDFSUtil.rmFiles(fs, output + "/unloadKey*");
          }
        } else {
          logger.error("generate dup apir failed");
        }
      } catch (EmptyParamValueException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }

      return flag;
    }

    /**
     *
     * 根据新的docid与指纹文件，和选择逻辑生成的删除列表，生成新的加载列表
     *
     *
     * @param seq
     * @return
     */
    private boolean generateLoadListFile(ParsedFingerInfo pinfo) {
      String seq = pinfo.getSeqTime();
      boolean flag = false;
      String newURLLoadFingerFile = SpiderJob.ROOT_DIR + "/" +
                                    SpiderJob.NEW_URL_AND_FINGER_DIR + "/" + seq;
      String selectDeleteFile = SpiderJob.ROOT_DIR + "/" +
                                SELECT_DOCID_DELETE_DIR + "/" + seq;
      String loadFile = SpiderJob.ROOT_DIR + "/" + LOAD_DOCID_DIR + "/" + seq;
      String unloadFile = ROOT_DIR + "/" + LOAD_DOCD_FINGER_NOT_CHANGE +
                          "/" + pinfo.getSeqTime();
      pinfo.setLoadFilePath(loadFile);
      newLoadMap.clear();
      SequenceFile.Reader allReader = null;
      SequenceFile.Reader deleteReader = null;
      SequenceFile.Reader unloadReader = null;
      SequenceFile.Writer loadWriter = null;

      try {
        Path urlloadPath = new Path(newURLLoadFingerFile);
        FileStatus status = fs.getFileStatus(urlloadPath);
        if (status.isDir()) {
          logger.error("new url and finger load file " +
                       newURLLoadFingerFile + " should be file");
          return false;
        }

        Path deletePath = new Path(selectDeleteFile);
        status = fs.getFileStatus(deletePath);
        if (!status.isDir()) {
          logger.error("select delete file " + selectDeleteFile +
                       " should be dir");
          return false;
        }

        int allNum = 0, fingernotchange = 0, duplicateDelete=0;
        ImmutableBytesWritable key = new ImmutableBytesWritable();
        ImmutableBytesWritable v = new ImmutableBytesWritable();
        allReader = new SequenceFile.Reader(fs, urlloadPath, conf);
        while (allReader.next(key, v)) {
          if (!newLoadMap.containsKey(key)) {
            ImmutableBytesWritable clone = new ImmutableBytesWritable();
            clone.set(key.copyBytes());
            ImmutableBytesWritable vclone = new ImmutableBytesWritable();
            vclone.set(v.copyBytes());
            newLoadMap.put(clone, vclone);
          }
        }
        allNum = newLoadMap.size();
        List<FileInfo> flist = new LinkedList<FileInfo>();
        HDFSUtil.listFile(fs, selectDeleteFile, flist, false);
        NullWritable nullobj = NullWritable.get();
        for (FileInfo info : flist) {
          if (!info.isDir()) {
            Log.info("process  finger delete list file " + info.toString());
            try {
              if (deleteReader != null)
                deleteReader.close();
              deleteReader = new SequenceFile.Reader(fs, new Path(info.getPath()), conf);
              while (deleteReader.next(key, v)) {
                if (newLoadMap.containsKey(key)) {
                  newLoadMap.removep(key);
                  duplicateDelete++;
                }
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }

        //去掉指纹相同的
        unloadReader = new SequenceFile.Reader(fs, new Path(unloadFile), conf);
        while (unloadReader.next(key, nullobj)) {
          if (newLoadMap.containsKey(key)) {
            newLoadMap.remove(key);
            fingernotchange++;
          }
        }

        loadWriter = SequenceFile.createWriter(fs, conf, new Path(
            loadFile), ImmutableBytesWritable.class,
                                               NullWritable.class);

        // 新加载的map
        for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry : newLoadMap
                 .entrySet()) {
          loadWriter.append(entry.getKey(), NullWritable.get());
        }
        flag = true;

        logger.info("generate load list file " + loadFile
                    + " succeed!new load list size=" + newLoadMap.size()
                    + "all num=" + allNum + " finger not change =" + fingernotchange+" duplicate docid="+duplicateDelete);

      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (allReader != null) {
          try {
            allReader.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }

        if (deleteReader != null) {
          try {
            deleteReader.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }

        if (unloadReader != null) {
          try {
            unloadReader.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }

        if (loadWriter != null) {
          try {
            loadWriter.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }

      }

      return flag;
    }

    public boolean generateLoadFlagFile(JobInfo jobinfo, String seq) {
      boolean flag = false;
      jobinfo.getParamMap().put("$seqTimestamp", seq);
      jobinfo.getParamMap().put("$job_name", "generate_load_flag_hfile_" + seq);
      String modifyList = ROOT_DIR + "/" + SELECT_DOCID_OUTPUT_DIR + "/" + seq;
      jobinfo.getParamMap().put("$modify_list", modifyList);
      int serialNum = SpiderJob.parseDateString(seq);
      jobinfo.getParamMap().put("$serialNumber",
                                String.valueOf(serialNum));

      String loadlist = SpiderJob.ROOT_DIR + "/" + LOAD_DOCID_DIR + "/"
                        + seq;
      jobinfo.getParamMap().put("$load_list", loadlist);

      String fingerFile = URL_AND_FINGER_OUTPUT + "/" + seq;
      jobinfo.getParamMap().put("$finger_list", fingerFile);
      String output = ROOT_DIR + "/" + LOAD_FLAG_HFILE_OUTPUT_DIR + "/"
                      + seq + "_bak";
      jobinfo.getParamMap().put("$hfile_output", output);

      try {
        if (fs.exists(new Path(output))) {
          HDFSUtil.rmdirs(fs, output);
        }
        String command = jobinfo.generateCommand();
        logger.info("start load flag hfile to hbase command=" + command);

        Job job = (Job) (JobCreateFactory.createJobObject(jobinfo));
        boolean res = job.waitForCompletion(true);
        if (!res) {
          logger.error("load load flag to hfile failed" + command);
          return false;
        }
        // 生成成功之后，将文件改名
        HDFSUtil.renameFile(fs, output, ROOT_DIR + "/"
                            + LOAD_FLAG_HFILE_OUTPUT_DIR + "/" + seq);
        flag = true;

      } catch (EmptyParamValueException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }
      return flag;
    }

    public boolean selecteDocidPair(JobInfo jobinfo, String seq) {
      boolean flag = false;

      jobinfo.getParamMap().put("$seqTimestamp", seq);
      jobinfo.getParamMap().put("$job_name", "select_dup_pair_" + seq);
      String input = ROOT_DIR + "/" + GENERATE_DUP_PAIR_DIR + "/" + seq;
      jobinfo.getParamMap().put("$select_input_dir", input);
      String output = ROOT_DIR + "/" + SELECT_DOCID_OUTPUT_DIR + "/" + seq;
      jobinfo.getParamMap().put("$select_output_dir", output);
      String blacklist = ROOT_DIR + "/" + SELECT_DOCID_DELETE_DIR + "/" + seq;
      try {
        boolean exist = fs.exists(new Path(output));
        if (exist) {
          HDFSUtil.rmdirs(fs, output);
        }
        String command = jobinfo.generateCommand();
        logger.info("select job start !command" + command);
        Job job = (Job) JobCreateFactory.createJobObject(jobinfo);
        boolean t = job.waitForCompletion(true);
        logger.info("select job finished! res=" + t);
        if (job.isSuccessful()) {
          flag = true;
          boolean res = HDFSUtil.moveFiles(fs, output + "/part*",
                                           blacklist);
          logger.info("move select delete load list to " + blacklist
                      + " res=" + res);
        }
      } catch (EmptyParamValueException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }

      return flag;
    }

  }

  class SendContent implements Runnable {

    public SendContent() {

    }

    @Override
    public void run() {
      while (running) {
        while (finishedGenerateQueue.size() == 0) {
          try {
            Thread.sleep(scanFolderInterval);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        ParsedFingerInfo info = finishedGenerateQueue.peek();
        if (info == null) {
          logger.error("get a null seq from finishedGenerateQueue");
          continue;
        }
        int failed = 0;
        do {
          // 开始抽取数据

          String output = ROOT_DIR + "/" + LOAD_FLAG_HFILE_OUTPUT_DIR
                          + "/" + info.getSeqTime();
          LoadIncrementalHFiles loader;
          try {
            loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(new Path(output), webDBTable);
            break;
          } catch (Exception e1) {
            e1.printStackTrace();
            logger.info("load hfile to hbase failed");
            failed++;
          }
          if (failed == retryTimes) {
            logger.error("load upload flag file to hbae reach max retry times ,abort;info="
                         + info);
          }
        } while (failed < retryTimes);

        // 生成索引所需要的数据加载文件与删除列表文件
        JobInfo sendJobInfo = jobInfoMap.get("generate_send_file");
        boolean generate_send_content = false;
        failed = 0;
        do {
          generate_send_content = generateSendDocContent(sendJobInfo,
                                                         info.getSeqTime());
          if (generate_send_content)
            break;
          else {
            failed++;
            logger.info("generate send content" + info
                        + " file failed,,try again");
            if (failed == retryTimes) {
              logger.error("wait a long time failed generate send file,quit");
              System.exit(0);
            }
            try {
              Thread.sleep(loadFailedSleepTime);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }

        } while (!generate_send_content);
        /*
         * String sendOutput = ROOT_DIR + "/" + SEND_CONTENT_DIR + "/" +
         * info.getSeqTime(); boolean splitfile =
         * splitSendContent(sendOutput); failed = 0; while (!splitfile)
         * { logger.info("split  send content file failed"); splitfile =
         * splitSendContent(sendOutput); failed++; if (failed ==
         * retryTimes) {
         * logger.error("wait a long time failed split file,quit");
         * System.exit(0); } }
         */

        logger.info("one round select and generate content file succeed,seq="
                    + info.getSeqTime());
        //开始清理各种文件，加写锁，防止扫描文件出现不一致的状态
        lock.writeLock().lock();
        // 清除此轮生成的各种临时文件
        // 1 原目录的urlid+finger
        String urlandfinger = info.getUrlAndFingerPath();
        String bakurlandfiner = urlandfinger.replaceFirst(ROOT_DIR,
                                                          ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, urlandfinger, bakurlandfiner);
        // 2 新的urlid+finger
        String newurlAndfinger = info.getNewURLAndFingerFile();
        String baknew = newurlAndfinger.replaceFirst(ROOT_DIR,
                                                     ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, newurlAndfinger, baknew);
        // 3 相同指纹杀掉的文件
        String sameDelete = SpiderJob.ROOT_DIR + "/"
                            + SpiderJob.SAME_FINGER_DOCID_DELETE_DIR + "/"
                            + info.getSeqTime();
        String bak = sameDelete.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, sameDelete, bak);

        // 4 选择逻辑杀掉的docid
        String selectDelete = ROOT_DIR + "/" + SELECT_DOCID_DELETE_DIR
                              + "/" + info.getSeqTime();
        String bakdelete = selectDelete.replaceFirst(ROOT_DIR,
                                                     ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, selectDelete, bakdelete);
        // 5 选择逻辑生成的状态修改列表

        String modifyDelete = ROOT_DIR + "/" + SELECT_DOCID_OUTPUT_DIR
                              + "/" + info.getSeqTime();
        String bakModify = modifyDelete.replaceFirst(ROOT_DIR,
                                                     ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, modifyDelete, bakModify);

        // 6 为hbase生成的加载的数据docid对
        String hbaseLoaddocid = SpiderJob.ROOT_DIR + "/"
                                + SpiderJob.SELECT_LOAD_DOCID_DIR + "/"
                                + info.getSeqTime();
        String bakLoaddocid = hbaseLoaddocid.replaceFirst(ROOT_DIR,
                                                          ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, hbaseLoaddocid, bakLoaddocid);

        // 7加载列表的docid
        String loadFile = SpiderJob.ROOT_DIR + "/" + LOAD_DOCID_DIR
                          + "/" + info.getSeqTime();
        String bakLoad = loadFile.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, loadFile, bakLoad);

        // 8 P:la hfile文件
        String hfileOutput = ROOT_DIR + "/"
                             + LOAD_FLAG_HFILE_OUTPUT_DIR + "/" + info.getSeqTime();
        String bakhfile = hfileOutput.replaceFirst(ROOT_DIR,
                                                   ROOT_DIR_BAK);
        if (deleteHFile) {
          HDFSUtil.rmdirs(fs, hfileOutput);
        } else {
          HDFSUtil.renameFile(fs, hfileOutput, bakhfile);
        }

        // 9 duplicate pair 生成的排重对

        String dupPair = ROOT_DIR + "/"
                         + SpiderJob.GENERATE_DUP_PAIR_DIR + "/"
                         + info.getSeqTime();
        String bakPair = dupPair.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, dupPair, bakPair);

        // 10 清除已加载并且指纹未修改的文件
        String fingerNotChange = ROOT_DIR + "/"
                                 + SpiderJob.LOAD_DOCD_FINGER_NOT_CHANGE + "/"
                                 + info.getSeqTime();
        String bakFile = fingerNotChange.replaceFirst(ROOT_DIR,
                                                      ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, fingerNotChange, bakFile);

        // 清理当前处理

        finishedGenerateQueue.remove(info);
        logger.info("remove " + info + " from send queue "
                    + finishedGenerateQueue.contains(info));
        lock.writeLock().unlock();
      }

    }

    public boolean generateSendDocContent(JobInfo jobinfo, String seq) {
      boolean flag = false;

      int serialNum = parseDateString(seq);
      jobinfo.getParamMap().put("$seqTimestamp", seq);
      jobinfo.getParamMap().put("$serialNumber",
                                String.valueOf(serialNum));

      jobinfo.getParamMap().put("$job_name", "generate_send_file_" + seq);
      String input = SpiderJob.ROOT_DIR + "/" + LOAD_DOCID_DIR + "/"
                     + seq;
      jobinfo.getParamMap().put("$load_file", input);
      String deletelist = ROOT_DIR + "/" + SELECT_DOCID_OUTPUT_DIR + "/"
                          + seq;
      jobinfo.getParamMap().put("$delete_file", deletelist);
      String output = ROOT_DIR + "/" + SEND_CONTENT_DIR + "/" + seq
                      + "_bak";
      jobinfo.getParamMap().put("$send_output", output);
      try {
        boolean exist = fs.exists(new Path(output));
        if (exist) {
          HDFSUtil.rmdirs(fs, output);
        }
        String command = jobinfo.generateCommand();
        logger.info("generate send content job start !command"
                    + command);
        Job job = (Job) JobCreateFactory.createJobObject(jobinfo);
        job.submit();
        while (!job.isComplete()) {
          float t = job.mapProgress();
          if (t == 1.0 && finishGetDataFromHbase.get() == false) {
            finishGetDataFromHbase.set(true);
          }
          Thread.sleep(1000);
        }
        boolean t = job.isSuccessful();
        logger.info("generate send content job finished! res=" + t);
        if (job.isSuccessful()) {
          flag = true;
          boolean res = HDFSUtil.renameFile(fs, output, ROOT_DIR
                                            + "/" + SEND_CONTENT_DIR + "/" + seq);
          logger.info("rename from " + output + " content output to"
                      + ROOT_DIR + "/" + SEND_CONTENT_DIR + "/" + seq
                      + res);
        }
      } catch (EmptyParamValueException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }

      return flag;

    }

  }

  private int getDocidMod(ImmutableBytesWritable k) {

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

  public boolean splitSendContent(String dataDir) {
    boolean flag = false;

    SequenceFile.Reader reader = null;
    int lastMod = -1;
    int sumSend = 0;
    int sumDelete = 0;
    int contentfileCreate = 0;
    int deleteFileCreate = 0;
    try {
      FileStatus st[] = fs.globStatus(new Path(dataDir + "/part*"));
      FSDataOutputStream fileOut = null;
      ImmutableBytesWritable k = new ImmutableBytesWritable();
      ImmutableBytesWritable v = new ImmutableBytesWritable();
      for (FileStatus s : st) {
        if (reader != null) {
          reader.close();
        }
        reader = new SequenceFile.Reader(fs, s.getPath(), conf);
        while (reader.next(k, v)) {
          sumSend++;
          int mod = this.getDocidMod(k);
          if (mod != lastMod) {
            Path file = new Path(dataDir + "/load-"
                                 + String.valueOf(mod));
            logger.debug("create new send file " + file);
            if (fileOut != null) {
              fileOut.close();
            }
            contentfileCreate++;
            fileOut = fs.create(file, false);
            lastMod = mod;
          }
          fileOut.write(v.get());
        }
      }
      if (reader != null)
        reader.close();
      if (fileOut != null) {
        fileOut.close();
      }

      st = fs.globStatus(new Path(dataDir + "/deletelist*"));
      for (FileStatus s : st) {
        if (reader != null) {
          reader.close();
        }
        reader = new SequenceFile.Reader(fs, s.getPath(), conf);
        while (reader.next(k, v)) {
          sumDelete++;
          int mod = this.getDocidMod(k);
          if (mod != lastMod) {
            Path file = new Path(dataDir + "/delete-"
                                 + String.valueOf(mod));
            logger.debug("create new delete file " + file);
            if (fileOut != null) {
              fileOut.close();
            }
            deleteFileCreate++;
            fileOut = fs.create(file, false);
            lastMod = mod;
          }
          fileOut.write(k.get());
        }
      }
      if (reader != null)
        reader.close();

      if (fileOut != null) {
        fileOut.close();
      }
      logger.info("sum send =" + sumSend + "\t fileCreate ="
                  + contentfileCreate + "\t sum delete =" + sumDelete
                  + "\t file delete=" + deleteFileCreate);
      if (contentfileCreate <= 1024 && deleteFileCreate <= 102)
        flag = true;

    } catch (IOException e) {
      e.printStackTrace();
    }

    return flag;

  }

  public static void main(String args[]) {
    try {
      Properties p = new Properties();
      p.load(SelectAndSendJob.class
             .getResourceAsStream("/log4j.properties"));
      PropertyConfigurator.configure(p);
    } catch (MalformedURLException e1) {
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Configuration conf = HBaseConfiguration.create();

    try {
      SelectAndSendJob selectAndSendJob = new SelectAndSendJob(conf);
      selectAndSendJob.runJob();
      // selectAndSendJob.splitSendContent("/spider_data/send_content_output/20121125083418");

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
