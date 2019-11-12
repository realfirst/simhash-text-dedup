package com.zhongsou.spider.hadoop.jobcontrol;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.zhongsou.spider.bean.JobInfo;
import com.zhongsou.spider.bean.ParsedURLInfo;
import com.zhongsou.spider.exception.EmptyParamValueException;
import com.zhongsou.spider.hadoop.FileInfo;
import com.zhongsou.spider.hadoop.HDFSUtil;

/**
 *
 * 分析的job，用于下载与分析网页,包括以下几个功能：
 *
 * <pre>
 * 1  扫描下载完成的目录,分析网页
 * 2  将分析完成的内容进行转化成hfile，url元数据（爬虫使用），加载使用docid与指纹，新分出的url信息
 * 3  url排重
 * 4  统计url的信息，主要两个任务
 *    4.1 统计某个网页新发现的url的个数，计算下次抓取时间，等等，生成hfile
 *    4.2 生成新发现的url的hfile，供入库
 * </pre>
 *
 * @author Xi Qi
 *
 */
public class ParserJob extends SpiderJob {
  static Logger logger = Logger.getLogger(ParserJob.class);
  PriorityBlockingQueue<String> finishedDownloadQueue =
      new PriorityBlockingQueue<String>();
  PriorityBlockingQueue<ParsedURLInfo> finishedParsedQueue =
      new PriorityBlockingQueue<ParsedURLInfo>();
  boolean running = true;
  ParseThread parseThread = new ParseThread();
  DuplicateThread duplicateThread = new DuplicateThread();
  LinkedList<FileInfo> tlist = new LinkedList<FileInfo>();
  final static String CLAWER_OUTPUT = ROOT_DIR + "/" + CLAWER_OUTPUT_DIR;
  HashMap<String, JobInfo> jobInfoMap;
  int scanInterval;
  int sleepInterval;
  int maxDocidFileNums;
  HTable webDBTable;
  boolean deleteHFile;

	public ParserJob(Configuration conf) throws IOException {
		super(conf);
		// TODO Auto-generated constructor stub
		scanClawerFolder();
		this.jobInfoMap = jobMap.get(this.getClass().getName());
		this.scanInterval = conf.getInt("scan_hbase_interval", 1000 * 10);
		this.sleepInterval = conf.getInt("sleep_interval", 1000 * 10);
		this.maxDocidFileNums = conf.getInt("max_docid_file_num", 100);
		webDBTable = new HTable(conf, "webDB");
		this.deleteHFile = conf.getBoolean("delete_hfile", true);
		
	}

  @Override
  public void runJob() {
    Thread parse = new Thread(this.parseThread);
    Thread duplicate = new Thread(this.duplicateThread);

    parse.start();
    duplicate.start();

    while (this.running) {
      // this.scanClawerFolder();
      try {
        Thread.sleep(1000 * 10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public int scanClawerFolder() {
    this.tlist.clear();
    HDFSUtil.listFile(fs, CLAWER_OUTPUT, tlist, false);
    for (FileInfo fileinfo : tlist) {
      String path = fileinfo.getPath();
      String name = path.substring(path.lastIndexOf("/") + 1);
      if (name.matches("\\d{14}") && !this.finishedDownloadQueue.contains(path)) {
        this.finishedDownloadQueue.add(path);
        logger.info("add a download finished folder to parseing queue,path=" + path +
                    "\tqueue size=" + this.finishedDownloadQueue.size());
      }
    }
    return this.finishedDownloadQueue.size();
  }

  @Override
  void checkFolderExists() {
    // TODO Auto-generated method stub
    HDFSUtil.mkdirs(fs, ROOT_DIR + "/" + CLAWER_DOCID_DIR);
  }

  class ParseThread implements Runnable {
    @Override
    public void run() {
      // TODO Auto-generated method stub
      while (running) {
        synchronized (this) {
          while (finishedDownloadQueue.size() == 0) {
            try {
              scanClawerFolder();
              if (finishedDownloadQueue.size() > 0)
                break;
              else
                this.wait(scanInterval);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        }
        String folder = finishedDownloadQueue.poll();
        if (folder == null || folder.equals("")) {
          logger.error("retrive a wrong path from folder");
          continue;
        }
        boolean t = this.parseClawerFolder(folder);
        if (t) {
          logger.info("parse job " + folder + " succeed");
        } else {
          logger.info("parse job " + folder + " failed");
        }
      }
    }

    public boolean parseClawerFolder(String folder) {
      boolean flag = false;
      JobInfo parseJobinfo = jobInfoMap.get("parse_url");
      String time = folder.substring(folder.lastIndexOf("/") + 1);
      String bindir = ROOT_DIR + "/" + PIPES_BIN_DIR;
      String parseOutput = ROOT_DIR + "/" + PARSE_OUTPUT_DIR + "/" + time;
      String parse_home = propertiesMap.get("$PARSE_HOME");
      // 每次下载之前删除以前的二进制文件，上传新的二进制文件
      HDFSUtil.rmdirs(fs, bindir + "/parse_content");
      HDFSUtil.upload(fs, parse_home + "/spider_c++_common/bin/parse_content", bindir);
      parseJobinfo.getParamMap().put("$parse_content_bin", bindir + "/parse_content");
      parseJobinfo.getParamMap().put("$output", parseOutput);
      parseJobinfo.getParamMap().put("$job_name", "parse_page_" + time);
      parseJobinfo.getParamMap().put("$input", folder);

      try {
        if (fs.exists(new Path(parseOutput))) {
          HDFSUtil.rmdirs(fs, parseOutput);
        }
        if (!fs.exists(new Path(folder))) {
          return false;
        }

        String command = parseJobinfo.generateCommand();
        logger.info("submit parse job comand =" + command);
        RunningJob job = (RunningJob) (JobCreateFactory.createJobObject(parseJobinfo));
        job.waitForCompletion();
        logger.info("parse  job execute finished " + command + " " + job.isSuccessful());

        int size = 0;

        if (!job.isSuccessful()) {
          List<FileInfo> tlist = new LinkedList<FileInfo>();
          HDFSUtil.listFile(fs, parseOutput, tlist, false);
          size = tlist.size();
          logger.warn("parse job failed " + command + "\t get parse succeed files =" + size);
        }
        // 开始转化的job
        if (size > 0 || job.isSuccessful()) {
          JobInfo convertJob = jobInfoMap.get("parse_convert");
          String parsed_convert_output = ROOT_DIR + "/" + PARSE_CONVERT_DIR + "/" + time;
          convertJob.getParamMap().put("$parsed_output", parseOutput);
          convertJob.getParamMap().put("$parsed_convert_output", parsed_convert_output);
          convertJob.getParamMap().put("$job_name", "parse_result_converter_" + time);
          convertJob.getParamMap().put("$seqTimestamp", time);

          if (fs.exists(new Path(parsed_convert_output))) {
            logger.warn("folder " + parsed_convert_output + " alread existed delete");
            HDFSUtil.rmdirs(fs, parsed_convert_output);
          }

          String convertCommand = convertJob.generateCommand();
          logger.info("submit convert parse job command=" + convertCommand);
          Job parseConvertJob = (Job) (JobCreateFactory.createJobObject(convertJob));
          parseConvertJob.waitForCompletion(true);
          logger.info("convert parse job finished status=" + parseConvertJob.isComplete());
          if (parseConvertJob.isSuccessful()) {
            String urlMetaFolder = ROOT_DIR + "/" + PARSED_URL_META_DIR + "/" + time;
            String urlAndFinger = ROOT_DIR + "/" + URL_AND_FINGER_DIR + "/" + time;
            String urlLinkInfo = ROOT_DIR + "/" + PARSED_URL_LINK_INFO_DIR + "/" + time;
            String bakparseOutput = parseOutput.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);

            // 将转化的结果移到相应的文件夹中
            logger.info("move meta file " + HDFSUtil.moveFiles(fs, parsed_convert_output + "/URLMeta*", urlMetaFolder));
            logger.info("move url info file " + HDFSUtil.moveFiles(fs, parsed_convert_output + "/parsedURL*", urlLinkInfo));
            logger.info("move new url " + HDFSUtil.moveFiles(fs, parsed_convert_output + "/part*", urlAndFinger + "_bak"));
            // 一次型改名，防止下面加载的job，加载时，改名没有全部完成，丢失数据
            HDFSUtil.renameFile(fs, urlAndFinger + "_bak", urlAndFinger);
            // 将网页解析完的数据移除到备份文件夹之下
            HDFSUtil.renameFile(fs, parseOutput, bakparseOutput);

            ParsedURLInfo info = new ParsedURLInfo();
            info.setMetaFolder(urlMetaFolder);
            info.setUrlInfoFolder(urlLinkInfo);
            info.setSeqTime(time);
            finishedParsedQueue.add(info);
            logger.info("convert job succeed ,add url info " + info + " to duplicate queue");
            flag = true;
          }

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

      finally {
        String bakClawerFolder = folder.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, folder, bakClawerFolder);
      }

      return flag;
    }
  }

  class DuplicateThread implements Runnable {
    private List<FileInfo> fileList = new LinkedList<FileInfo>();

    public DuplicateThread() {
      initQueue();
    }

    /**
     *
     * load上一次job正常退出的剩下的文件
     *
     * @return
     */
    public int initQueue() {
      int i = 0;
      String meta = ROOT_DIR + "/" + PARSED_URL_META_DIR;
      String urlLinkInfo = ROOT_DIR + "/" + PARSED_URL_LINK_INFO_DIR;
      HDFSUtil.listFile(fs, meta, fileList, false);

      for (FileInfo fileinfo : fileList) {
        String path = fileinfo.getPath();
        String time = path.substring(path.lastIndexOf("/") + 1);
        if (time.matches("\\d{14}")) {
          try {
            String urlinfo = urlLinkInfo + "/" + time;
            if (fs.exists(new Path(urlinfo))) {
              ParsedURLInfo info = new ParsedURLInfo();
              info.setMetaFolder(path);
              info.setUrlInfoFolder(urlinfo);
              info.setSeqTime(time);
              logger.info("load last left info into finished queue" + info);
              finishedParsedQueue.add(info);
              i++;
            }
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

        }

      }
      logger.info("start load left parsed url info into queue" + i);
      return i;

    }

    @Override
    public void run() {
      // TODO Auto-generated method stub
      String docid_dir = ROOT_DIR + "/" + CLAWER_DOCID_DIR;
      while (running) {
        synchronized (this) {
          while (finishedParsedQueue.size() == 0) {
            try {
              this.wait(sleepInterval);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        }
        this.fileList.clear();
        try {
          HDFSUtil.listFile(fs, docid_dir, fileList, false);
          if (this.fileList.size() == 0 || this.fileList.size() > maxDocidFileNums) {
            JobInfo info = jobInfoMap.get("export_docid");
            assert (info != null);
            this.exportDocidFromHbase(info);
          }

          ParsedURLInfo info = finishedParsedQueue.poll();
          if (info == null) {
            logger.error("error get a parsed url info");
            continue;
          }
          JobInfo duplicateInfo = jobInfoMap.get("duplicate_url");
          assert (duplicateInfo != null);
          boolean res = this.duplicateURL(duplicateInfo, info);
          if (res) {
            JobInfo statisticOldinfo = jobInfoMap.get("statistic_old_url");
            boolean flag = this.importOldURL(statisticOldinfo, info);
            logger.info("statistic old url info result:" + flag);
            JobInfo statisticNewInfo = jobInfoMap.get("statistic_new_url");
            flag = this.importNewURL(statisticNewInfo, info);
            logger.info("statistic new url info result:" + flag);
          } else {
            logger.info("duplicate url info job failed");
          }
        } catch (Exception e) {
          e.printStackTrace();
          logger.error("get a exception");
        }

      }

    }

    /**
     * 导出docid到hdfs之上
     *
     *
     * @param info
     * @return
     */
    public boolean exportDocidFromHbase(JobInfo info) {
      boolean flag = false;
      String exportDocid = ROOT_DIR + "/" + CLAWER_DOCID_DIR + "_bak";
      info.getParamMap().put("$clawer_docid_dir", exportDocid);
      HDFSUtil.rmdirs(fs, exportDocid);
      try {
        String command = info.generateCommand();
        Job job = (Job) JobCreateFactory.createJobObject(info);
        logger.info("start export docid job " + command);
        job.waitForCompletion(true);
        logger.info("end export docid job status=" + job.isSuccessful());
        if (job.isSuccessful()) {
          HDFSUtil.rmdirs(fs, ROOT_DIR + "/" + CLAWER_DOCID_DIR);
          boolean t = HDFSUtil.renameFile(fs, exportDocid, ROOT_DIR + "/" + CLAWER_DOCID_DIR);
          if (t) {
            logger.info("recreate docid directory succeed!");
            flag = true;
          }

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

    /**
     *
     * 排重的job
     *
     * @param info
     * @param urlInfo
     * @return
     */
    public boolean duplicateURL(JobInfo info, ParsedURLInfo urlInfo) {
      boolean flag = false;
      String duplicate_output = ROOT_DIR + "/" + DUPLICATE_DOCID_DIR + "/" + urlInfo.getSeqTime();
      String docid_dir = ROOT_DIR + "/" + CLAWER_DOCID_DIR;
      String new_url_src_docid_dir = ROOT_DIR + "/" + NEW_URL_SRC_DOCID_DIR + "/" + urlInfo.getSeqTime();

      info.getParamMap().put("$clawer_docid", docid_dir);
      info.getParamMap().put("$parsed_url_info", urlInfo.getUrlInfoFolder());
      info.getParamMap().put("$job_name", "duplicate_url_" + urlInfo.getSeqTime());
      info.getParamMap().put("$duplicate_url_output", duplicate_output);
      info.getParamMap().put("$num_reduce", String.valueOf(10));

      try {
        if (fs.exists(new Path(duplicate_output))) {
          fs.delete(new Path(duplicate_output), true);
        }
        String command = info.generateCommand();
        logger.info("start duplicate url command " + command);
        Job duplicateJob = (Job) (JobCreateFactory.createJobObject(info));
        duplicateJob.waitForCompletion(true);
        logger.info("end duplicate url status=" + duplicateJob.isSuccessful());
        if (duplicateJob.isSuccessful()) {
          // 将生成的新的url docid移到全库之中
          String output = ROOT_DIR + "/" + DUPLICATE_DOCID_DIR + "/" + "new_docid_" + urlInfo.getSeqTime();

          boolean res = HDFSUtil.mergeMultipleSeqFile2One(fs, conf, duplicate_output + "/newdocid*", output, ImmutableBytesWritable.class, NullWritable.class);
          if (res == true) {
            boolean t1 = HDFSUtil.renameFile(fs, output, docid_dir);
            boolean t2 = HDFSUtil.rmFiles(fs, duplicate_output + "/newdocid*");
            logger.info("merge new docid to one file" + output + " succeed rename =" + t1 + "\tremove old new docid files " + t2);
          } else {
            logger.info("merge new docid to one file" + output + " failed");
            HDFSUtil.moveFiles(fs, duplicate_output + "/newdocid*", docid_dir);
          }
          flag = true;
          HDFSUtil.moveFiles(fs, duplicate_output + "/srcurldocid*", new_url_src_docid_dir);
          String bakURLInfo = urlInfo.getUrlInfoFolder().replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
          HDFSUtil.renameFile(fs, urlInfo.getUrlInfoFolder(), bakURLInfo);
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

    public boolean importOldURL(JobInfo info, ParsedURLInfo urlInfo) {
      boolean flag = false;
      String urlMetaFolder = urlInfo.getMetaFolder();
      String newURLSrcDocidFolder = ROOT_DIR + "/" + NEW_URL_SRC_DOCID_DIR + "/" + urlInfo.getSeqTime();
      String staticOldOutput = ROOT_DIR + "/" + STATISTIC_OLD_URL_OUTPUT_DIR + "/" + urlInfo.getSeqTime();
      try {
        boolean exist = fs.exists(new Path(urlMetaFolder));
        boolean exist2 = fs.exists(new Path(newURLSrcDocidFolder));
        if (exist && exist2) {
          if (fs.exists(new Path(staticOldOutput))) {
            HDFSUtil.rmdirs(fs, staticOldOutput);
          }
          info.getParamMap().put("$parsed_urlmeta", urlMetaFolder);
          info.getParamMap().put("$srcdocid", newURLSrcDocidFolder);
          info.getParamMap().put("$job_name", "statistic_old_url_" + urlInfo.getSeqTime());
          info.getParamMap().put("$oldurlimport_output", staticOldOutput);
          logger.info("create statistic old url job command " + info.generateCommand());
          Job statisticOldJob = (Job) (JobCreateFactory.createJobObject(info));
          statisticOldJob.waitForCompletion(true);
          if (statisticOldJob.isSuccessful()) {
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            logger.info("start load hfile to webDb table path=" + staticOldOutput);
            loader.doBulkLoad(new Path(staticOldOutput), webDBTable);
            logger.info("load " + staticOldOutput + " to webDB table succeed!");
            flag = true;
          } else {
            logger.info("statistic old url job failed");
          }
        } else {
          logger.info("folder is missing " + urlMetaFolder + " existed:" + exist + "\t " + newURLSrcDocidFolder + " existed:" + exist2);
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        logger.error("import old url info error " + e.getMessage());
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (EmptyParamValueException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        logger.error("statistic old  url info job failed!" + e.getMessage());
      } finally {
        String metaBak = urlMetaFolder.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, urlMetaFolder, metaBak);
        String newURLBak = newURLSrcDocidFolder.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, newURLSrcDocidFolder, newURLBak);
        if (deleteHFile) {
          logger.info("delete import hfile " + staticOldOutput);
          HDFSUtil.rmdirs(fs, staticOldOutput);
        } else {
          String hfileBak = staticOldOutput.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
          HDFSUtil.renameFile(fs, staticOldOutput, hfileBak);
          logger.info("rename fhile from folder " + staticOldOutput + " to " + hfileBak);
        }
      }

      return flag;
    }

    public boolean importNewURL(JobInfo info, ParsedURLInfo urlInfo) {
      boolean flag = false;
      String duplicate_output = ROOT_DIR + "/" + DUPLICATE_DOCID_DIR + "/" + urlInfo.getSeqTime();
      String staticNewOutput = ROOT_DIR + "/" + STATISTIC_NEW_URL_OUTPUT_DIR + "/" + urlInfo.getSeqTime();
      try {
        boolean exist = fs.exists(new Path(duplicate_output));
        if (exist) {
          info.getParamMap().put("$new_url_info", duplicate_output);
          info.getParamMap().put("$job_name", "statistic_new_url_" + urlInfo.getSeqTime());
          info.getParamMap().put("$newurlimport_output", staticNewOutput);
          logger.info("create statistic new url job command " + info.generateCommand());
          if (fs.exists(new Path(staticNewOutput))) {
            HDFSUtil.rmdirs(fs, staticNewOutput);
          }
          Job statisticOldJob = (Job) (JobCreateFactory.createJobObject(info));
          statisticOldJob.waitForCompletion(true);
          if (statisticOldJob.isSuccessful()) {
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            logger.info("start load hfile to webDb table path=" + staticNewOutput);
            loader.doBulkLoad(new Path(staticNewOutput), webDBTable);
            logger.info("load " + staticNewOutput + " to webDB table succeed!");
            flag = true;
          } else {
            logger.info("statistic old url job failed");
          }
        } else {
          logger.info("folder is missing " + duplicate_output + " existed:" + exist);
          ;
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        logger.error("import old url info error " + e.getMessage());
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (EmptyParamValueException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        logger.error("statistic new url info job failed!" + e.getMessage());
      } finally {
        String duplicateBak = duplicate_output.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
        HDFSUtil.renameFile(fs, duplicate_output, duplicateBak);
        if (deleteHFile) {
          HDFSUtil.rmdirs(fs, staticNewOutput);
          logger.info("delete new url hfile " + staticNewOutput);
        } else {
          String newHfile = staticNewOutput.replaceFirst(ROOT_DIR, ROOT_DIR_BAK);
          HDFSUtil.renameFile(fs, staticNewOutput, newHfile);
          logger.info("rename new url import hfile from " + staticNewOutput + " to " + newHfile);
        }
      }

      return flag;
    }
  }

  public static void main(String args[]) {
    try {
      Properties p = new Properties();
      p.load(SelectAndSendJob.class.getResourceAsStream("/log4j.properties"));
      PropertyConfigurator.configure(p);
    } catch (MalformedURLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Configuration conf = HBaseConfiguration.create();
    try {
      ParserJob parseJob = new ParserJob(conf);
      parseJob.runJob();

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
