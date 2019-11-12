package com.zhongsou.spider.hadoop.jobcontrol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.zhongsou.spider.bean.JobInfo;
import com.zhongsou.spider.bean.Jobtype;
import com.zhongsou.spider.exception.EmptyParamValueException;
import com.zhongsou.spider.hadoop.FileInfo;
import com.zhongsou.spider.hadoop.HDFSUtil;

/**
 *
 * 下载job的基类，利用所有job的控制
 *
 * @author Xi Qi
 *
 */
abstract public class SpiderJob {
  static Logger LOG = Logger.getLogger(SpiderJob.class);

  public static final String ROOT_DIR_BAK = "/spider_data_old";

  public static final String ROOT_DIR = "/spider_data";

  /**
   *
   * pipes 的二进制目录
   */
  public static final String PIPES_BIN_DIR = "pipes_bin";

  /**
   * URL 打分的输出根目录
   *
   */
  public static final String SCORE_OUTPUT_DIR = "score_output";

  /**
   *
   * url 选择的输出目录
   */
  public static final String SELECT_URL_OUTPUT_DIR = "select_url_output";


  /**
   *
   * 选择的url的输出目录
   */

  public static final String SORT_URL_OUTPUT_DIR="sort_url_output";


  /**
   * 下载完成的存储的数据根目录
   */
  public static final String CLAWER_OUTPUT_DIR = "clawer_output";

  /**
   * 网页分析完成之后的结果
   *
   */
  public static final String PARSE_OUTPUT_DIR = "parse_output";

  /**
   *
   * 网页分析转化完成的结果
   */

  public static final String PARSE_CONVERT_DIR = "parse_convert_output";
  /**
   * 新下载的网页指纹与docid的根目录
   */
  public static final String URL_AND_FINGER_DIR = "url_and_finger_output";

  /**
   * 解析完成的url的元数据信息
   */
  public static final String PARSED_URL_META_DIR = "parsed_url_meta";

  /**
   *
   * 解析出的url的链接的信息
   */

  public static final String PARSED_URL_LINK_INFO_DIR = "parsed_url_info";

  /**
   * 爬虫的所有docid目录
   *
   */
  public static final String CLAWER_DOCID_DIR = "clawer_all_docid";

  /**
   * 发现新网页url的源网页的docid的目录
   *
   */
  public static final String NEW_URL_SRC_DOCID_DIR = "new_url_src_docid";

  /**
   *
   * url排重的产生新的docid的目录
   */
  public static final String DUPLICATE_DOCID_DIR = "clawer_duplicte_docid";

  /**
   *
   * 统计老的url的信息的输出
   *
   */

  public static final String STATISTIC_OLD_URL_OUTPUT_DIR = "statistic_old_output";

  /**
   *
   * 统计新url的信息的输出
   */

  public static final String STATISTIC_NEW_URL_OUTPUT_DIR = "statistic_new_newoutput";

  /**
   * 已下载的url的更新数据的存储根目录
   */
  public static final String OLD_URL_HFILE_UPDATE_DIR = "old_url_update_output";

  /**
   *
   * 生产排重的对的输出根目录
   */
  public static final String GENERATE_DUP_PAIR_DIR = "generate_dup_pair_output";


  /**
   * 指纹未修改的加载的docid
   *
   */
  public static final String LOAD_DOCD_FINGER_NOT_CHANGE="load_docid_finger_not_change_output";
  /**
   * 新发现url的存储的根目录
   */
  public static final String NEW_URL_HFILE_INSERT_DIR = "new_url_insert_output";
  /**
   * 选择逻辑生成的docid的根目录
   *
   */
  public static final String SELECT_DOCID_OUTPUT_DIR = "select_docid_output";
  /**
   * 杀掉相同指纹之后的新下载的指纹与dcid的目录
   *
   */
  public static final String NEW_URL_AND_FINGER_DIR = "new_url_and_finger";

  /**
   * 相同指纹与排重杀掉存储的目录
   */
  public static final String SAME_FINGER_DOCID_DELETE_DIR = "same_finger_docid_delete_output";

  /**
   * 加载的docid的文件目录
   *
   */
  public static final String LOAD_DOCID_DIR = "load_docid_output";

  /**
   *
   * 生成加载标记与状态更新hfile的目录
   */
  public static final String LOAD_FLAG_HFILE_OUTPUT_DIR = "load_flag_hfile_output";

  /**
   *
   * 选择逻辑生成的加载列表hbase的目录
   *
   */
  public static final String SELECT_LOAD_DOCID_DIR = "select_load_docid_hbase_keys";

  /**
   * 选择逻辑存储删除列表存储的目录
   */

  public static final String SELECT_DOCID_DELETE_DIR = "select_docid_delete_output";
  /**
   * 发送数据的存储的根目录
   */
  public static String SEND_CONTENT_DIR = "send_content_output";

  public static DateFormat DATE_FORMAT = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
  Configuration conf;
  protected FileSystem fs;
  private Comparator pathComparator = new PathComparator();
  protected String LOCAL_PWD_HOME;
  protected boolean running = true;

  public final static String JOB_INFO = "/jobinfo.xml";
  public static final HashMap<String, HashMap<String, JobInfo>> jobMap = new HashMap<String, HashMap<String, JobInfo>>();
  public static final HashMap<String, String> propertiesMap = new HashMap<String, String>();

  static {
    loadJobInfo();
  }

  abstract void checkFolderExists();

  static class PathComparator implements Comparator<FileInfo> {
    @Override
    public int compare(FileInfo o1, FileInfo o2) {
      return o1.getPath().compareTo(o2.getPath());
    }
  }

  protected int getFileList(String path, List<FileInfo> list) {
    HDFSUtil.listFile(fs, path, list, false);
    Collections.sort(list, pathComparator);
    return list.size();
  }

  protected boolean uploadFiles(String localPath, String remotePath) {
    return HDFSUtil.upload(fs, localPath, remotePath);
  }

  public void init() {
    this.checkFolderExists();
  }

  public static int parseDateString(String str) {
    Date a;
    try {
      a = DATE_FORMAT.parse(str);
      long m = a.getTime();
      int second = (int) (m / 1000);
      return second;
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return 0;
  }

  public static String execJarJob(JobInfo info) {
    String returnString = "";
    Process pro = null;
    Runtime runTime = Runtime.getRuntime();
    if (runTime == null) {
      System.err.println("Create runtime false!");
    }
    String command = null;
    try {
      command = info.generateCommand();
      pro = runTime.exec(command);
      BufferedReader input = new BufferedReader(new InputStreamReader(pro.getInputStream()));
      PrintWriter output = new PrintWriter(new OutputStreamWriter(pro.getOutputStream()));
      String line;
      while ((line = input.readLine()) != null) {
        // System.out.println(line);
        returnString = returnString + line + "\n";
      }
      input.close();
      output.close();
      pro.destroy();
    } catch (IOException ex) {
      LOG.error(command + "\t" + ex.getMessage());
    } catch (EmptyParamValueException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      LOG.error("empty command message " + command);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error(command + "\n" + e.getMessage());
    }
    return returnString;

  }

  public SpiderJob(Configuration conf) throws IOException {
    this.conf = conf;
    fs = FileSystem.get(conf);
    SchemaMetrics.configureGlobally(conf);
    this.LOCAL_PWD_HOME = conf.get("PWD_HOME", "/home/kaifa/xiqi");
    init();
  }

  abstract public void runJob();

  public void cleanup() {

  }

  private static void loadJobInfo() {
    try {
      InputStream input = SpiderJob.class.getResourceAsStream(JOB_INFO);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document doc = db.parse(input);
      doc.getDocumentElement().normalize();

      NodeList propertyLst = doc.getElementsByTagName("properties");
      String name, value;

      Node pnode = propertyLst.item(0);

      NodeList property = ((Element) pnode).getElementsByTagName("property");

      for (int i = 0; i < property.getLength(); i++) {
        Node fNode = property.item(i);
        if (fNode.getNodeType() == Node.ELEMENT_NODE) {
          try {
            Element fstElmnt = (Element) fNode;

            NodeList nodeList = fstElmnt.getElementsByTagName("name");
            Element ele = (Element) nodeList.item(0);
            name = ((Node) ele.getChildNodes().item(0)).getNodeValue();
            nodeList = fstElmnt.getElementsByTagName("value");
            ele = (Element) nodeList.item(0);
            value = ((Node) ele.getChildNodes().item(0)).getNodeValue();
            propertiesMap.put(name, value);
            System.out.println("name=" + name + "\tvalue=" + value);
          } catch (Exception e) {
            e.printStackTrace();
            continue;
          }
        }
      }

      NodeList nodelist = doc.getElementsByTagName("Jobinfo");

      Node jobinfo = nodelist.item(0);
      NodeList plist = ((Element) jobinfo).getElementsByTagName("Jobs");

      String id, type, shell;
      for (int i = 0; i < plist.getLength(); i++) {
        Node jobnode = plist.item(i);
        Node idNode = jobnode.getAttributes().getNamedItem("id");
        id = idNode.getNodeValue();
        if (jobnode.getNodeType() == Node.ELEMENT_NODE) {
          HashMap<String, JobInfo> jobinfoMap = new HashMap<String, JobInfo>();
          jobMap.put(id, jobinfoMap);
          try {
            NodeList jobnodelist = ((Element) jobnode).getElementsByTagName("job");
            for (int k = 0; k < jobnodelist.getLength(); k++) {
              Node jobinfoNode = jobnodelist.item(k);
              JobInfo jobInfo = new JobInfo();
              Element fstElmnt = (Element) jobinfoNode;
              NodeList nodeList = fstElmnt.getElementsByTagName("name");
              Element ele = (Element) nodeList.item(0);
              name = ((Node) ele.getChildNodes().item(0)).getNodeValue();
              nodeList = fstElmnt.getElementsByTagName("type");
              ele = (Element) nodeList.item(0);
              type = ((Node) ele.getChildNodes().item(0)).getNodeValue();

              nodeList = fstElmnt.getElementsByTagName("shell");
              ele = (Element) nodeList.item(0);
              shell = ((Node) ele.getChildNodes().item(0)).getNodeValue();

              jobInfo.setName(name);
              jobInfo.setCommand(shell);
              jobInfo.setType(Jobtype.valueOf(type));

              propertyLst = fstElmnt.getElementsByTagName("params");
              String paramName, paramValue;

              pnode = propertyLst.item(0);

              NodeList param = ((Element) pnode).getElementsByTagName("param");
              if (param.getLength() > 0) {
                HashMap<String, String> parammap = new HashMap<String, String>();
                for (int j = 0; j < param.getLength(); j++) {
                  Node fNode = param.item(j);
                  if (fNode.getNodeType() == Node.ELEMENT_NODE) {
                    try {
                      Element element = (Element) fNode;

                      NodeList nameList = element.getElementsByTagName("name");
                      Element nameEle = (Element) nameList.item(0);
                      paramName = ((Node) nameEle.getChildNodes().item(0)).getNodeValue();
                      nodeList = element.getElementsByTagName("value");
                      ele = (Element) nodeList.item(0);
                      if (ele.getChildNodes().getLength() > 0) {
                        paramValue = ((Node) ele.getChildNodes().item(0)).getNodeValue();
                        parammap.put(paramName, paramValue);
                      } else {
                        parammap.put(paramName, "");
                      }
                      // System.out.println("param=" +
                      // paramName + "\tparamvalue=" +
                      // paramValue);
                    } catch (Exception e) {
                      e.printStackTrace();
                      continue;
                    }
                  }
                }
                jobInfo.setParamMap(parammap);
              }

              // System.out.println(jobInfo.generateCommand());
              jobinfoMap.put(jobInfo.getName(), jobInfo);
            }
          } catch (Exception e) {
            e.printStackTrace();
            continue;
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
