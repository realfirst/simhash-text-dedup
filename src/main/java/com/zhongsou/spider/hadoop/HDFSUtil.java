package com.zhongsou.spider.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class HDFSUtil {
  private static Logger log = Logger.getLogger(HDFSUtil.class);

  public synchronized static FileSystem getFileSystem(String ip, int port) {
    FileSystem fs = null;
    String url = "hdfs://" + ip + ":" + String.valueOf(port);
    Configuration config = new Configuration();
    config.set("fs.default.name", url);
    try {
      fs = FileSystem.get(config);
    } catch (Exception e) {
      log.error("getFileSystem failed :" + ExceptionUtils.getFullStackTrace(e));
    }
    return fs;
  }

  public synchronized static FileSystem getFileSystem() {
    FileSystem fs = null;
    Configuration config = new Configuration();
    try {
      fs = FileSystem.get(config);
    } catch (Exception e) {
      log.error("getFileSystem failed :" + ExceptionUtils.getFullStackTrace(e));
    }
    return fs;
  }

  public synchronized static void listNode(FileSystem fs) {
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    try {
      DatanodeInfo[] infos = dfs.getDataNodeStats();
      for (DatanodeInfo node : infos) {
        System.out.println("HostName: " + node.getHostName() + "/n" + node.getDatanodeReport());
        System.out.println("--------------------------------");
      }
    } catch (Exception e) {
      log.error("list node list failed :" + ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 打印系统配置
   *
   * @param fs
   */
  public synchronized static void listConfig(FileSystem fs) {
    Iterator<Entry<String, String>> entrys = fs.getConf().iterator();
    while (entrys.hasNext()) {
      Entry<String, String> item = entrys.next();
      log.info(item.getKey() + ": " + item.getValue());
    }
  }

  /**
   * 创建目录和父目录
   *
   * @param fs
   * @param dirName
   */
  public synchronized static void mkdirs(FileSystem fs, String dirName) {

    Path src = new Path(dirName);
    // FsPermission p = FsPermission.getDefault();
    boolean succ;
    try {
      if (fs.exists(src))
        return;
      succ = fs.mkdirs(src);
      if (succ) {
        log.info("create directory " + dirName + " successed. ");
      } else {
        log.info("create directory " + dirName + " failed. ");
      }
    } catch (Exception e) {
      log.error("create directory " + dirName + " failed :" + ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 删除目录和子目录
   *
   * @param fs
   * @param dirName
   */
  public synchronized static void rmdirs(FileSystem fs, String dirName) {

    String dir = dirName;
    Path src = new Path(dir);
    boolean succ;
    try {
      succ = fs.delete(src, true);
      if (succ) {
        log.info("remove directory " + dir + " successed. ");
      } else {
        log.info("remove directory " + dir + " failed. ");
      }
    } catch (Exception e) {
      log.error("remove directory " + dir + " failed :" + ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 上传目录或文件
   *
   * @param fs
   * @param local
   * @param remote
   */
  public synchronized static boolean upload(FileSystem fs, String local, String remote) {

    Path dst = new Path(remote);
    Path src = new Path(local);
    try {
      fs.copyFromLocalFile(false, true, src, dst);
      log.info("upload " + local + " to  " + remote + " successed. ");
    } catch (Exception e) {
      log.error("upload " + local + " to  " + remote + " failed :" + ExceptionUtils.getFullStackTrace(e));
      return false;
    }
    return true;
  }

  /**
   * 下载目录或文件
   *
   * @param fs
   * @param local
   * @param remote
   */
  public synchronized static void download(FileSystem fs, String local, String remote) {
    Path dst = new Path(remote);
    Path src = new Path(local);
    try {
      fs.copyToLocalFile(false, dst, src);
      log.info("download from " + remote + " to  " + local + " successed. ");
    } catch (Exception e) {
      log.error("download from " + remote + " to  " + local + " failed :" + ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 字节数转换
   *
   * @param size
   * @return
   */
  public synchronized static String convertSize(long size) {
    String result = String.valueOf(size);
    if (size < 1024 * 1024) {
      result = String.valueOf(size / 1024) + " KB";
    } else if (size >= 1024 * 1024 && size < 1024 * 1024 * 1024) {
      result = String.valueOf(size / 1024 / 1024) + " MB";
    } else if (size >= 1024 * 1024 * 1024) {
      result = String.valueOf(size / 1024 / 1024 / 1024) + " GB";
    } else {
      result = result + " B";
    }
    return result;
  }

  public synchronized static boolean renameFile(FileSystem fs, String oldPath, String newPath) {
    try {
      if (oldPath == null || newPath == null || oldPath.equals("") || newPath.equals(""))
        return false;
      String folder = newPath;
      if (!newPath.endsWith("/")) {
        folder = newPath.substring(0, newPath.lastIndexOf("/"));
      }
      Path t = new Path(folder);
      if (!fs.exists(t)) {
        boolean flag = fs.mkdirs(t);
        if (flag == false) {
          log.info("create dst folder failed" + t.toString());
          return false;
        }
      }

      return fs.rename(new Path(oldPath), new Path(newPath));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  /**
   *
   *
   * 通配符删除文件
   *
   *
   * @param fs
   * @param srcPattern
   * @return
   */
  public synchronized static boolean rmFiles(FileSystem fs, String srcPattern) {
    if (srcPattern == null || srcPattern.equals(""))
      return false;

    try {
      FileStatus filestatus[] = fs.globStatus(new Path(srcPattern));
      if (filestatus != null && filestatus.length > 0) {

        for (FileStatus file : filestatus) {
          fs.delete(file.getPath(), false);
        }

      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public synchronized static boolean moveFiles(FileSystem fs, String srcPattern, String dest) {
    if (srcPattern == null || dest == null || srcPattern.equals("") || dest.equals(""))
      return false;

    try {
      FileStatus filestatus[] = fs.globStatus(new Path(srcPattern));
      if (filestatus != null && filestatus.length > 0) {
        Path destPath = new Path(dest);
        if (!fs.exists(destPath)) {
          fs.mkdirs(destPath);
        }
        for (FileStatus file : filestatus) {
          fs.rename(file.getPath(), destPath);
        }

      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * 遍历HDFS上的文件和目录
   *
   * @param fs
   * @param path
   */
  public synchronized static void listFile(FileSystem fs, String path, List<FileInfo> infoList, boolean isRecursive) {
    Path workDir = fs.getWorkingDirectory();
    Path dst;
    if (null == path || "".equals(path)) {
      dst = new Path(workDir + "/" + path);
    } else {
      dst = new Path(path);
    }
    try {
      String relativePath = "";
      FileStatus[] fList = fs.listStatus(dst);
      if (fList != null) {
        for (FileStatus f : fList) {
          if (null != f) {
            FileInfo info = new FileInfo();
            if (f.getPath().getName().startsWith("_"))
              continue;
            if (!f.getPath().getParent().getName().equals(""))
              relativePath = new StringBuffer().append(f.getPath().getParent()).append("/").append(f.getPath().getName()).toString();
            else
              relativePath = new StringBuffer().append(f.getPath().getParent()).append(f.getPath().getName()).toString();
            info.setDir(f.isDir());
            info.setPath(relativePath);
            info.setLastModifyTime(f.getModificationTime());
            info.setFileLen(f.getLen());
            infoList.add(info);
            if (f.isDir() && isRecursive) {
              listFile(fs, relativePath, infoList, isRecursive);
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("list files of " + path + " failed :" + ExceptionUtils.getFullStackTrace(e));
    } finally {
    }
  }

  /**
   *
   * 合并多个sequence file到一个sequence file之中
   *
   * @param <A>
   * @param <B>
   * @param fs
   * @param conf
   * @param inputPattern
   *            输入文件glob表达式
   * @param outputFile
   *            输出的文件的路径
   * @param Keyclass
   *            sequence file key class
   * @param Value
   *            sequence file value class
   * @return
   */
  public static <A extends Writable, B extends Writable> boolean mergeMultipleSeqFile2One(FileSystem fs, Configuration conf, String inputPattern, String outputFile, Class<A> Keyclass, Class<B> Value) {
    boolean flag = false;
    try {
      Path output = new Path(outputFile);
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, output, Keyclass, Value);
      FileStatus status[] = fs.globStatus(new Path(inputPattern));
      A k = Keyclass.newInstance();
      B v = null;
      if (!Value.getName().equals("NullWritable")) {
        System.out.println("null value");
        v = (B) NullWritable.get();
      } else {
        v = Value.newInstance();
      }

      for (FileStatus s : status) {
        if (s.getPath().equals(output))
          continue;
        SequenceFile.Reader reader = null;
        reader = new SequenceFile.Reader(fs, s.getPath(), conf);
        if (reader.getKeyClassName().equals(Keyclass.getName()) && reader.getValueClassName().equals(Value.getName())) {
          while (reader.next(k, v)) {
            writer.append(k, v);
          }
        } else {
          System.out.println(s.getPath() + "\t key class " + reader.getKeyClassName() + "\t value class " + reader.getValueClassName() + " doesn't equal " + Keyclass.getName()
                             + "\t value class=" + Value.getName());
        }
        reader.close();
      }
      writer.close();
      flag = true;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InstantiationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return flag;
  }

  public static void main(String args[]) {
    FileSystem fs = HDFSUtil.getFileSystem();
    // HDFSUtil.moveFiles(fs,
    // "/spider_data/parse_convert_output/20121119212622/part*",
    // "/spider_data/parsed_url_meta/20121119212622");
    // System.out.println(HDFSUtil.renameFile(fs,
    // "/user/kaifa/sortoutput/sort_201211140744/part-r-00003",
    // "/sortoutput/sort_201211140744/part-r-00003"));
    HDFSUtil.mergeMultipleSeqFile2One(fs, new Configuration(), "/spider_data/clawer_all_docid/newdocid20121123060831*", "/spider_data/clawer_all_docid/newdocid_20121123060831",
                                      ImmutableBytesWritable.class, NullWritable.class);
    HDFSUtil.rmFiles(fs, "/spider_data/clawer_all_docid/newdocid20121123060831*");
  }
}
