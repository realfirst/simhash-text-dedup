package com.zhongsou.spider.hadoop;

import java.text.SimpleDateFormat;
import java.util.Date;

public class FileInfo {
  private String path;
  boolean isDir;
  private String permission;
  private long lastModifyTime;
  private long fileLen;
  static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public String getPath() {
    return path;
  }

  public long getFileLen() {
    return fileLen;
  }

  public void setFileLen(long fileLen) {
    this.fileLen = fileLen;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public boolean isDir() {
    return isDir;
  }

  public void setDir(boolean isDir) {
    this.isDir = isDir;
  }

  public String getPermission() {
    return permission;
  }

  public void setPermission(String permission) {
    this.permission = permission;
  }

  public long getLastModifyTime() {
    return lastModifyTime;
  }

  public void setLastModifyTime(long lastModifyTime) {
    this.lastModifyTime = lastModifyTime;
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append(this.path + "\t" + format.format(new Date(this.getLastModifyTime())) + "\t" + this.fileLen + "\t" + this.isDir);
    return buffer.toString();

  }
}
