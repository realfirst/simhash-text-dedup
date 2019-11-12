package com.zhongsou.spider.bean;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.zhongsou.spider.exception.EmptyParamValueException;
import com.zhongsou.spider.hadoop.jobcontrol.SpiderJob;

public class JobInfo {
  Logger log = Logger.getLogger(JobInfo.class);
  private String name;
  private HashMap<String, String> paramMap = new HashMap<String, String>();
  private String command;
  private Jobtype type;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public HashMap<String, String> getParamMap() {
    return paramMap;
  }

  public void setParamMap(HashMap<String, String> paramMap) {
    this.paramMap = paramMap;
  }

  public String getCommand() {
    return command;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public Jobtype getType() {
    return type;
  }

  public void setType(Jobtype type) {
    this.type = type;
  }

  public String generateCommand() throws EmptyParamValueException {
    String s = this.command;
    for (Map.Entry<String, String> t : SpiderJob.propertiesMap.entrySet()) {
      s = s.replace(t.getKey(), t.getValue());
    }

    for (Map.Entry<String, String> entry : this.paramMap.entrySet()) {
      if (entry.getValue() == null || entry.getValue().equals("")) {
        log.equals("error value key=" + entry.getKey() + "\t command=" + command);
        throw new EmptyParamValueException();
      }
      s = s.replace(entry.getKey(), entry.getValue());
    }
    return s.trim();
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("name:" + name + "\ttype=" + type.toString() + "\tcommand=" + command + "\n");
    String s = buffer.toString();

    for (Map.Entry<String, String> entry : this.paramMap.entrySet()) {
      buffer.append("\t" + entry.getKey() + "\t" + entry.getValue() + "\n");
    }

    return buffer.toString();
  }

}
