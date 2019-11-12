package com.zhongsou.spider.plugin;

import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class Plugin {
  private String className;
  private TreeMap<Integer, PluginImplementation> tmap = new TreeMap<Integer, PluginImplementation>();

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public TreeMap<Integer, PluginImplementation> getImplementionmap() {

    return tmap;
  }

  public static class PluginImplementation {
    String className;
    int order;
    boolean enabled;
    HashMap<String, String> attributes;

    public String getClassName() {
      return className;
    }

    public void setClassName(String className) {
      this.className = className;
    }

    public int getOrder() {
      return order;
    }

    public void setOrder(int order) {
      this.order = order;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public void put(String key, String value) {
      if (this.attributes == null) {
        this.attributes = new HashMap<String, String>();

      }
      this.attributes.put(key, value);
    }

    public String getAttribute(String key) {
      if (this.attributes == null || !this.attributes.containsKey(key)) {
        return null;
      } else {
        return this.attributes.get(key);
      }
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append("className=" + className + "\torder" + order + "\tenable=" + enabled);
      return buffer.toString();
    }

    public Object newInstance(Configuration conf) {
      try {
        Object t = Class.forName(this.className).newInstance();
        if (Configured.class.isAssignableFrom(t.getClass())) {
          ((Configured) t).setConf(conf);
        } else if (Configurable.class.isAssignableFrom(t.getClass())) {
          ((Configurable) t).setConf(conf);
        }
        return t;
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
      return null;
    }

  }

}
