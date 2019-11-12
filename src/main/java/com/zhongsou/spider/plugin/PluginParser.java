package com.zhongsou.spider.plugin;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class PluginParser {
  String configName;
  static PluginParser _instance = null;
  Map<String, Plugin> pluginMap = new HashMap<String, Plugin>();
  public static final Logger LOG = LoggerFactory.getLogger(PluginParser.class);

  private PluginParser() {
    this.configName = "/plugin-config.xml";
    this.parseConfigFile();
  }

  void parseConfigFile() {
    try {

      InputStream input = PluginParser.class.getResourceAsStream(this.configName);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document doc = db.parse(input);
      doc.getDocumentElement().normalize();
      NodeList nodeLst = doc.getElementsByTagName("plugin");
      String className, order, enable;
      for (int i = 0; i < nodeLst.getLength(); i++) {
        Node pluginNode = nodeLst.item(i);
        Node idNode = pluginNode.getAttributes().getNamedItem("id");
        if (idNode == null)
          continue;

        String id = idNode.getNodeValue();
        Plugin c = new Plugin();
        c.setClassName(id);
        NodeList cnodeLst = ((Element) pluginNode).getElementsByTagName("implementation");
        for (int s = 0; s < cnodeLst.getLength(); s++) {
          Node fstNode = cnodeLst.item(s);

          if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
            try {
              Element fstElmnt = (Element) fstNode;

              NodeList nodeList = fstElmnt.getElementsByTagName("class");
              Element ele = (Element) nodeList.item(0);
              className = ((Node) ele.getChildNodes().item(0)).getNodeValue();
              nodeList = fstElmnt.getElementsByTagName("order");
              ele = (Element) nodeList.item(0);
              order = ((Node) ele.getChildNodes().item(0)).getNodeValue();
              nodeList = fstElmnt.getElementsByTagName("enable");
              ele = (Element) nodeList.item(0);
              enable = ((Node) ele.getChildNodes().item(0)).getNodeValue();

              Plugin.PluginImplementation t = new Plugin.PluginImplementation();
              t.setClassName(className);
              t.setEnabled(Boolean.parseBoolean(enable));
              t.setOrder(Integer.parseInt(order));
              if (Class.forName(id).isAssignableFrom(Class.forName(className)) && Boolean.parseBoolean(enable)) {
                c.getImplementionmap().put(Integer.parseInt(order), t);
              } else {
                LOG.info("implemenation " + t + " ingnored");
              }
              NodeList paranodeList = fstElmnt.getElementsByTagName("parameter");
              for (int j = 0; j < paranodeList.getLength(); j++) {
                Node paraNode = paranodeList.item(j);
                Node nameNode = paraNode.getAttributes().getNamedItem("name");
                Node valueNode = paraNode.getAttributes().getNamedItem("value");
                if (nameNode != null && valueNode != null) {
                  t.put(nameNode.getNodeValue(), valueNode.getNodeValue());
                  System.out.println(nameNode.getNodeValue()+"\t"+valueNode.getNodeValue());
                }
              }

            } catch (Exception e) {
              e.printStackTrace();
              continue;
            }

          }

        }
        if (c.getImplementionmap().size() > 0) {
          LOG.info("class " + c.getClassName() + " load " + c.getImplementionmap().size() + " implmentations");
          this.pluginMap.put(c.getClassName(), c);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public static PluginParser getInstance() {
    if (_instance == null) {
      synchronized (PluginParser.class) {
        if (_instance == null) {
          _instance = new PluginParser();
        }
      }
    }
    return _instance;

  }

  public Plugin getPlugin(String pluginName) {
    if (this.pluginMap.containsKey(pluginName))
      return this.pluginMap.get(pluginName);
    else
      return null;
  }

  public static void main(String args[]) {
    PluginParser parser = PluginParser.getInstance();

  }

}
