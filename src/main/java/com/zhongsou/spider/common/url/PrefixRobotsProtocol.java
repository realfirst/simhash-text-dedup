package com.zhongsou.spider.common.url;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.sun.tools.javac.util.Pair;

public class PrefixRobotsProtocol extends RobotsProtocol {

  boolean containsWildCard;
  //
  List<String> prefixList = null;
  List<Pair<Boolean, String>> wildCardList = null;

  @Override
  public boolean checkURL(String path) {
    // TODO Auto-generated method stub
    if (prefixList != null) {
      for (String t : prefixList) {
        // 匹配到前缀
        boolean prefixm = true;
        for (int i = 1; i < t.length(); i++) {
          if (i-1 >= path.length()) {
            prefixm = true;
            break;
          }
          if (path.charAt(i - 1) == t.charAt(i)) {
            continue;
          } else {
            prefixm = false;
            break;
          }
        }

        if (prefixm) {
          //如果匹配的一个allow的协议，直接返回
          if (t.charAt(0) == '+') {
            //                                          return true;
            break;
          } else {
            //匹配了一个disallow的前缀，返回
            return false;
          }
        }
      }

    }

    // 以下检测通配符的匹配
    if (this.wildCardList != null) {
      for (Pair<Boolean, String> p : this.wildCardList) {
        boolean allow = p.fst;
        boolean match = matchWildCard(path, p.snd);
        if (match) {
          if (!allow) {
            return false;
          } else if (allow) {
            return true;
          }
        } else {
          // if (allow) {
          // return false;
          // }
        }
      }
    }
    //没有匹配前缀与没有匹配通配符，allow
    return true;
  }


  @Override
  public boolean init(String[] str) {
    // TODO Auto-generated method stub

    for (String s : str) {
      // 如果最后一个字符是*或者$替换掉
      if (s.indexOf("*") == s.length() - 1 || s.endsWith("$")) {
        s = s.substring(0, s.length() - 1);
      }
      if (s.contains("*")) {
        if (wildCardList == null) {
          wildCardList = new LinkedList<Pair<Boolean, String>>();
        }
        if (s.charAt(0) == '-') {
          wildCardList.add(new Pair(false, s.substring(1)));
        } else {
          wildCardList.add(new Pair(true, s.substring(1)));
        }

        continue;
      }

      if (this.prefixList == null) {
        this.prefixList = new ArrayList<String>(str.length);
      }
      prefixList.add(s);
    }

    return true;
  }

}
