package com.zhongsou.spider.common.url;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 *
 * robots 协议的处理
 *
 * @author dape
 *
 */
public class RobotsProtocolParser {

  public RobotsProtocolParser() {
    robotNames.put("zhongsouspier", 1);
    robotNames.put("googlebot", 2);
    robotNames.put("baiduspider", 3);
    robotNames.put("*", 4);

  }

  private static final int NO_PRECEDENCE = Integer.MAX_VALUE;

  private static final RobotRuleSet EMPTY_RULES = new RobotRuleSet();

  private static RobotRuleSet FORBID_ALL_RULES = getForbidAllRules();

  private HashMap<String, Integer> robotNames = new HashMap<String, Integer>();

  static RobotRuleSet getForbidAllRules() {
    RobotRuleSet rules = new RobotRuleSet();
    rules.addPrefix("", false);
    return rules;
  }

  /**
   *
   * 根据robots.txt的内容，选择合适的robots user-agent来进行下载
   *
   *
   * @param content
   * @return
   */
  public String parserRobotsText(String content) {
    RobotRuleSet set = this.parseRules(content);
    if (set == EMPTY_RULES)
      return "";
    else
      return set.toString();
  }

  /**
   * This class holds the rules which were parsed from a robots.txt file, and
   * can test paths against those rules.
   */
  public static class RobotRuleSet {
    ArrayList<RobotsEntry> tmpEntries = new ArrayList<RobotsEntry>();
    RobotsEntry[] entries = null;
    long expireTime;
    long crawlDelay = -1;

    /**
     */
    private class RobotsEntry {
      String prefix;
      boolean allowed;

      RobotsEntry(String prefix, boolean allowed) {
        this.prefix = prefix;
        this.allowed = allowed;
      }
    }

    /**
     */
    private void addPrefix(String prefix, boolean allow) {
      if (tmpEntries == null) {
        tmpEntries = new ArrayList<RobotsEntry>();
        if (entries != null) {
          for (int i = 0; i < entries.length; i++)
            tmpEntries.add(entries[i]);
        }
        entries = null;
      }

      tmpEntries.add(new RobotsEntry(prefix, allow));
    }

    /**
     */
    private void clearPrefixes() {
      if (tmpEntries == null) {
        tmpEntries = new ArrayList<RobotsEntry>();
        entries = null;
      } else {
        tmpEntries.clear();
      }
    }

    /**
     * Change when the ruleset goes stale.
     */
    public void setExpireTime(long expireTime) {
      this.expireTime = expireTime;
    }

    /**
     * Get expire time
     */
    public long getExpireTime() {
      return expireTime;
    }

    /**
     * Get Crawl-Delay, in milliseconds. This returns -1 if not set.
     */
    public long getCrawlDelay() {
      return crawlDelay;
    }

    /**
     * Set Crawl-Delay, in milliseconds
     */
    public void setCrawlDelay(long crawlDelay) {
      this.crawlDelay = crawlDelay;
    }

    /**
     * Returns <code>false</code> if the <code>robots.txt</code> file
     * prohibits us from accessing the given <code>url</code>, or
     * <code>true</code> otherwise.
     */
    public boolean isAllowed(URL url) {
      String path = url.getPath(); // check rules
      if ((path == null) || "".equals(path)) {
        path = "/";
      }
      return isAllowed(path);
    }

    /**
     * Returns <code>false</code> if the <code>robots.txt</code> file
     * prohibits us from accessing the given <code>path</code>, or
     * <code>true</code> otherwise.
     */
    public boolean isAllowed(String path) {
      try {
        path = URLDecoder.decode(path, "utf-8");
      } catch (Exception e) {
        // just ignore it- we can still try to match
        // path prefixes
      }

      if (entries == null) {
        entries = new RobotsEntry[tmpEntries.size()];
        entries = tmpEntries.toArray(entries);
        tmpEntries = null;
      }

      int pos = 0;
      int end = entries.length;
      while (pos < end) {
        if (path.startsWith(entries[pos].prefix))
          return entries[pos].allowed;
        pos++;
      }

      return true;
    }

    /**
     */
    @Override
    public String toString() {
      isAllowed("x"); // force String[] representation
      StringBuffer buf = new StringBuffer();
      for (int i = 0; i < entries.length; i++)
        if (entries[i].allowed)
          buf.append("+" + entries[i].prefix
                     + System.getProperty("line.separator"));
        else
          buf.append("-" + entries[i].prefix
                     + System.getProperty("line.separator"));
      return buf.toString();
    }
  }

  /**
   *
   * 分析robots协议的内容
   *
   * @param content
   * @return
   */
  RobotRuleSet parseRules(String content) {
    StringTokenizer lineParser = new StringTokenizer(content, "\n\r");

    RobotRuleSet bestRulesSoFar = null;
    int bestPrecedenceSoFar = NO_PRECEDENCE;

    RobotRuleSet currentRules = new RobotRuleSet();
    int currentPrecedence = NO_PRECEDENCE;

    boolean addRules = false; // in stanza for our robot
    boolean doneAgents = false; // detect multiple agent lines

    while (lineParser.hasMoreTokens()) {
      String line = lineParser.nextToken();

      // trim out comments and whitespace
      int hashPos = line.indexOf("#");
      if (hashPos >= 0)
        line = line.substring(0, hashPos);
      line = line.trim();

      if ((line.length() >= 11) &&
          (line.substring(0, 11).equalsIgnoreCase("User-agent:"))) {

        if (doneAgents) {
          if (currentPrecedence < bestPrecedenceSoFar) {
            bestPrecedenceSoFar = currentPrecedence;
            bestRulesSoFar = currentRules;
            currentPrecedence = NO_PRECEDENCE;
            currentRules = new RobotRuleSet();
          }
          addRules = false;
        }
        doneAgents = false;

        String agentNames = line.substring(line.indexOf(":") + 1);
        agentNames = agentNames.trim();
        StringTokenizer agentTokenizer = new StringTokenizer(agentNames);

        while (agentTokenizer.hasMoreTokens()) {
          // for each agent listed, see if it's us:
          String agentName = agentTokenizer.nextToken().toLowerCase()
                             .trim();
          // System.out.println("agents="+agentName);
          Integer precedenceInt = robotNames.get(agentName);

          if (precedenceInt != null) {
            int precedence = precedenceInt.intValue();
            if ((precedence < currentPrecedence)
                && (precedence < bestPrecedenceSoFar))
              currentPrecedence = precedence;
            //System.out.println("agents=" + agentName);
          }
        }

        if (currentPrecedence < bestPrecedenceSoFar)
          addRules = true;

      } else if ((line.length() >= 9)
                 && (line.substring(0, 9).equalsIgnoreCase("Disallow:"))) {

        doneAgents = true;
        String path = line.substring(line.indexOf(":") + 1);
        path = path.trim();
        try {
          path = URLDecoder.decode(path, "utf-8");
        } catch (Exception e) {

        }

        if (path.length() == 0) { // "empty rule"
          if (addRules)
            currentRules.clearPrefixes();
        } else { // rule with path
          if (addRules)
            currentRules.addPrefix(path, false);
        }

      } else if ((line.length() >= 6)
                 && (line.substring(0, 6).equalsIgnoreCase("Allow:"))) {

        doneAgents = true;
        String path = line.substring(line.indexOf(":") + 1);
        path = path.trim();

        if (path.length() == 0) {
          // "empty rule"- treat same as empty disallow
          if (addRules)
            currentRules.clearPrefixes();
        } else { // rule with path
          if (addRules)
            currentRules.addPrefix(path, true);
        }
      } else if ((line.length() >= 12)
                 && (line.substring(0, 12).equalsIgnoreCase("Crawl-Delay:"))) {
        doneAgents = true;
        if (addRules) {
          long crawlDelay = -1;
          String delay = line.substring("Crawl-Delay:".length(),
                                        line.length()).trim();
          if (delay.length() > 0) {
            try {
              crawlDelay = Long.parseLong(delay) * 1000; // sec to
              // millisec
            } catch (Exception e) {

            }
            currentRules.setCrawlDelay(crawlDelay);
          }
        }
      }
    }

    if (currentPrecedence < bestPrecedenceSoFar) {
      bestPrecedenceSoFar = currentPrecedence;
      bestRulesSoFar = currentRules;
    }

    if (bestPrecedenceSoFar == NO_PRECEDENCE)
      return EMPTY_RULES;
    return bestRulesSoFar;
  }

  /**
   * Returns a {@link RobotRuleSet} object which encapsulates the rules parsed
   * from the supplied <code>robotContent</code>.
   */
  RobotRuleSet parseRules(byte[] robotContent) {
    if (robotContent == null)
      return EMPTY_RULES;

    String content = new String(robotContent);
    return this.parseRules(content);

  }

  public static void main(String args[]) {

    File file = new File("/tmp/robots.txt");
    int filelen = (int) file.length();
    System.out.println("file length = " + filelen);
    byte[] content = new byte[(int) (filelen)];

    try {
      BufferedInputStream stream = new BufferedInputStream(
          new FileInputStream(file));
      int len = 0;
      byte k[] = new byte[4096];
      int offset = 0;
      while ((len = stream.read(content, offset, filelen - offset)) != -1) {
        offset += len;
        if (offset == filelen)
          break;
      }
      System.out.println("read length = " + len);
      stream.close();
      RobotsProtocolParser parser = new RobotsProtocolParser();
      RobotRuleSet set = parser.parseRules(content);
      System.out.println(set.toString());
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
