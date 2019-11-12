package com.zhongsou.spider.common.url;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class BMTrieRobots {

  static class BMTrieNode {
    byte label;                         // 256 ascii char
    byte flag;                          // value={-1, 0, 1}
    byte [] bitmap;                     // indicate child postion
    BMTrieNode [] children;             // child link
    int numChildren;                    // remaining children for per traverse
    int savedPos;                       // for array storage

    public BMTrieNode(byte alph, byte permit) {
      label = alph;
      flag = permit;
      children = null;
      bitmap = new byte [32];
      savedPos = -1;
    }

    public BMTrieNode addChild(byte label, byte flag) {
      if (this.children == null) {
        children = new BMTrieNode[256];
        children[label] = new BMTrieNode(label, flag);
      } else {
        if (this.children[label] == null) {
          children[label] = new BMTrieNode(label, flag);
        } else {
          // previous is internal node, now is end node
          if ((children[label].flag == 0) && (flag != 0)) {
            children[label].flag = flag;
          }
        }
      }
      return children[label];
    }

    // get first not-null child
    public BMTrieNode getChild() {
      BMTrieNode child = null;
      for (int i = 0; i < children.length; i++) {
        if (children[i] != null) {
          child = children[i];
        }
      }
      return child;
    }

    public void setChildBit(byte childLabel) {
      int i = childLabel / 8;
      bitmap[i] |= 1 << (7 - childLabel % 8);
    }

    public int countChildren() {
      int sum = 0;
      byte x;
      int n;
      for (int i = 0; i < bitmap.length; i++) {
        x = (byte) (bitmap[i] & 0x000000ff);
        n = 0;
        while (x != 0) {
          ++n;
          x &= (x - 1);
        }
        sum += n;
      }
      return sum;
    }

    @Override
    public String toString() {
      return "" + (char)label + "\t" + String.valueOf(flag);
    }

  }

  /**
   * Describe <code>build</code> method here.
   *
   * @param noWildcards a <code>String</code> value
   * @return a <code>root of BMTrie</code> value
   */
  private BMTrieNode build(String noWildcards) {
    BMTrieNode root = new BMTrieNode((byte)' ', (byte)0);

    String [] items = noWildcards.split("[\r\n]{1,}");
    int j = 0;
    for (String item : items) {
      System.out.println("item: " + j + " " +  item);
      j++;
      byte [] t = item.getBytes();
      BMTrieNode curr = root;
      BMTrieNode next = null;
      // build each branch
      for (int i = 1; i < t.length; i++) {
        if (i != (t.length - 1)) {
          next = curr.addChild(t[i], (byte)0);
        } else {
          if (t[0] == (byte)'-') {
            next = curr.addChild(t[i], (byte)-1);
          } else {
            next = curr.addChild(t[i], (byte)1);
          }
        }
        curr.setChildBit(t[i]);
        curr = next;
        System.out.println("" + curr);
      }
    }
    return root;
  }

  public byte [] traverseAndSave(String noWildcards) {
    BMTrieNode root = build(noWildcards);
    BMTrieNode curr = root;
    BMTrieNode next = null;
    byte [] trieBuf = new byte [1024 * 1024 * 2];
    byte [] nodeInfo = null;
    int localOffset= 0;
    int savedNodeNum = 0;

    // every node contains: label(1) + flag(1) + bitmap(32) + every children offset((int)4
    // * num of 1 in bitmap)
    int remainingChildren = 0;
    int nextChildPos = 0;
    int totalSavedLen = 0;
    int cfOffset = 0;
    for (int i = 0; i < root.countChildren(); i++) {
      while (curr.flag == 0) {
        if (curr.savedPos == -1) {
          curr.savedPos += totalSavedLen;
          curr.numChildren = curr.countChildren();
          nodeInfo = new byte [1 + 1 + 32 + 4 * curr.countChildren()];
          System.arraycopy(curr.label, 0, nodeInfo, 0, 1);
          System.arraycopy(curr.flag, 0, nodeInfo, 1, 1);
          System.arraycopy(curr.bitmap, 0, nodeInfo, 2, 32);
          totalSavedLen += nodeInfo.length;
          System.arraycopy(nodeInfo, 0, trieBuf, totalSavedLen, nodeInfo.length);
          next = curr.getChild();
          if (next.savedPos == -1) {
            nodeInfo = new byte[1 + 1 + 32 + 4 * next.countChildren()];
            System.arraycopy(next.label, 0, nodeInfo, 0, 1);
            System.arraycopy(next.flag, 0, nodeInfo, 1, 1);
            System.arraycopy(next.bitmap, 0, nodeInfo, 2, 32);
            cfOffset = totalSavedLen - curr.savedPos;
            System.arraycopy(cfOffset, 4, trieBuf,
                             totalSavedLen - cfOffset + 34 + 4 * (curr.countChildren() - curr.numChildren), 4);
            curr.numChildren--;
          } else {
            
          }

        } else {
          
        }

      }
      curr = next;

    }
    return null;
  }

  public static void main(String[] args) {
    try {
      File robotsFile =
          new File("/home/arch/zhongsou/project/spider_common/data/nowildcard-robots.txt");
      FileReader fr = new FileReader(robotsFile);
      BufferedReader br = new BufferedReader(fr);
      String line = null;
      StringBuilder sb = new StringBuilder();
      while ((line = br.readLine()) != null) {
        sb.append(line);
        sb.append("\n");
      }
      line = sb.toString();
      new BMTrieRobots().build(line);
      // System.out.println(line);
    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }
}
