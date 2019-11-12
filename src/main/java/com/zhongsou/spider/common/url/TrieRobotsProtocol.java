package com.zhongsou.spider.common.url;

// import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import com.sun.tools.javac.util.Pair;

public class TrieRobotsProtocol extends RobotsProtocol {

	boolean containsWildCard;
	TrieNode root = new TrieNode((byte) ' ', (byte) 0);
	List<Pair<Boolean, String>> wildCardList = null;

  /**
   * 检查这个Url是否合乎规则，allow返回true,disallow 返回为false
   *
   * @param path
   * @return
   */
  @Override
  public boolean checkURL(String path) {
    TrieNode curr = root;
    TrieNode next = null;

		byte s[] = path.getBytes();
		int i = 0;
		boolean passed = false;
		do {
			next = curr.getOneChild(s[i]+128);
			if (next == null) {
				passed = true;
				break;
			} else {
				if (next.flag == -1) {
					{
						passed = false;
						break;
					}
				} else if (next.flag == 1) {
					{
						passed = true;
						curr = next;
						// 如果true，不break，继续判断
						// break;
					}
				} else {
					curr = next;
				}
			}
			i++;

    // url 没有通过trie树
    if (!passed) {
      return false;
    }

    // 以下检测通配符的匹配
    if (this.wildCardList != null) {
      for (Pair<Boolean, String[]> p : this.wildCardList) {
        boolean allow = p.fst;
        int pos = 0;
        for (int m = 0; m < p.snd.length; m++) {
          pos = path.indexOf(p.snd[m], pos);
          if (pos == -1)
            break;
          // 如果匹配到最后的一个字符串
          if (m == p.snd.length - 1 && !allow) {
            StringBuffer buf = new StringBuffer();
            for (int j = 0; j < p.snd.length; j++) {
              buf.append(p.snd[j]);
            }
            return false;
          } else if (m == p.snd.length - 1 && allow) {
            StringBuffer buf = new StringBuffer();
            for (int j = 0; j < p.snd.length; j++) {
              buf.append(p.snd[j]);
            }
            return true;
          }
        }
      }
    }
    return true;
  }

		// 以下检测通配符的匹配
		if (this.wildCardList != null) {
			for (Pair<Boolean, String> p : this.wildCardList) {
				boolean allow = p.fst;
				boolean match = matchWildCard(path, p.snd);
				if (match) {
					if (!allow) {
						return false;
					}
					else if(allow)
					{
						return true;
					}
				} else {
//					if (allow) {
//						return false;
//					}
				}
			}
		}

		return passed;
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
			byte t[] = s.getBytes();
			TrieNode curr = root;
			TrieNode next = null;
			for (int i = 1; i < t.length; i++) {
				if (i != t.length - 1) {
					next = curr.addOneChild((t[i]+128), (byte) 0);
				} else {
					if (t[0] == (byte) '-')
						next = curr.addOneChild((t[i]+128), (byte) -1);
					else
						next = curr.addOneChild((t[i]+128), (byte) 1);
				}
				curr = next;
			}
		}

    /**
     * 0 代表中间结点，-1代表disallow，1代表allow
     */
    byte flag;

    public TrieNode(byte k, byte flag) {
      this.v = k;
      this.flag = flag;
      child = null;
    }

    public String toString() {
      return new String((char)v + "\t" + String.valueOf(flag));
    }

    /**
     *
     * 添加一个孩子结点
     *
     * @param v
     * @param isLeaf
     * @return
     */
    public TrieNode addOneChild(byte v, byte flag) {
      if (this.child == null) {
        child = new TrieNode[256];
        child[v] = new TrieNode(v, flag);
        // Log.info("i am in branch 1: " + child[v].toString());
        // return child[v];
      } else {
        if (this.child[v] == null) {
          child[v] = new TrieNode(v, flag);
          // Log.info("i am in branch 2.1: " + child[v].toString());
          // return child[v];
        } else {                        // 原来是中间，现在是叶子
          if ((child[v].flag == 0) && (flag != 0)) {
            // Log.info("i am in branch 2.2 before flag change: " + child[v].toString());
            child[v].flag = flag;
            // return child[v];
            // Log.info("i am in branch 2.2: after flag change: " + child[v].toString());
          }
          // return child[v];
        }
      }
      return child[v];
    }

		/**
		 * 
		 * 添加一个孩子结点
		 * 
		 * @param v
		 * @param isLeaf
		 * @return
		 */
		public TrieNode addOneChild(int v, byte flag) {
			if (this.child == null) {
				child = new TrieNode[256];
				child[v] = new TrieNode((byte)v, flag);
				return child[v];
			} else {
				if (this.child[v] == null) {
					child[v] = new TrieNode((byte)v, flag);
					return child[v];
				} else {
					if (child[v].flag == 0 && flag != 0) {
						child[v].flag = flag;
						return child[v];
					}
					return child[v];
				}
			}
		}

		public TrieNode getOneChild(int a) {
			if (this.child == null || this.child[a] == null)
				return null;
			else
				return child[a];
		}
	}
}
