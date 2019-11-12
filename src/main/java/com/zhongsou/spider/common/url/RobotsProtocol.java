package com.zhongsou.spider.common.url;

public abstract class  RobotsProtocol {

	abstract public boolean init(String str[]);
	/**
	 * 
	 * 检查这个Url是否合乎规则，allow返回true,disallow 返回为false
	 * 
	 * @param path
	 * @return
	 */
	public abstract boolean checkURL(String path);
	public  boolean matchWildCard(String str, String wildcard)
	{
		//所有通配符串匹配到str的前面一部分，即str有多余的字符，仍然当作匹配
		return matchWildCard(str,wildcard,true);
	}
	
	/**
	 * 
	 * 通配符匹配算法
	 * 参见：http://www.drdobbs.com/architecture-and-design/matching-wildcards
	 * -an-algorithm/210200888
	 * 
	 * @param str
	 * @param wildcard
	 * @return
	 */
	public  boolean matchWildCard(String str, String wildcard,boolean prefixMatch) {
		boolean matched = true;

		int pAfterLastWild = -1; // The location after the last '*', if we’ve
									// encountered one
		int pAfterLastTame = -1; // The location in the tame string, from which
									// we started after last wildcard
		char t, w;
		int strPos = 0, wildCardPos = 0;
		// Walk the text strings one character at a time.
		while (true) {
			// 到达最后一个字符
			if (strPos >= str.length()) {
				// 通配符串也到最后一个字符
				if (wildCardPos == wildcard.length()) {
					break;
				}
				// 通配符串后面是一个*,匹配当前字符，+1 继续探测通配符串
				else if (wildcard.charAt(wildCardPos) == '*') {
					wildCardPos++;
					continue;
				}
				// 通配符串后不是*,那么回溯到上一个有通配符的位置
				else if (pAfterLastTame != -1) {
					// 回溯的位置，str为空，但是通配串不为空,
					 if (pAfterLastTame >= str.length()) {
					 matched = false;
					 break;
					 }
					strPos = pAfterLastTame++;
					wildCardPos = pAfterLastWild;
					continue;

				}
				// 没有上一次匹配的通配符的位置，源串没有字符，通配符还有字符，不匹配退出
				matched = false;
				break;
			}
			
			t = str.charAt(strPos);

			// w使用最大值,假定源串中没有最后这个字符
			if (wildCardPos >= wildcard.length()) {
				//如果部分匹配
				if(prefixMatch)
					break;
				else
					w = Character.MAX_VALUE;
			} else {
				w = wildcard.charAt(wildCardPos);
			}

			if (t != w) {
				if (w == '*') {
					pAfterLastWild = ++wildCardPos;
					pAfterLastTame = strPos;
					// 已经到通配串的最后一个位置，并且最后一个串是*,全部匹配，跳出
					if (wildCardPos == wildcard.length()) {
						break;
					}
					continue;
				} else if (pAfterLastWild != -1) {
					// 找到上一个通配符的位置，继续匹配
					if (pAfterLastWild != wildCardPos) {
						wildCardPos = pAfterLastWild;
						assert (wildCardPos <= wildcard.length() - 1);
						w = wildcard.charAt(wildCardPos);
						if (t == w) {
							wildCardPos++;
						}

					}
					// 用上一个通配符的*吃掉下一个源串的字符
					strPos++;
					continue;
				} else {
					matched = false;
					break;
				}
			}

			// 源串仍有字符串
			strPos++;
			wildCardPos++;

		}

		return matched;
	}

	
}
