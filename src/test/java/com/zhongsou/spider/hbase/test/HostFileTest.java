package com.zhongsou.spider.hbase.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.mortbay.log.Log;

import com.sun.tools.hat.internal.parser.Reader;
import com.zhongsou.spider.common.util.URLUtil;

public class HostFileTest {
	String fileName;
	int maxLevelOneHost = 1000;
	int maxLevelTwoHost = 100;
	HashSet<String> blackDomain = new HashSet<String>();
	HashSet<String> blackHost = new HashSet<String>();

	public HostFileTest(String file) {

		this.fileName = file;
	}

	public void test() {

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(this.fileName))));
			String domain = "";
			String reverseDomain = "";
			String host = "";
			boolean first = true;
			String lastLevelOneHost = "";
			String lastLevelTwoHost = "";
			int levelOneHostNum = 0;
			int levelTwoHostNum = 0;
			String lastDomain = "";
			String line = "";
			while ((line = reader.readLine()) != null) {
				host = line.trim();
				// System.out.println(host);
				// if (first) {
				domain = URLUtil.getDomainByHost(URLUtil.hostReverse(host));
				if (!domain.equals(lastDomain)) {
					lastDomain = domain;
					levelOneHostNum = 0;
					levelTwoHostNum = 0;
					lastLevelOneHost = "";
					lastLevelTwoHost = "";
				}
				reverseDomain = URLUtil.hostReverse(domain);
				first = false;
				// }
				if (this.blackDomain.contains(reverseDomain))
					continue;

				if (host.length() == reverseDomain.length() && host.equals(reverseDomain)) {
					if (!lastLevelOneHost.equals(host)) {
						lastLevelOneHost = host;
						levelOneHostNum++;
						levelTwoHostNum = 0;
						Log.debug("find level one host " + host + "\t" + levelOneHostNum + "\t" + domain);
						if (levelOneHostNum > this.maxLevelOneHost) {
							Log.info("exceed max num of level one host" + domain + "\t" + this.maxLevelOneHost + "\t" + levelOneHostNum);
							this.blackDomain.add(URLUtil.hostReverse(domain));
							continue;
						}
					}

					continue;
				}

				int t = host.indexOf(reverseDomain);
				if (t == -1) {
					Log.info("error host " + host + "\t" + reverseDomain);

					continue;

				} else {
					int levelOneDot = -1;
					if (host.length() <= reverseDomain.length() + 1) {
						continue;
					}
					levelOneDot = host.indexOf(".", reverseDomain.length() + 1);
					if (levelOneDot == -1) {
						if (this.blackHost.contains(host))
							continue;
						if (!lastLevelOneHost.equals(host)) {
							lastLevelOneHost = host;
							levelOneHostNum++;
							Log.info("find level one host " + lastLevelOneHost + "\tnum=" + levelOneHostNum + "\tdomain=" + domain + "\tlastLeveltwoNum" + levelTwoHostNum);
							levelTwoHostNum = 0;
							if (levelOneHostNum > this.maxLevelOneHost) {
								
								Log.info("exceed max num of level one host" + domain + "\t" + this.maxLevelOneHost + "\t" + levelOneHostNum);
								this.blackDomain.add(URLUtil.hostReverse(domain));
								continue;
							}
						}

					} else {
						String levelOneHost = host.substring(0, levelOneDot);
						if (this.blackHost.contains(levelOneHost))
							continue;
						if (!levelOneHost.equals(lastLevelOneHost)) {
							lastLevelOneHost = levelOneHost;
							levelOneHostNum++;
							Log.info("find level one host " + lastLevelOneHost + "\tnum=" + levelOneHostNum + "\tdomain=" + domain + "\tlastLeveltwoNum" + levelTwoHostNum);
							levelTwoHostNum = 0;
							if (levelOneHostNum > this.maxLevelOneHost) {
								Log.info("exceed max num of level one host" + domain + "\t" + this.maxLevelOneHost + "\t" + levelOneHostNum);
								this.blackDomain.add(URLUtil.hostReverse(domain));
								continue;
							}

						}

						if (host.length() > levelOneHost.length()) {
							int levelTwoDot = host.indexOf(".", levelOneHost.length() + 1);
							String levelTwoHost = "";
							if (levelTwoDot == -1) {
								levelTwoHost = host;
							} else {
								levelTwoHost = host.substring(0, levelTwoDot);
							}

							if (!levelTwoHost.equals(lastLevelTwoHost)) {
								lastLevelTwoHost = levelTwoHost;
								levelTwoHostNum++;
								Log.debug("find level two host " + levelTwoHost + " last level two host" + levelTwoHost + "\t level one host=" + levelOneHost + "\tlevel two num=" + levelTwoHostNum
										+ "\tdomain=" + domain);
								if (levelTwoHostNum > this.maxLevelTwoHost) {
									Log.info("exceed max num of level two host" + domain + "\t" + this.maxLevelOneHost + "\t" + levelOneHostNum);
									this.blackHost.add(levelOneHost);
									continue;
								}
							}
						}

					}

				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (String bdomain : this.blackDomain) {
			System.out.println("black domain\t" + bdomain);
		}

		for (String bhost : this.blackHost) {
			System.out.println("black host\t" + bhost);
		}
	}

	public static void main(String args[]) {

		HostFileTest fileTest = new HostFileTest("/home/dape/hostinfo/part-r-00001");
		fileTest.test();

	}

}
