package com.zhongsou.spider.hbase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class GenerateHost {
	static String domainSuffix[] = { "", "", "edu", "org", "com", "com.cn", "cn", "net", "edu.cn", "", "" };
	static char ch[] = "abcdefghijklmnopqrstuvwxyz1234560789".toCharArray();
	private int domainNum = 0;
	private int hostMax = 0;
	Random rand = new Random();
	static int domainMinLetter = 3;
	static int domainMaxLetter = 8;
	static int hostMaxLevel = 5;
	static int hostMinLetter = 1;
	static int hostMaxLetter = 9;
	static int basicURLNUM = 500;
	int hostCount;
	char domainBuffer[] = new char[8];
	static int chSize = ch.length;
	int domainHostMax;

	public GenerateHost(int domainNum, int domainHostMax, int hostNum) {
		this.domainNum = domainNum;
		this.hostMax = hostNum;
		this.domainHostMax = domainHostMax;
	}

	int getAGaussianNum(int min, int max, int mid) {
		int delay;
		do {
			double val = rand.nextGaussian() + mid;
			delay = (int) Math.round(val);
		} while (delay < min || delay > max);
		return delay;
	}

	int getBasicGussianNum(int min, int max) {
		int delay;
		do {
			double val = rand.nextGaussian() * 10000;
			delay = (int) Math.round(val);
		} while (delay < min || delay > max);
		return delay;
	}

	public void GenerateDomain() {
		int domaincount = 0;
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/tmp/host.txt"))));
			int index, domainhost;

			out: for (; domaincount < this.domainNum; domaincount++) {
				index = this.getAGaussianNum(2, 10, 5);
				int dLetterNum = rand.nextInt(domainMaxLetter - domainMinLetter) + domainMinLetter;
				for (int i = 0; i < dLetterNum; i++) {
					domainBuffer[i] = ch[rand.nextInt(chSize)];
				}
				String domain = new String(domainBuffer, 0, dLetterNum) + "." + domainSuffix[index];

				domainhost = this.getAGaussianNum(1, this.domainHostMax, 5);
				for (int i = 0; i < domainhost; i++) {
					int dhostNum = rand.nextInt(hostMaxLevel - hostMinLetter) + hostMinLetter;
					for (int k = 0; k < dhostNum; k++) {
						domainBuffer[k] = ch[rand.nextInt(chSize)];
					}
					String host = new String(domainBuffer, 0, dhostNum) + "." + domain;
					// System.out.println(host);
					writer.write(host + "\n");
					this.hostCount++;
					if (this.hostCount >= this.hostMax)
						break out;
					if (hostCount % 1000 == 0) {
						System.out.println("host count=" + hostCount);
					}
				}
			}
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("final host count=" + this.hostCount + " domain count=" + domaincount);
	}

	public void crateOneFile(List<String> hostList, int fileNum, int urlSumNum) {
		System.out.println("host size=" + hostList.size());
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/tmp/part-" + fileNum + ".txt"))));
			int leftURL = urlSumNum - hostList.size() * basicURLNUM;

			if (leftURL < 0) {
				leftURL = 0;
			}
			int hostURLNum = 0;
			for (String host : hostList) {
				hostURLNum = basicURLNUM;
				int otherPart = this.getBasicGussianNum(0, 1000000);
				if (leftURL > 0) {
					leftURL -= otherPart;
					hostURLNum += otherPart;
				}
				writer.write(host + "\t" + hostURLNum + "\n");
			}
			writer.close();
			if (leftURL > 0) {
				System.out.println("left url=" + leftURL);
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void splitFile(String inputFile, int oneFileHostNum, int oneHostURLSum) {
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputFile))));
			int i = 0;
			String line = null;
			int fileNum = 0;
			List<String> hostList = new LinkedList<String>();
			while ((line = reader.readLine()) != null) {
				if (i < oneFileHostNum) {
					hostList.add(line);
					i++;
					continue;
				}
				this.crateOneFile(hostList, fileNum++, oneHostURLSum);
				i = 0;
				hostList.clear();
				System.out.println("file num=" + fileNum);
			}
			this.crateOneFile(hostList, fileNum++, oneHostURLSum);
			

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {
		GenerateHost p = new GenerateHost(650000, 1000, 3000000);
		 p.GenerateDomain();
		p.splitFile("/tmp/host.txt", 100000, 166666666);

	}
}