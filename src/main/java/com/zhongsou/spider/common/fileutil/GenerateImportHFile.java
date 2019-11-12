package com.zhongsou.spider.common.fileutil;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import com.zhongsou.spider.common.util.MD5;

public class GenerateImportHFile {

	Random rand = new Random();
	static char letter[] = "abcdefghijklmnopqrstuvwxyz".toCharArray();

	String generateURL() {
		StringBuffer buffer = new StringBuffer("http://");
		int t = rand.nextInt(512) + 64;
		int m = 0;
		int hostDot = rand.nextInt(2) + 2;
		int dot = 0;
		char lastch;
		for (int i = 0; i < t; i++) {
			m = rand.nextInt(26);
			lastch = letter[m];
			buffer.append(lastch);
			if (dot == hostDot && m % 7 == 0 && lastch != '/') {
				lastch = '/';
				buffer.append("/");
				i++;
			}
			if (dot < hostDot && lastch != '.' && m % 3 == 0) {
				buffer.append(".");
				dot++;
				lastch = '.';
				i++;
			}

		}

		return buffer.toString();
	}

	private String destFile;
	private int size;

	public GenerateImportHFile(String destFile, int size) {
		this.destFile = destFile;
		this.size = size;
	}

	public void generate() {
		BufferedOutputStream writer = null;
		byte t[] = new byte[1024];
		int dataSize = 0;
		try {
			writer = new BufferedOutputStream(new FileOutputStream(destFile));
			for (int i = 0; i < size; i++) {
				String url = this.generateURL();
				byte[] md5 = MD5.digest8(url.getBytes()).getDigest();
				writer.write(md5, 0, 8);
				writer.write("\t".getBytes("utf-8"));
				writer.write(url.getBytes("utf-8"));
				writer.write("\t".getBytes("utf-8"));
				dataSize = this.generateData(t);
				writer.write(t, 0, dataSize);
				writer.write("\r\n".getBytes("utf-8"));
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	int generateData(byte src[]) {

		int size = this.rand.nextInt(512) + 512;
		for (int i = 0; i < size; i++) {
			src[i] = (byte) ('a' + rand.nextInt(26));
		}
		return size;
	}

	public static void main(String args[]) {
		if (args.length < 2) {
			System.out.println("usage java com.zhongsou.spider.common.fileutil.GenerateImportHFile filename size ");
			return;
		}
		GenerateImportHFile f = new GenerateImportHFile(args[0], Integer.parseInt(args[1]));
		f.generate();
	}
}
