package com.zhongsou.spider.hbase.test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.util.StringUtils;

import com.zhongsou.spider.common.util.NumberUtil;

public class NumberTest {

	public static List<String> readFile() {
		List<String> list = new LinkedList<String>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/home/dape/a.txt"))));
			String str;
			while ((str = reader.readLine()) != null) {
				// System.out.println(str);
				String tmp[] = str.split("\t");
				String t1 = tmp[0];
				String t3 = "";
				for (int j = 0; j < t1.length(); j += 4) {

				}

				String t2 = tmp[1];
				char t[] = t2.toCharArray();
				String res = "0e";
				for (int i = 0; i < t.length; i++) {
					int b = t[i];
					res = res + Integer.toHexString(b);

					// System.out.print("char c="+t[i]+" d="+b+"\t"+Integer.toHexString(b));
				}

				String res2 = res.substring(res.length() - 1) + res.substring(0, res.length() - 1);
				System.out.println(t1 + "\t" + res);

				byte b1[] = StringUtils.hexStringToByte(t1);
				byte b2[] = StringUtils.hexStringToByte(res);
				int i = 0;
				for (i = 0; i < t1.length(); i += 2) {
					System.out.print(t1.substring(i, i + 2) + "\t");
				}
				System.out.println();

				for (i = 0; i < res.length(); i += 2) {
					System.out.print(res.substring(i, i + 2) + "\t");
				}
				System.out.println();
				// //
				// for(i=0;i<b1.length-1;i++)
				// {
				// System.out.print(""+Integer.toHexString((b1[i]&b1[i+1]))+"\t");
				// }
				// System.out.println();
				// for(i=0;i<b1.length-1;i++)
				// {
				// System.out.print(""+Integer.toHexString((b1[i]|b1[i+1]))+"\t");
				// }
				// System.out.println();
				//
				byte a = b2[0];
				for (i = 1; i < b2.length ; i++) {
					a = (byte) (a ^ b2[i]);
					if (a != (byte)0) {
						a = (byte) (a - (byte) 1);
					} else {
						a = (byte) 0xff;
					}
					System.out.print("" + Integer.toHexString(a) + "\t");
				}
				System.out.println();
				// for(i=0;i<b1.length-1;i++)
				// {
				// System.out.print(""+Integer.toHexString((b1[i]+b1[i+1]))+"\t");
				// }
				// System.out.println();
				// //
				// for(i=0;i<b1.length-1;i++)
				// {
				// System.out.print(""+Integer.toHexString((b1[i]^(~b1[i+1])))+"\t");
				// }
				// System.out.println();
				//
				//

				list.add(str);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}

	public static void decimatohex(long l) {

		String t = "";

	}

	public static void main(String args[]) {
		NumberTest.readFile();

	}
}