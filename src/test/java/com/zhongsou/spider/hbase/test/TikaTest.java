package com.zhongsou.spider.hbase.test;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.zhongsou.incload.DupPair;

public class TikaTest {

	public static void main(String args[]) {
		String result = null;
		// if (args.length < 1) {
		// System.out.println("java TikaTest fileName");
		// System.exit(0);
		// }
		// Tika tika = new Tika();
		// Parse all given files and print out the extracted text content
		/*
		 * for (String file : args) { String text; try { text =
		 * tika.parseToString(new File(file)); System.out.print(text); } catch
		 * (IOException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } catch (TikaException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 * 
		 * }
		 */
		ByteArrayOutputStream barray = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(barray);
		DupPair o = new DupPair();
		try {
			o.write(out);
			byte[] array = barray.toByteArray();
			for (int i = 0; i < array.length; i++) {
				System.out.println(String.valueOf(array[i]));
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

}
