package com.zhongsou.spider.hbase.test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import com.zhongsou.spider.common.util.NumberUtil;

public class AvroTest2 {

	Schema schema;
	String fileName;
	DatumReader<GenericArray> arrayReader;

	public AvroTest2(String name, String schemaFile) {
		this.fileName = name;
		Schema.Parser parser = new Schema.Parser();
		try {
			schema = parser.parse(new File(schemaFile));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(schema.toString(true));
	}

	String getConent(String name, GenericRecord t) {
		Object o = t.get(name);
		if (o instanceof ByteBuffer) {
			ByteBuffer buf = (ByteBuffer) o;
			try {
				String s = null;

				if (!name.equals("down_content")) {
					s = new String(buf.array(), "utf-8");
					System.err.println("name=" + name + " value=" + s + "\t" + buf.array().length);
				} else {
					s = new String(buf.array(), 0, buf.remaining());
					System.out.println(s + "\t" + buf.array().length);
				}
				return s;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	public void readFile() {

		File f = new File(this.fileName);
		byte a[] = new byte[(int) f.length()];
		try {
			BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File(this.fileName)));
			byte b[] = new byte[4096];
			int i = 0;
			int sum = 0;
			while ((i = input.read(a, sum, a.length - sum)) != -1) {
				sum += i;
				System.out.println("sum" + sum);
				if (sum == f.length())
					break;
			}
			input.close();
			BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(a, null);
			DatumReader<GenericRecord> greader = new GenericDatumReader<GenericRecord>(schema);
			GenericRecord result = null;
			int offset=0;
			while (true) {
				int length=NumberUtil.readInt(a, offset);
				System.out.println("new length="+length);
				
				decoder = DecoderFactory.get().binaryDecoder(a,offset+4,length, decoder);
				result = greader.read(null, decoder);
				if (result == null)
					break;

				getConent("down_status", result);
				getConent("down_encode", result);
				// getConent("parser_content", result);
				// getConent("down_contenttype", result);
				// getConent("down_url", result);
				// getConent("parser_filetype", result);
				// getConent("parser_status", result);
				// getConent("parser_pagetype", result);
				// getConent("parser_category", result);
				// getConent("parser_sitetitle", result);
				// getConent("parser_realtitle", result);
				// getConent("parser_keyword", result);
				// getConent("parser_description", result);
				// getConent("simhash", result);
				// getConent("seg_content", result);
				// getConent("down_content", result);

				Object o = result.get("parser_link");
				if (o instanceof GenericArray) {
					GenericArray array = (GenericArray) o;
					System.out.println("array size="+array.size());
					for (int k = 0; k < array.size(); k++) {
						GenericRecord record = (GenericRecord) array.get(k);
						ByteBuffer buffer = (ByteBuffer) (((GenericRecord) record).get("url"));
						System.out.println("url\t" + new String(buffer.array(), "gbk"));
						buffer = (ByteBuffer) (((GenericRecord) record).get("text"));
						System.out.println("title\t" + new String(buffer.array(), "gbk"));
					}
				}
				offset+=(4+length);
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {
		AvroTest2 p = new AvroTest2("/home/dape/a.txt", "/home/dape/Documents/programs/spider_common/test/com/zhongsou/spider/hbase/test/resources/schema.avro");
		p.readFile();

	}

}
