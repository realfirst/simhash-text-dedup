package com.zhongsou.spider.hbase.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.zhongsou.spider.hadoop.HadoopFileSystemManager;

public class AvroTest {
	HadoopFileSystemManager hdfsManager = HadoopFileSystemManager.getInstance();

	Schema schema;
	String fileName;
	SequenceFile.Reader reader;
	Schema arraySchema;
	DatumReader<GenericArray> arrayReader;

	public AvroTest(String fileName) {

		try {
			Path path=new Path(fileName);
			reader = new SequenceFile.Reader(hdfsManager.getFileSystem(), path, new Configuration());
		} catch (Exception e) {
			e.printStackTrace();

		}

		this.fileName = fileName;

		// schema = Schema.parse("{\"type\":\"record\",\"name\":\"Person\"," +
		// "\"fields\":[{\"name\": \"ID\", \"type\": \"long\"}," +
		// "{\"name\": \"First\", \"type\": \"string\"},"
		// + " {\"name\": \"Last\", \"type\": \"string\"}," +
		// "{\"name\": \"Phone\", \"type\": \"string\"}," +
		// "{\"name\": \"Age\", \"type\": \"int\"}]}", true);
		Schema.Parser parser = new Schema.Parser();
		String s = "{\"type\":\"record\", " + "\"name\":\"parser_data\", " + "\"fields\":[ " + "{\"name\": \"down_status\", \"type\": \"bytes\"},  "
				+ "{\"name\": \"down_meta\", \"type\": \"bytes\"},  " + "{\"name\": \"down_url\", \"type\": \"bytes\"},  " + "{\"name\": \"down_contenttype\", \"type\": \"bytes\"},  "
				+ "{\"name\": \"down_lang\", \"type\": \"bytes\"},  " + "{\"name\": \"down_encode\", \"type\": \"bytes\"},  " + "{\"name\": \"down_content\", \"type\": \"bytes\"},  "
				+ "{\"name\": \"simhash\", \"type\": \"bytes\"},  " + "{\"name\": \"parser_filetype\", \"type\": \"bytes\"}, " + "{\"name\": \"parser_status\", \"type\": \"bytes\"},  "
				+ "{\"name\": \"parser_pagetype\", \"type\": \"bytes\"},  " + "{\"name\": \"parser_category\", \"type\": \"bytes\"},  " + "{\"name\": \"parser_sitetitle\", \"type\": \"bytes\"},  "
				+ "{\"name\": \"parser_realtitle\", \"type\": \"bytes\"},  " + "{\"name\": \"parser_keyword\", \"type\": \"bytes\"},  " + "{\"name\": \"parser_description\", \"type\": \"bytes\"},  "
				+ "{\"name\": \"parser_time\", \"type\": \"bytes\"},  " + "{\"name\": \"parser_lang\", \"type\": \"bytes\"},  " + "{\"name\": \"parser_content\", \"type\": \"bytes\"},  "
				+ "{\"name\": \"parser_mainbody\", \"type\": \"bytes\"},  " + "{\"name\": \"parser_host\", \"type\": \"bytes\"},  " + "{\"name\": \"parser_link\", \"type\":"
				+ "{\"type\": \"array\", \"items\": {" + "\"name\": \"link_info\"," + "\"type\": \"record\"," + "\"fields\": [" + "{\"name\":\"url\",\"type\":\"bytes\"}, "
				+ "{\"name\":\"source\",\"type\":\"bytes\"}, " + "{\"name\":\"text\",\"type\":\"bytes\"}, " + "{\"name\":\"host\",\"type\":\"bytes\"}, " + "{\"name\":\"inout\",\"type\":\"bytes\"} "
				+ "]" + "}}}," + "{\"name\": \"parser_other\", \"type\": \"bytes\"},  " + "{\"name\": \"seg_url\", \"type\": \"bytes\"},  " + "{\"name\": \"seg_sitetitle\", \"type\": \"bytes\"},  "
				+ "{\"name\": \"seg_realtitle\", \"type\": \"bytes\"},  " + "{\"name\": \"seg_content\", \"type\": \"bytes\"} " + "] " + "}";
		schema = parser.parse(s);
		Map<String, Schema> map = parser.getTypes();
		Field d = schema.getField("parser_link");
		System.out.println(d.schema());
		arraySchema = d.schema();
		arrayReader = new GenericDatumReader<GenericArray>(arraySchema);
		for (Map.Entry<String, Schema> entry : map.entrySet()) {
			System.out.println(entry.getKey());
		}
		System.out.println("schema is " + schema.toString(true));
	}

	String getConent(String name, GenericRecord t) {
		Object o = t.get(name);
		if (o instanceof ByteBuffer) {
			ByteBuffer buf = (ByteBuffer) o;
			try {
				String s = null;

				if (!name.equals("down_content")) {
					s = new String(buf.array(), "gbk");
					System.err.println("name=" + name + " value=" + s + "\t" + buf.array().length);
				} else {
					s = new String(buf.array(),0,buf.remaining());
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

	public void readArray(byte content[]) {
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(content, null);

		GenericArray array = null;
		try {
			array = arrayReader.read(array, decoder);
			int size = array.size();
			// System.out.println("schema =" + arraySchema);
			System.out.println("array size " + size);
			for (int i = 0; i < size; i++) {
				Object o = array.get(i);
				if (o instanceof GenericRecord) {
					ByteBuffer buffer = (ByteBuffer) (((GenericRecord) o).get("url"));
					System.out.println("url\t" + new String(buffer.array(), "gbk"));
					buffer = (ByteBuffer) (((GenericRecord) o).get("text"));
					// System.out.println("title\t" + new
					// String(buffer.array(), "gbk"));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void parse() {

		try {
			// DataFileReader<GenericRecord> dataFileReader = new
			// DataFileReader<GenericRecord>(new File(this.fileName), reader);

			GenericRecord record = null;
			// File file = new File(this.fileName);
			//
			// BufferedInputStream input = new BufferedInputStream(new
			// DataInputStream(new FileInputStream(new File(this.fileName))));
			// int t = 0;
			// int offset = 0;
			//
			// int sum = (int) (file.length());
			// byte a[] = new byte[sum];
			// while ((t = input.read(a, offset, sum - offset)) != -1) {
			// offset += t;
			// if (offset == sum)
			// break;
			// }

			DatumReader<GenericRecord> greader = new GenericDatumReader<GenericRecord>(schema);
			Text key = new Text();
			Text value = new Text();
			BinaryDecoder decoder = null;
			GenericRecord result = null;
			int j = 0;
			DatumWriter<GenericArray> gwriter = null;
			BinaryEncoder encoder = null;
			while (reader.next(key, value)) {
				try {
					System.out.println("url " + key);
					byte a[] = value.getBytes();
					System.out.println("value length " + a.length + " length " + value.getLength());
					if (a.length <= 8)
						continue;
					if (a[0] == '0' || a[0] == '3') {
						System.out.println("donwload or parsed failed,key len=" + key.getBytes().length);
						continue;
					}
					decoder = DecoderFactory.get().binaryDecoder(a, 0, value.getLength(), decoder);
					result = greader.read(result, decoder);

					Object o = result.get("parser_link");
					if (o instanceof GenericArray) {
						GenericArray array = (GenericArray) o;
						if (gwriter == null) {
							gwriter = new GenericDatumWriter<GenericArray>(this.arraySchema);
						}
						ByteArrayOutputStream out = new ByteArrayOutputStream();
						encoder = EncoderFactory.get().binaryEncoder(out, encoder);
						gwriter.write(array, encoder);
						encoder.flush();
						byte arraybyte[] = out.toByteArray();
					//	this.readArray(arraybyte);
						int size = array.size();
						System.out.println("find it size=" + size);
						// for (int i = 0; i < size; i++) {
						// o = array.get(i);
						// if (o instanceof GenericRecord) {
						// ByteBuffer buffer = (ByteBuffer) (((GenericRecord)
						// o).get("url"));
						// // System.out.println("url\t" + new
						// // String(buffer.array(), "gbk"));
						// buffer = (ByteBuffer) (((GenericRecord)
						// o).get("text"));
						// // System.out.println("title\t" + new
						// // String(buffer.array(), "gbk"));
						// }
						// }

					} else {
						System.out.println("array ");
					}
					Schema t = result.getSchema();
					// System.out.println(t.toString(true));
					getConent("down_status", result);
					getConent("down_encode", result);
					// getConent("parser_content", result);
//					getConent("down_contenttype", result);
//					getConent("down_url", result);
//					getConent("parser_filetype", result);
//					getConent("parser_status", result);
//					getConent("parser_pagetype", result);
//					getConent("parser_category", result);
//					getConent("parser_sitetitle", result);
//					getConent("parser_realtitle", result);
//					getConent("parser_keyword", result);
//					getConent("parser_description", result);
//					getConent("simhash", result);
					// getConent("seg_content", result);
					// getConent("down_content", result);

				} catch (Exception e) {
					e.printStackTrace();
				}
				// value = new Text();
				// key = new Text();
				if (j++ > 10)
					break;

			}

			// while (dataFileReader.hasNext()) {
			// record = dataFileReader.next(record);
			// if (record != null)
			// System.out.println(record.get("parser_link"));
			// }

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		AvroTest test = new AvroTest("file:///home/dape/parsed_1/part-00001");
		test.parse();

	}

}
