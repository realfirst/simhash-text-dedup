package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import com.zhongsou.spider.avro.ParserResultConverter;
import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.MD5.MD5Len;
import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.common.util.URLUtil;

public class ExportLinkInfo {
	public static class ScanMapper extends TableMapper<Text, NullWritable> {
		byte[] f = Bytes.toBytes("F");
		byte[] u = Bytes.toBytes("u");
		byte[] h = Bytes.toBytes("h");
		byte[] p = Bytes.toBytes("P");
		byte[] l = Bytes.toBytes("l");
		static Schema recordSchema;
		static Schema arraySchema;
		DatumReader<GenericArray> arrayReader;
		static byte[] EMPTY_DOCID = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
		HashSet<Text> doicText = new HashSet<Text>();
		static {
			Schema.Parser parser = new Schema.Parser();
			try {
				recordSchema = parser.parse(ParserResultConverter.class.getResourceAsStream("resource/parser_data.avro"));
				Field d = recordSchema.getField("parser_link");
				System.out.println(d.schema());
				arraySchema = d.schema();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			arrayReader = new GenericDatumReader<GenericArray>(arraySchema);
		}

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			byte row[] = key.get();
			assert (row.length == 8);
			byte host[] = null;

			byte url[] = value.getValue(f, u);
			if (url == null) {
				MD5 md5 = null;
				try {
					md5 = new MD5(row, MD5Len.eight, 0);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				context.getCounter("parse_link", "empty_url").increment(1);
				Log.info("row key=" + md5.getHexString(true) + " url is null");
				return;
			}
			String surl = new String(url);
			try {
				host = URLUtil.getDomainName(surl).getBytes();
			} catch (Exception e) {
				e.printStackTrace();
				MD5 md5 = null;
				try {
					md5 = new MD5(row, MD5Len.eight, 0);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				context.getCounter("parse_link", "error_url").increment(1);
				Log.info("row key=" + md5.getHexString() + "\turl=" + surl + " end");
				return;
			}

			if (host == null) {
				context.getCounter("parse_link", "error_host").increment(1);
				return;
			}
			byte link[] = value.getValue(p, l);
			byte srcSiteId[] = MD5.digest8(host).getDigest();
			if (link != null) {
				context.getCounter("parse_link", "have_link").increment(1);
				this.doicText.clear();
				BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(link, null);
				GenericArray array = null;
				try {
					array = arrayReader.read(array, decoder);
					int size = array.size();
					// System.out.println("schema =" + arraySchema);
					// StringBuffer strbuffer = new
					// StringBuffer("<link_info>\r\n");
					for (int i = 0; i < size; i++) {
						context.getCounter("parse_link", "anchor_num").increment(1);
						Object o = array.get(i);
						if (o instanceof GenericRecord) {
							ByteBuffer buffer = (ByteBuffer) (((GenericRecord) o).get("url"));
							byte[] desturl = buffer.array();

							byte destURLDocid[] = MD5.digest8(desturl).getDigest();
							buffer = (ByteBuffer) (((GenericRecord) o).get("host"));

							byte[] desthost = buffer.array();
							byte[] destHostId = MD5.digest8(desthost).getDigest();

							buffer = (ByteBuffer) (((GenericRecord) o).get("text"));
							byte[] archorText = buffer.array();
							// Log.info("archor " + NumberUtil.getHexString(row)
							// + "\toffset=" + buffer.arrayOffset() +
							// "\tremain=" + buffer.remaining() + "\tlen=" +
							// buffer.capacity() + "\tarch="
							// + new String(archorText, "gbk"));
							buffer = (ByteBuffer) (((GenericRecord) o).get("source"));
							byte[] source = buffer.array();

							if (source == null || source.length > 1 || source[0] > 9 || source[0] < 0)
								continue;

							int sumLen = 2 + archorText.length + 1 + 8 * 4;
							if (archorText.length > 32768) {
								Log.info("error archor len" + archorText.length);
								Log.info("archor " + NumberUtil.getHexString(row) + "\toffset=" + buffer.arrayOffset() + "\tremain=" + buffer.remaining() + "\tlen=" + buffer.capacity() + "\tarch="
										+ new String(archorText, "gbk"));
								context.getCounter("spider", "error_archor").increment(1);
//								continue;
							}
							byte res[] = new byte[sumLen];
							int offset = 0;
							byte archLen[] = NumberUtil.convertIntToShortC(archorText.length);
							System.arraycopy(archLen, 0, res, 0, 2);
							offset += 2;
							System.arraycopy(archorText, 0, res, 2, archorText.length);
							offset += archorText.length;
							res[offset] = (byte) ((byte) 127 - (byte) (source[0]));
							offset += 1;
							System.arraycopy(row, 0, res, offset, 8);// docid
							offset += 8;
							System.arraycopy(srcSiteId, 0, res, offset, 8);
							offset += 8;
							System.arraycopy(destURLDocid, 0, res, offset, 8);
							offset += 8;
							System.arraycopy(destHostId, 0, res, offset, 8);
							Text k2 = new Text(res);
							if (!this.doicText.contains(k2)) {
								context.write(k2, NullWritable.get());
								context.getCounter("parse_link", "link").increment(1);
								this.doicText.add(k2);
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			} else {
				int sumLen = 2 + 1 + 8 * 4;
				byte res[] = new byte[sumLen];
				int offset = 0;
				byte archLen[] = NumberUtil.convertIntToShortC(0);
				System.arraycopy(archLen, 0, res, 0, 2);
				offset += 2;
				res[offset] = 0;
				offset += 1;
				System.arraycopy(row, 0, res, offset, 8);// docid
				offset += 8;
				System.arraycopy(srcSiteId, 0, res, offset, 8);
				offset += 8;
				System.arraycopy(EMPTY_DOCID, 0, res, offset, 8);
				offset += 8;
				System.arraycopy(EMPTY_DOCID, 0, res, offset, 8);
				Text k2 = new Text(res);
				context.write(k2, NullWritable.get());

				context.getCounter("parse_link", "empty_link").increment(1);
			}
		}

	}

	public static class ScanReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
		}

	}

	static Job createSubmitJob(Configuration conf, String args[]) {
		try {
			conf.setBoolean("mapred.compress.map.output", true);
			conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
			conf.set("mapred.output.compression.type", CompressionType.BLOCK.toString());
			conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
			Job job = new Job(conf);
			Scan scan = new Scan();
			String tableName = conf.get("tableName", "webDB");
			scan.setCaching(500);
			scan.addColumn(Bytes.toBytes("P"), Bytes.toBytes("h"));
			scan.addColumn(Bytes.toBytes("P"), Bytes.toBytes("l"));
			scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("u"));
			if (conf.get("scan_filter") != null) {
				Filter filter;
				try {
					ParseFilter parse = new ParseFilter();
					filter = parse.parseFilterString(conf.get("scan_filter"));
					System.out.println("set filter " + conf.get("scan_filter"));
					scan.setFilter(filter);
				} catch (CharacterCodingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			scan.setMaxVersions(1);
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
			TableMapReduceUtil.initTableMapperJob(tableName, scan, ScanMapper.class, ImmutableBytesWritable.class, NullWritable.class, job);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			conf.setBoolean("mapred.output.compress", true);
			conf.setBoolean("mapred.compress.map.output", true);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			// job.setReducerClass(ScanReducer.class);
			job.setNumReduceTasks(0);
			return job;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String args[]) {
		// Bytes.equals(left, leftOffset, leftLen, right, rightOffset, rightLen)
		Configuration conf = HBaseConfiguration.create();
		try {
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			Job job = ExportLinkInfo.createSubmitJob(conf, otherArgs);
			if (otherArgs.length < 1) {
				System.out.println("Wrong number of arguments: " + otherArgs.length + " usage: outputpath SingleColumnValueFilter('F','s',=,'binary:5',true,true)");
				System.exit(-1);
			}
			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
