package com.zhongsou.spider.hadoop.job;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

import com.zhongsou.spider.avro.ParserResultConverter;
import com.zhongsou.spider.bean.ParserResult;
import com.zhongsou.spider.bean.URLInfo;
import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.MD5.MD5Len;
import com.zhongsou.spider.hbase.HFileOutputFormat;

public class ParseResultConverterJob extends Configured implements Tool {

	public static class ReduceClass<K, V> implements Reducer<BytesWritable, BytesWritable, K, V> {
		private MultipleOutputs mos;
		private OutputCollector collector;
		private ParserResultConverter parseConverter;
		ImmutableBytesWritable hbaseKey = new ImmutableBytesWritable();
		LinkedList<KeyValue> kvList = new LinkedList<KeyValue>();
		int timestamp;
		static DateFormat dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss");

		public void configure(JobConf conf) {
			mos = new MultipleOutputs(conf);
			
			String seqtime = conf.get("seqTimestamp");
			
			if (seqtime == null || seqtime.equals("")) {
				Log.info("seq time is null" + seqtime);
			}
			try {
				Date d = dateFormat.parse(seqtime);
				this.timestamp = (int) (d.getTime() / 1000);
				parseConverter = new ParserResultConverter(timestamp);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void reduce(BytesWritable key, Iterator<BytesWritable> values, OutputCollector<K, V> output, Reporter reporter) throws IOException {
			// TODO Auto-generated method stub

			// 以前我们并不使用这个方法，但在使用MultipleOutputs时就要记得使用configure()方法
			// 将配置写到MultipleOutputs中
			ImmutableBytesWritable k = new ImmutableBytesWritable();
			k.set(key.getBytes());
			Text k3 = new Text();
			k3.set(key.getBytes());
			while (values.hasNext()) {
				BytesWritable value = values.next();
				hbaseKey.set(key.getBytes());
				kvList.clear();
				byte row[] = new byte[key.getLength()];
				System.arraycopy(key.getBytes(), 0, row, 0, key.getLength());
				ParserResult result = parseConverter.converter(row, key.getLength(), value.getBytes(), value.getLength(), kvList);
				if (result != null) {
					if (result.getFinger() != null && result.getFinger().length == 8) {
						Text finger = new Text();
						finger.set(result.getFinger());
						collector = mos.getCollector("urlAndfinger", reporter); // 设置输出文件名
						collector.collect(k3, finger);
					}
					Set<URLInfo> infoset = result.getInfoSet();
					if (infoset != null && infoset.size() > 0) {
						for (URLInfo info : infoset) {
							collector = mos.getCollector("urlinfo", reporter); // 设置输出文件名
							collector.collect(k3, info);
						}
					}
					if (kvList.size() > 0) {
						try {
							// 在插入hbase之前要进行kv的排序
							Collections.sort(kvList, KeyValue.COMPARATOR);
							for (KeyValue kv : kvList) {
								if (key.getLength() == 8) {
									collector = mos.getCollector("hfiledata", reporter);
									try {
										collector.collect(k, kv);
									} catch (Exception e) {
										e.printStackTrace();
										MD5 md5 = new MD5(kv.getRow(), MD5Len.eight, 0);
										Log.info("kv=" + md5.getHexString() + "\tsring binary=" + Bytes.toStringBinary(kv.getRow()));
									}
								} else {
									Log.info("error key length " + key);
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						System.out.println("kv list size =" + kvList.size());
					}

				}
			}
		}

		public void close() throws IOException {
			mos.close();// 调用父类MultipleOutputs的方法
		}
	}

	public static class MapClass extends MapReduceBase implements Mapper<Text, Text, BytesWritable, BytesWritable> {
		BytesWritable k2 = new BytesWritable();
		BytesWritable v2 = new BytesWritable();

		@Override
		public void map(Text key, Text value, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			k2.set(key.getBytes(), 0, key.getLength());

			v2.set(value.getBytes(), 0, value.getLength());
			output.collect(k2, v2);
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, ParseResultConverterJob.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJobName("ParseResultConverter");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);

		MultipleOutputs.addNamedOutput(job, "urlinfo", SequenceFileOutputFormat.class, Text.class, URLInfo.class); // 此处的文件名必须与getCollector设置的文件名相同
		MultipleOutputs.addNamedOutput(job, "urlAndfinger", SequenceFileOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "hfiledata", HFileOutputFormat.class, ImmutableBytesWritable.class, KeyValue.class);

		RunningJob rjob = JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(HBaseConfiguration.create(), new ParseResultConverterJob(), args);
		System.exit(res);
	}

}
