package com.zhongsou.spider.hbase.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.common.util.URLUtil;
import com.zhongsou.spider.hadoop.ScoreURLInfo;
import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;

public class ScoreURLStatistic extends Configured implements MapreduceV2Job {

	static class ScoreURLMapper extends
			Mapper<FloatWritable, Text, Text, ScoreURLInfo> {
		HashSet<String> blackDomain=new HashSet<String>();
		HashSet<String> blackHost=new HashSet<String>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stubc
			Path cachesFiles[] = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path path : cachesFiles) {
				Log.info("get a local caches file path" + path.toString());
				String t = path.toString();
				if (t.contains("blackdomain.txt")) {
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(new FileInputStream(new File(
									t))));
					String line = null;
					while ((line = reader.readLine()) != null) {
						String reHost = URLUtil.hostReverse(line.trim());
						this.blackDomain.add(reHost);
					}
					reader.close();
				} else if (t.contains("blackhost.txt")) {
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(new FileInputStream(new File(
									t))));
					String line = null;
					while ((line = reader.readLine()) != null) {
						String reHost = URLUtil.hostReverse(line.trim());
						this.blackHost.add(reHost);
					}
					reader.close();
				}

			}
			Log.info("load black doamin size="+this.blackDomain.size()+"\tblack host size="+this.blackHost.size());

		}

		@Override
		protected void map(FloatWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			byte bytes[] = value.getBytes();
			int urlLen = NumberUtil.readInt(bytes, 44);
			String urlStr = new String(bytes, 48, urlLen);
			try {
				URL url = new URL(urlStr);
				String host = url.getHost();
				String reHost = URLUtil.hostReverse(host);

				String domain = URLUtil.getDomainByHost(host);
				String redomain = URLUtil.hostReverse(domain);
				if (this.blackDomain.contains(redomain)) {
					ScoreURLInfo info = new ScoreURLInfo();
					info.setAllNum(new IntWritable(1));
					if (key.get() < 0) {
						info.setNewNum(new IntWritable(1));
					} else {
						info.setNewNum(new IntWritable(0));
					}
					context.write(new Text(domain), info);
					context.getCounter("urlCounter", "blackDomain").increment(1);
				} else {
					String reLevelOneHost = reHost;
					if (reHost.length() > redomain.length()) {
						int dot = reHost.indexOf(".", redomain.length()+2);
						if (dot != -1) {
							reLevelOneHost = reHost.substring(0, dot);
						}

					}

					if (this.blackHost.contains(reLevelOneHost)) {
						String levelOneHost = URLUtil
								.hostReverse(reLevelOneHost);
						ScoreURLInfo info = new ScoreURLInfo();
						info.setAllNum(new IntWritable(1));
						if (key.get() < 0) {
							info.setNewNum(new IntWritable(1));
						} else {
							info.setNewNum(new IntWritable(0));
						}
						context.getCounter("urlCounter", "blackHost").increment(1);
						context.write(new Text(levelOneHost), info);
					} else {
						ScoreURLInfo info = new ScoreURLInfo();
						info.setAllNum(new IntWritable(1));
						if (key.get() < 0) {
							info.setNewNum(new IntWritable(1));
						} else {
							info.setNewNum(new IntWritable(0));
						}
						context.write(new Text(host), info);
						context.getCounter("urlCounter", "normalURL").increment(1);
					}

				}
				
				

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	static class ScoreURLReducer extends
			Reducer<Text, ScoreURLInfo, Text, Text> {
		private List<Pair<String, ScoreURLInfo>> tlist = new ArrayList<Pair<String, ScoreURLInfo>>(1000000);

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);

			Collections.sort(tlist,
					new Comparator<Pair<String, ScoreURLInfo>>() {

						@Override
						public int compare(Pair<String, ScoreURLInfo> arg0,
								Pair<String, ScoreURLInfo> arg1) {
							// TODO Auto-generated method stub
							return arg0.getSecond().getAllNum()
									.compareTo(arg1.getSecond().getAllNum());
						}
					});

			for (Pair<String, ScoreURLInfo> p : tlist) {
				ScoreURLInfo info = p.getSecond();
				String t = info.getAllNum().get() + "\t"
						+ info.getNewNum().get() + "\t"
						+ (info.getAllNum().get() - info.getNewNum().get());
				context.write(new Text(p.getFirst()), new Text(t));
			}

		}

		@Override
		protected void reduce(Text key, Iterable<ScoreURLInfo> values,
				Context context) throws IOException, InterruptedException {
			int allNum = 0, oldNum = 0, newNum = 0;
			for (ScoreURLInfo info : values) {
				allNum += info.getAllNum().get();
				newNum += info.getNewNum().get();
			}
			oldNum = allNum - newNum;

			ScoreURLInfo info = new ScoreURLInfo();
			info.setAllNum(new IntWritable(allNum));
			info.setNewNum(new IntWritable(newNum));
			this.tlist
					.add(new Pair<String, ScoreURLInfo>(key.toString(), info));

		}
	}

	static Job createJob(Configuration conf, String[] args) {
		Job job;
		try {
			job = new Job(conf);
			// 旧的url docid
			MultipleInputs.addInputPath(job, new Path(args[0]),
					SequenceFileInputFormat.class, ScoreURLMapper.class);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(ScoreURLInfo.class);
			job.setReducerClass(ScoreURLReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			return job;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	public static void main(String args[]) {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.out
						.println("Wrong number of arguments: "
								+ otherArgs.length
								+ " args: urldocid newurlinfo outputpath number_of_reduce");
				System.exit(-1);
			}
			Job job = createJob(conf, otherArgs);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Job createRunnableJob(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		this.setConf(conf);
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.out
						.println("Wrong number of arguments: "
								+ otherArgs.length
								+ " args: urldocid newurlinfo outputpath number_of_reduce");
				System.exit(-1);
			}
			Job job = createJob(conf, otherArgs);
			return job;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

}
