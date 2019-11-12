package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zhongsou.spider.bean.URLInfo;
import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;

/**
 * 
 * 
 * url排重，从本地读取一份原始的urldocid文件 与分析出来的一份urlinfo的文件进行合并， 找出其中的新的url
 * 
 * <pre>
 *  输出３种文件: <br>
 * 1 key new url md5(docid) value NullWritable 供继续排重<br>
 * 2 key new url docid,value url 供insert hbase 之中<br>
 * 3 key src url docid value NullWritable 供统计每一个网页的新的url数目的job使用，以用于爬虫的下载调度<br>
 * </pre>
 * 
 * @author Xi Qi
 * 
 */
public class DuplicateURL extends Configured implements MapreduceV2Job {

	/**
	 * 
	 * 装入全部的docid,value为空的URLInfo
	 * 多加一个字节0,以表相同的docid分组时排在前面，从而判断后面的docid是否为新url,新的url的key的docid的最后一个字节为1
	 * 
	 * @author Xi Qi
	 * 
	 */
	static class URLDocidMapper extends Mapper<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, URLInfo> {
		ImmutableBytesWritable k2 = new ImmutableBytesWritable();
		byte b[] = new byte[9];
		static URLInfo nullinfo = new URLInfo();
		static {
			nullinfo.setSrcURLmd5(null);
			nullinfo.setUrl(null);
			nullinfo.setUrlmd5(null);
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

		@Override
		protected void map(ImmutableBytesWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.arraycopy(key.get(), 0, b, 0, 8);
			b[8] = '0';
			k2.set(b);
			context.write(k2, nullinfo);
		}

	}

	/**
	 * 
	 * 新的url的信息 多加一个字节，最后一个字节为1,以便于区分新旧的url
	 * 
	 * @author Xi Qi
	 * 
	 */
	static class NewURLInfoMapper extends Mapper<ImmutableBytesWritable, URLInfo, ImmutableBytesWritable, URLInfo> {
		ImmutableBytesWritable k2 = new ImmutableBytesWritable();
		byte b[] = new byte[9];

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

		@Override
		protected void map(ImmutableBytesWritable key, URLInfo value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.arraycopy(value.getUrlmd5(), 0, b, 0, 8);
			b[8] = '1';
			k2.set(b);
			context.write(k2, value);
			context.getCounter("newurl", "input_url").increment(1);
		}

	}

	/**
	 * 
	 * 排重，接收的第一个数据，如果o.getUrl为null，则代表此url已经存在，直接返回 <br>
	 * 否则，代表这个url是一个新的url,输出3个文件:<br>
	 * 1 new url docid value 为空，加入到现在的文件之中<br>
	 * 2 新的url docid 和url 3 最后输出这个url来自于那个源文件的url的docid
	 * 有可能这个新url，这一次来自多个不同的网页，这些源网页的docid都输出一次
	 * 
	 */
	static class NewURLInfoReducer extends Reducer<ImmutableBytesWritable, URLInfo, ImmutableBytesWritable, ImmutableBytesWritable> {
		private MultipleOutputs mos;
		private String baseName;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			mos = new MultipleOutputs(context);
			k3 = new ImmutableBytesWritable();
			baseName = context.getConfiguration().get("new_docid_prefix");
			System.out.println("baseName=" + baseName);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			mos.close();
		}

		ImmutableBytesWritable k = new ImmutableBytesWritable();
		ImmutableBytesWritable k3;

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<URLInfo> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Iterator<URLInfo> e = values.iterator();
			boolean flag = true;

			byte a[] = new byte[8];
			System.arraycopy(key.get(), 0, a, 0, 8);
			k.set(a);
			while (e.hasNext()) {
				URLInfo o = e.next();
				// 这md5已经存在
				if (flag == true) {
					if (o.getUrl() == null) {
						return;
					} else {
						flag = false;
						// new url docid +url
						context.write(k, new ImmutableBytesWritable(o.getUrl()));

						context.getCounter("newurl", "url").increment(1);
						// new url docid
						mos.write(baseName, k, NullWritable.get());
					}
				}
				context.getCounter("newurl", "srcurl").increment(1);
				k3.set(o.getSrcURLmd5(), 0, 8);
				// 有新url的源网页的docid
				mos.write("srcurldocid", k3, NullWritable.get());
				// 此url是新url

			}
		}

	}

	static class URLInfoGroupComparator implements RawComparator<ImmutableBytesWritable> {

		@Override
		public int compare(ImmutableBytesWritable o1, ImmutableBytesWritable o2) {
			return Bytes.compareTo(o1.get(), 0, 8, o2.get(), 0, 8);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			return Bytes.compareTo(b1, s1 + 4, 8, b2, s2 + 4, 8);
		}

	}

	static class PartitionURL extends Partitioner<ImmutableBytesWritable, URLInfo> {
		static Hash hash = Hash.getInstance(Hash.JENKINS_HASH);

		@Override
		public int getPartition(ImmutableBytesWritable key, URLInfo value, int numPartitions) {
			// TODO Auto-generated method stub
			int t = Math.abs(hash.hash(key.get(), 0, 8, 0));
			return t % numPartitions;
		}

	}

	static Job createJob(Configuration conf, String[] args) {
		Job job;
		try {
			job = new Job(conf);
			// 旧的url docid
			MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class, URLDocidMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, NewURLInfoMapper.class);
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			int jobnum = Integer.parseInt(args[3]);
			job.setNumReduceTasks(jobnum);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(URLInfo.class);
			job.setGroupingComparatorClass(URLInfoGroupComparator.class);
			job.setReducerClass(NewURLInfoReducer.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setPartitionerClass(PartitionURL.class);
			job.setOutputValueClass(ImmutableBytesWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			DateFormat format = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
			String baseName = "newdocid" + format.format(new java.util.Date());
			job.getConfiguration().set("new_docid_prefix", baseName);
			MultipleOutputs.addNamedOutput(job, "srcurldocid", SequenceFileOutputFormat.class, ImmutableBytesWritable.class, NullWritable.class);
			MultipleOutputs.addNamedOutput(job, baseName, SequenceFileOutputFormat.class, ImmutableBytesWritable.class, NullWritable.class);
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
			if (otherArgs.length < 4) {
				System.out.println("Wrong number of arguments: " + otherArgs.length + " args: urldocid newurlinfo outputpath number_of_reduce");
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
			if (otherArgs.length < 4) {
				System.out.println("Wrong number of arguments: " + otherArgs.length + " args: urldocid newurlinfo outputpath number_of_reduce");
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
