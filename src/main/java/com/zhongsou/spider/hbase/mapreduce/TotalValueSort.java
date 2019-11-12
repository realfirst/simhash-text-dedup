package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.InputSampler;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.InputSampler.Sampler;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;

public class TotalValueSort extends Configured implements Tool, MapreduceV2Job {

	public TotalValueSort() {
		this(new Configuration());
	}

	public TotalValueSort(Configuration conf) {
		this.setConf(conf);
	}

	static class TotalValueMapper extends
			Mapper<FloatWritable, Text, FloatWritable, Text> {
		Configuration conf;
		public static final String DEFAULT_PATH = "_partition.lst";
		public static final String PARTITIONER_PATH = "mapreduce.totalorderpartitioner.path";
		RawComparator<FloatWritable> comparator;
		FloatWritable[] splitPoints;
		// int mapIndexNum;
		int oldRecordIndex;
		int newRecordIndex;
		int partitions;

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void map(FloatWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// super.map(key, value, context);
			int pos = Arrays.binarySearch(splitPoints, key, comparator) + 1;
			pos = pos < 0 ? -pos : pos;
			// System.out.println("pos=" + pos);
			if (pos <= this.oldRecordIndex || pos >= this.newRecordIndex) {
				int urlLen = NumberUtil.readInt(value.getBytes(), 44);
				String url = new String(value.getBytes(), 48, urlLen);
				System.out.println("pos=" + pos + " url=" + url + " score="
						+ key);
				context.write(key, value);
			} else {
				// System.out.println("counter=" + pos);
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			this.conf = context.getConfiguration();
			String parts = conf.get(PARTITIONER_PATH, DEFAULT_PATH);
			final Path partFile = new Path(parts);
			final FileSystem fs = (DEFAULT_PATH.equals(parts)) ? FileSystem
					.getLocal(conf) // assume
									// in
									// DistributedCache
					: partFile.getFileSystem(conf);

			Job job = new Job(conf);
			this.partitions = conf.getInt("total.sort.pre.partition",
					job.getNumReduceTasks());
			this.oldRecordIndex = conf.getInt("old.record.index",
					job.getNumReduceTasks() - 1);
			this.newRecordIndex = conf.getInt("new.record.index",
					this.partitions);
			splitPoints = readPartitions(fs, partFile, FloatWritable.class,
					conf);
			System.out.println("splitPoints.length" + splitPoints.length
					+ " old.record.index=" + oldRecordIndex
					+ "new.record.index" + newRecordIndex + "reduce="
					+ job.getNumReduceTasks());


			System.out.println("split points length=" + splitPoints.length);
			comparator = (RawComparator<FloatWritable>) job.getSortComparator();
			for (int i = 0; i < splitPoints.length - 1 && i < this.partitions; ++i) {
				if (comparator.compare(splitPoints[i], splitPoints[i + 1]) > 0) {
					throw new IOException("Split points are out of order");
				}
			}

		}

		private FloatWritable[] readPartitions(FileSystem fs, Path p,
				Class keyClass, Configuration conf) throws IOException {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
			ArrayList<FloatWritable> parts = new ArrayList<FloatWritable>();
			FloatWritable key = (FloatWritable) ReflectionUtils.newInstance(
					keyClass, conf);
			NullWritable value = NullWritable.get();
			while (reader.next(key, value)) {
				parts.add(key);
				key = (FloatWritable) ReflectionUtils.newInstance(keyClass,
						conf);
			}
			reader.close();
			return parts.toArray((FloatWritable[]) Array.newInstance(keyClass,
					parts.size()));
		}

	}

	static class TotalValueReducer extends
			Reducer<FloatWritable, Text, ImmutableBytesWritable, Text> {

		byte a[] = new byte[12];
		ImmutableBytesWritable k3 = new ImmutableBytesWritable();

		@Override
		protected void reduce(FloatWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			byte score[] = Bytes.toBytes(key.get());
			int urlLen = 0;
			System.arraycopy(score, 0, a, 8, 4);
			for (Text o : values) {
				byte bytes[] = o.getBytes();
				urlLen = NumberUtil.readInt(bytes, 44);
				byte docid[] = MD5.digest8(bytes, 48, urlLen).getDigest();
				System.arraycopy(docid, 0, a, 0, 8);
				k3.set(a);
				context.write(k3, o);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// GenericOptionsParser parser = new GenericOptionsParser(getConf(),
		// args);

		Job job = this.createRunnableJob(args);
		if (job != null) {
			return job.waitForCompletion(true) ? 0 : 1;
		} else
			return 1;
	}

	public static <K, V> boolean writePartitionFile(Job job,
			Sampler<K, V> sampler) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = job.getConfiguration();
		final InputFormat inf = ReflectionUtils.newInstance(
				job.getInputFormatClass(), conf);
		int numPartitions = job.getConfiguration().getInt(
				"total.sort.pre.partition", job.getNumReduceTasks());
		K[] samples = sampler.getSample(inf, job);
		System.out.println("partitions length=" + numPartitions
				+ " samples length =" + samples.length);
		RawComparator<K> comparator = (RawComparator<K>) job
				.getSortComparator();
		Arrays.sort(samples, comparator);

		// for (K k : samples) {
		// System.out.println("k=" + k);
		// }
		Path dst = new Path(TotalOrderPartitioner.getPartitionFile(conf));
		FileSystem fs = dst.getFileSystem(conf);
		if (fs.exists(dst)) {
			fs.delete(dst, false);
		}
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dst,
				job.getMapOutputKeyClass(), NullWritable.class);
		NullWritable nullValue = NullWritable.get();
		float stepSize = samples.length / (float) numPartitions;
		int last = -1;
		int i = 1;
		int m = 0;
		for (; i < numPartitions; ++i) {
			int k = Math.round(stepSize * i);
			while (last >= k
					&& comparator.compare(samples[last], samples[k]) == 0) {
				++k;
			}
			if (k < samples.length) {
				m++;
				writer.append(samples[k], nullValue);
				last = k;
			} else {
				System.out.println("sample failed");
				break;
			}
		}
		writer.close();
		System.out.println("write sample finished,m=" + m + "\ti=" + i
				+ " num partitions=" + numPartitions);
		if (m == numPartitions - 1) {
			return true;
		} else {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	public static void main(String args[]) throws Exception {
		int exitCode = ToolRunner.run(new TotalValueSort(), args);
		System.exit(exitCode);

		// String m =
		// "-libjars  /home/dape/workspace_java/spider_common/target/spider_common-0.0.1-SNAPSHOT.jar  -Dmapred.output.key.comparator.class=com.zhongsou.spider.hbase.mapreduce.FloatReverseComparator -Dmapreduce.outputformat.class=org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat -Dmapred.job.name=select_url -Dmapred.reduce.tasks=10 -DoldNewRatio=0.5 /spider_data/score_output/20121116202450 /spider_data/select_url_output/20121116202450  4030388 1000";
		// String[] otherArgs = new GenericOptionsParser(new Configuration(),
		// m.split(" {1,}")).getRemainingArgs();
		// System.out.println("other args length=" + otherArgs.length);

	}

	@Override
	public Job createRunnableJob(String[] args) {
		// TODO Auto-generated method stub
		try {
			String[] otherArgs = new GenericOptionsParser(getConf(), args)
					.getRemainingArgs();
			if (otherArgs.length < 4) {
				System.out.println("Wrong number of arguments: "
						+ otherArgs.length + " input ");
				System.exit(-1);
			}
			/*
			 * Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
			 * if (job == null) { return -1; }
			 */
			System.out.println("create running job args length=" + otherArgs);

			Job job = new Job(getConf());
			int sumRecord = 0, fetchRecord = 0;
			float oldURLRation = 0.5f;
			if (otherArgs.length >= 3) {
				sumRecord = Integer.parseInt(otherArgs[2]);
			}

			if (otherArgs.length >= 4) {
				fetchRecord = Integer.parseInt(otherArgs[3]);
			}
			oldURLRation = job.getConfiguration().getFloat("oldNewRatio", 0.0f);
			int partitions = 0, oldRecordIndex = job.getNumReduceTasks() - 1, newRecordIndex = -1;
			if (sumRecord == 0 || fetchRecord == 0) {
				partitions = job.getNumReduceTasks();
			} else {
				if (fetchRecord > sumRecord) {
					partitions = job.getNumReduceTasks();
					oldRecordIndex = job.getNumReduceTasks() - 1;
				} else {
					int t = job.getNumReduceTasks();
					int oldPart = (int) (t * oldURLRation);
					int newPart = t - oldPart;

					int part = fetchRecord / t;
					int allPart = sumRecord / part;
					if (sumRecord % part > (part / 2)) {
						allPart++;
					}
					partitions = allPart;
					if (oldPart != 0) {
						oldRecordIndex = oldPart - 1;
					} else {
						oldRecordIndex = -1;
					}
					if (newPart != 0) {
						newRecordIndex = allPart - newPart;
					} else {
						newRecordIndex = allPart+1 ;
					}
				}
			}

			int sampleLen = sumRecord / 100;
			if (sampleLen < 10000 && sumRecord / 2 > 10000) {
				sampleLen = 10000;
			}
			if (sampleLen > 100000) {
				sampleLen = 50000;
			}
			System.out.println("total.sort.pre.partition=" + partitions
					+ "\told.record.index=" + oldRecordIndex
					+ "\tnew.record.index=" + newRecordIndex);
			// getConf().set("total.sort.pre.partition",
			// String.valueOf(partitions));

			job.getConfiguration().setInt("old.record.index", oldRecordIndex);
			job.getConfiguration().setInt("new.record.index", newRecordIndex);
			job.getConfiguration().setInt("total.sort.pre.partition",
					partitions);
			// int numreduce =
			// job.getConfiguration().getInt("mapred.reduce.tasks",
			// 6);
			FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
			// job.getConfiguration().set("mapred.output.key.comparator.class",
			// "com.zhongsou.spider.hbase.mapreduce.TotalValueSort.FloatReverseComparator");
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputKeyClass(FloatWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputValueClass(Text.class);
			// job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapperClass(TotalValueMapper.class);
			job.setPartitionerClass(TopKOrderPartition.class);
			job.setSortComparatorClass(FloatReverseComparator.class);
			// SequenceFileOutputFormat.setCompressOutput(job, true);
			// SequenceFileOutputFormat.setOutputCompressorClass(job,
			// GzipCodec.class);
			// SequenceFileOutputFormat.setOutputCompressionType(job,
			// CompressionType.BLOCK);
			job.setReducerClass(TotalValueReducer.class);
			InputSampler.Sampler<FloatWritable, Text> sampler = new InputSampler.RandomSampler<FloatWritable, Text>(
					0.1, sampleLen, 10);
			Path input = FileInputFormat.getInputPaths(job)[0];
			input = input.makeQualified(input.getFileSystem(getConf()));
			Path partitionFile = new Path(input, "_partitions");
			FileSystem fs = partitionFile.getFileSystem(job.getConfiguration());
			TopKOrderPartition.setPartitionFile(job.getConfiguration(),
					partitionFile);
			int t = 0;
			do {
				boolean m = writePartitionFile(job, sampler);
				if (m)
					break;
				else {
					t++;
					System.out.println("generate partition file failed!" + t
							+ " try again");
				}
			} while (t < 10);
			if (t == 10) {
				System.out.println("generate partition file failed!");
				System.exit(0);
			}
			URI partitionUri = new URI(partitionFile.toString()
					+ "#_partitions");
			DistributedCache.addCacheFile(partitionUri, getConf());
			DistributedCache.createSymlink(getConf());
			fs.deleteOnExit(partitionFile);
			return job;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
