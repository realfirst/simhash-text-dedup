package com.zhongsou.incload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.zhongsou.spider.hadoop.jobcontrol.MapreduceV2Job;

/**
 * Describe class <code>SelectLogic</code> here. the dirver class for
 * selectlogic mapreduce
 *
 * @author <a href="mailto:dingje@zhongsou.com">Ding Jingen</a>
 * @version 1.0
 */
public class SelectLogic extends Configured implements Tool, MapreduceV2Job {

  public Job createJob(String args[]) throws IOException {
    Configuration conf = this.getConf();
    Job job = new Job(conf);
    job.setJarByClass(SelectLogic.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(SelectLogicMapper.class);
    job.setReducerClass(SelectLogicReducer.class);
    job.setMapOutputKeyClass(Triple.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(ImmutableBytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    // job.setPartitionerClass(urlidPartitioner.class);
    job.setSortComparatorClass(TripleComparator.class);
    // job.setGroupingComparatorClass(GroupComparator.class);
    job.setNumReduceTasks(1);

    Path inDir = new Path(args[0]);
    Path outDir = new Path(args[1]);

    FileInputFormat.addInputPath(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);

    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(ImmutableBytesWritable.class);

    MultipleOutputs.addNamedOutput(job, "modifyingList", SequenceFileOutputFormat.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class);
    MultipleOutputs.setCountersEnabled(job, true);
    return job;
  }

  public int run(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs;
    try {
      otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length < 2) {
        System.out.println("Wrong number of arguments: " + otherArgs.length + " args:  args: select-input select-output");
        System.exit(-1);
      }
      this.setConf(conf);
      Job job = createJob(otherArgs);
      return job.waitForCompletion(true) ? 0 : -1;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return -1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new SelectLogic(), args));
  }

  public static class urlidPartitioner extends Partitioner<Triple, NullWritable> {

    @Override
      public int getPartition(Triple key, NullWritable value, int numPartitions) {
      return key.getKey().hashCode() % numPartitions;
    }
  }

  public static class TripleComparator extends WritableComparator {

    protected TripleComparator() {
      super(Triple.class, true);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      float a = WritableComparator.readFloat(b1, s1 + 12);
      float b = WritableComparator.readFloat(b2, s2 + 12);
      if (a > b) {
        return -1;
      } else if (a < b) {
        return 1;
      }

      int cmp;

      cmp = WritableComparator.compareBytes(b1, s1 + 17, 8, b2, s2 + 17, 8);
      if (cmp != 0) {
        return cmp;
      }

      return WritableComparator.compareBytes(b1, s1 + 40, 8, b2, s2 + 40, 8);

    }
  }

  /**
   * Describe class <code>GroupComparator</code> here. make recoeds in one
   * reduce group by urlid not by fourtuple
   */
  public static class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(Triple.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
      return 0;
      // Triple t1 = (Triple) w1;
      // Triple t2 = (Triple) w2;
      // return t1.getKey().compareTo(t2.getKey());
    }
  }

  @Override
  public Job createRunnableJob(String[] args) {
    // TODO Auto-generated method stub
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs;
    try {
      otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length < 2) {
        System.out.println("Wrong number of arguments: " + otherArgs.length + " args: select-input select-output");
        System.exit(-1);
      }
      this.setConf(conf);
      Job job = createJob(otherArgs);
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
