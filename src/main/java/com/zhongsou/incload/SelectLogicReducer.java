package com.zhongsou.incload;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.mortbay.log.Log;


/**
 * Describe class <code>SelectLogicReducer</code> here.
 *
 * @author Ding Jingen
>/Ding Jingen/g
 * @version 1.0
 */
public class SelectLogicReducer
    extends Reducer<Triple, NullWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
  private MultipleOutputs mos;

  private static HashSet<ImmutableBytesWritable> delSet
      = new HashSet<ImmutableBytesWritable>();
  private static HashSet<ImmutableBytesWritable> mdfSet
      = new HashSet<ImmutableBytesWritable>();

  private static final byte[] NEW = Bytes.toBytes("1");
  private static final byte[] LOAD = NEW;
  private static final byte[] UNLOAD = Bytes.toBytes("0");
  // private static final KeyValue kvLf = new KeyValue(key.get(), cf, lf,
  // TRUE);
  private static final byte[] cf = Bytes.toBytes("P");
  private static final byte[] l = Bytes.toBytes("l");

  @Override
    protected void setup(Context context)
      throws IOException, InterruptedException {
    mos = new MultipleOutputs(context);
    super.setup(context);
  }

  
  /**
   * Describe <code>reduce</code> method here.
   *
   * @param key a <code>Triple</code> value
   * @param values an <code>Iterable<NullWritable></code> value
   * @param context a <code>Context</code> value
   * @exception IOException if an error occurs
   * @exception InterruptedException if an error occurs
   */
  public void reduce(Triple key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {
    DupPair dupPair = key.getDupPair();
    PageNode n1 = dupPair.getN1();
    PageNode n2 = dupPair.getN2();

    byte[] n1Nf = n1.getNf();
    byte[] n2Nf = n2.getNf();
    byte[] n1Lf = n1.getLf();
    byte[] n2Lf = n2.getLf();

    // ImmutableBytesWritable outputKey = null;
    // ImmutableBytesWritable n1Urlid = null;
    // Log.info("n1:=" + n1 + "\tn2:=" + n2);
    ImmutableBytesWritable outputKey = new ImmutableBytesWritable(n2.getUrlid());
    ImmutableBytesWritable n1Urlid = new ImmutableBytesWritable(n1.getUrlid());
    // n1 为new，并且n1不在删除名单之中，分成两种情况：1 n2为new，n2进删除名单 2 n2为load
    // n2则进入状态修改列表（此状态列表即为后面的发给索引的删除列表）
    if (Arrays.equals(n1Nf, NEW)) {
      if (!delSet.contains(n1Urlid)) {
        if (Arrays.equals(n2Nf, NEW)) {
          if (!delSet.contains(outputKey)) {
            delSet.add(outputKey);
            Log.info("n1 is new ,n2 is new, n2 add to delete list n1=" + n1 + " n2=" + n2);
            context.write(outputKey, n1Urlid);
          }
        }

        if (Arrays.equals(n2Lf, LOAD)) {
          if (!mdfSet.contains(outputKey)) {
            mdfSet.add(outputKey);
            Log.info("n1 is new ,n2 is load,n2 add to modify list n1=" + n1 + " n2=" + n2);
            mos.write("modifyingList", outputKey, n1Urlid);
          }
        }
      }
    }
    // n1 为load
    else {
      if (Arrays.equals(n2Nf, NEW)) {
        if (!delSet.contains(outputKey)) {
          delSet.add(outputKey);
          Log.info("n1 is load ,n2 is new,n2 add to delete list n1=" + n1 + " n2=" + n2);
          context.write(outputKey, n1Urlid);
        }
      }

    }
  }

  @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    mos.close();
    Log.info("current size of delSet:" + delSet.size());
    Log.info("current size of mdfSet:" + mdfSet.size());
    super.cleanup(context);
  }

}
