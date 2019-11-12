package com.zhongsou.incload;

import java.io.IOException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.log.Log;

/**
 * Describe class <code>SelectLogicMapper</code> here.
 *
 * @author <a href="mailto:dingje@zhongsou.com">Ding Jingen</a>
 * @version 1.0
 */
class SelectLogicMapper
    extends Mapper<ImmutableBytesWritable, DupPair, Triple, NullWritable> {
    
  /**
   * Describe <code>map</code> method here.
   * the function of this is only exchange the n1 and n2
   * when the score of n1 is less than the score of n2
   * @param key an <code>ImmutableBytesWritable</code> value
   * @param value a <code>DupPair</code> value
   * @param context a <code>Context</code> value
   * @exception IOException if an error occurs
   * @exception InterruptedException if an error occurs
   */
  @Override
    protected void map(ImmutableBytesWritable key,
                       DupPair value,
                       Context context)
      throws IOException, InterruptedException {
    PageNode n1 = value.getN1();
    PageNode n2 = value.getN2();

    byte[] score = getScore(n1);
    if (Bytes.toFloat(score) < Bytes.toFloat(getScore(n2))) {
      // Log.info("n1 urlid:\t" + StringUtils.byteToHexString(key.get()) + "score of n1: " + Bytes.toFloat(score) + "\tscore of n2: " + Bytes.toFloat(getScore(n2)));
      score = getScore(n2);
      value = new DupPair(n2, n1);
    }

    context.write(new Triple(key, score, value), NullWritable.get());
  }

  // this method cat be overload for furture
  // measure criterion change
  private byte[] getScore(PageNode pn) {
    return pn.getPr();
  }
}

