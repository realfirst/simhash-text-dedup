package com.zhongsou.incload;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Describe class <code>DeDupReducer</code> here.
 *
 * reduce class for deduplicate
 *
 * @author <a href="mailto:dingje@zhongsou.com">Ding Jingen</a>
 * @version 1.0
 */
public class DeDupReducer extends
                          Reducer<KeyValuePair, DupPair, ImmutableBytesWritable, DupPair> {
  private DupPair value = new DupPair();
  // private DupPair failValue;
  private DupPair firstValue;
  private final PageNode n1 = new PageNode();
  private MultipleOutputs mos;

  @Override
    protected void setup(Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    mos = new MultipleOutputs(context);
  }

  
  /**
   * Describe <code>reduce</code> method here.
   *
   * @param combineKey a <code>KeyValuePair</code> value
   * @param values an <code>Iterable<DupPair></code> value
   * @param context a <code>Context</code> value
   * @exception IOException if an error occurs
   * @exception InterruptedException if an error occurs
   */
  public void reduce(KeyValuePair combineKey, Iterable<DupPair> values, Context context)
      throws IOException, InterruptedException {
    ImmutableBytesWritable key = combineKey.getDupUrlid();
    Iterator<DupPair> it = values.iterator();
    firstValue = it.next();

    PageNode pn = firstValue.getN1();
    PageNode n2 = firstValue.getN2();

    // 有未加载docid，输出
    assert(!(n2 == null && pn == null));
    assert(pn != null);
    if (pn != null) {
      n1.set(pn);
    }

    while (it.hasNext()) {
      value = it.next();
      if (value.getN1() == null && value.getN2() == null) {
        mos.write("unloadKey", key, NullWritable.get());
        context.getCounter("dedup", "emit4").increment(1);
        continue;
      }
      value.setN1(n1);
      context.write(key, value);
    }
  }

  @Override
    protected void cleanup(Context context) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub
    super.cleanup(context);
    mos.close();
  }

}
