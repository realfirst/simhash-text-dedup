package com.zhongsou.incload;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.NumberUtil;

/**
 * Describe class <code>DeDupMapper</code> here.
 *
 * @author <a href="mailto:dingje@zhongsou.com">Ding Jingen</a>
 * @version 1.0
 */
public class DeDupMapper extends TableMapper<KeyValuePair, DupPair> {
  // extends TableMapper<ImmutableBytesWritable, DupPair> {
  private static final int TABLE_SIZE = 4;
  private static final int URLID_LEN = 8;
  private static final int FINGER_PREFIX_LEN = 2;
  private static final int FINGER_SUFFIX_LEN = 6;
  private static final int URLID_FINGERSUFFIX_SIZE = 14;

  private Map<ImmutableBytesWritable, ImmutableBytesWritable> map = null;
  private Set<ImmutableBytesWritable> keySet = null; // keySet == urlidTable
  private List<Map<Character, byte[]>> extTables = null;

  private final byte[] columnFamily = Bytes.toBytes("P");
  private final byte[] f = Bytes.toBytes("f");
  private final byte[] p = Bytes.toBytes("p");
  private final byte[] l = Bytes.toBytes("la");
  private final byte[] n = Bytes.toBytes("n");

  private ImmutableBytesWritable dupUrlid = new ImmutableBytesWritable();
  private DupPair value = new DupPair();
  private KeyValuePair combineKey = new KeyValuePair();

  private final PageNode n1 = new PageNode();
  private final PageNode n2 = null;

  private byte[] currentPfxArray = new byte[FINGER_PREFIX_LEN];
  private byte[] probedUrlid = new byte[URLID_LEN];
  private byte[] permutedFingerSuffixs = new byte[FINGER_SUFFIX_LEN
                                                  * TABLE_SIZE];

  private static final byte[] LOAD = Bytes.toBytes("1");
  private static final byte[] UNLOAD = Bytes.toBytes("0");
  private static final byte[] NEW = Bytes.toBytes("1");
  private static final byte[] OLD = Bytes.toBytes("0");
  private byte[] timestampByte;
  private static final byte[] ZERO = Bytes.toBytes("0");
  private static final byte[] allzero = new byte[8];
  private Set<ImmutableBytesWritable> dupUrlidSet = new HashSet<ImmutableBytesWritable>(4);

  private int ts;

  enum MyCounter {
    EMIT1, EMIT2, EMIT3, EMIT4;
  }

  @Override
    public void setup(Context context) throws IOException {
    String uric = context.getConfiguration().get("urlid_finger_load_file");
    MemTable memTable = new MemTable(context.getConfiguration());
    map = memTable.buildTable(uric);
    keySet = map.keySet();
    Log.info("the size of per mem table:\t" + keySet.size());
    extTables = memTable.buildExtTable(map);
    ts = context.getConfiguration().getInt("timestamp", 0);
    timestampByte = Bytes.toBytes(ts);
    Log.info("new timestamp for nf:" + ts);
  }

  /**
   * Describe <code>map</code> method here.
   *
   * @param row
   *            an <code>ImmutableBytesWritable</code> value
   * @param columns
   *            a <code>Result</code> value
   * @param context
   *            a <code>Context</code> value
   * @exception IOException
   *                if an error occurs
   * @exception InterruptedException
   *                if an error occurs
   */
  @Override
    public void map(ImmutableBytesWritable row, Result columns, Context context)
      throws IOException, InterruptedException {

    byte[] urlid = null;
    byte[] finger = null;
    byte[] newfinger = null;
    byte[] pr = null;
    byte[] lf = null;
    byte[] nf = null;
    // byte [] nf = Bytes.toBytes("1");

    Character currentPfx = null;
    Map<Character, byte[]> currentExtTable = null;
    byte[] prefixMatchedTableValue = null;
    try {
      urlid = columns.getRow();
      finger = columns.getValue(columnFamily, f);
      pr = columns.getValue(columnFamily, p);
      // pr = Bytes.toBytes(rand.nextFloat());

      if ((lf = columns.getValue(columnFamily, l)) != null) {
        // lf = columns.getValue(columnFamily, l);
        if (Bytes.compareTo(lf, ZERO) != 0)
          lf = LOAD;
        else
          lf = UNLOAD;
      } else {
        lf = UNLOAD;
      }

      nf = columns.getValue(columnFamily, n);

      if (nf != null && Arrays.equals(nf, timestampByte)) {
        nf = NEW;
        context.getCounter("dedup", "new").increment(1);
      } else {
        nf = OLD;
        context.getCounter("dedup", "old").increment(1);
      }

      n1.set(urlid, finger, pr, lf, nf);
      // Log.info("get a input from hbase:" + n1.toString());


      if (keySet.contains(row) ||
          (Arrays.equals(lf, LOAD) && Arrays.equals(nf, OLD))) {
        // query urlidTable
        if (keySet.contains(row)) {
          newfinger = this.map.get(row).get();
          // 如果是新下载，并且指纹未发生变化，并且在后台是加载，需要在reduce之内输出到未加载列表之中
          // 注：如果后台未加载，指纹未变化，不进入这个不加载列表
          if (finger != null && Arrays.equals(finger, newfinger) && Arrays.equals(lf, LOAD)) {
            value.set(n2, n2);
            combineKey.set(row, value);
            context.write(combineKey, value);
            context.getCounter("dedup", "same_finger").increment(1);
          } else {
            context.getCounter("dedup", "different_finger").increment(1);
            finger = newfinger;
            n1.setFinger(newfinger);
          }

          value.set(n1, n2);
          combineKey.set(row, value);
          context.write(combineKey, value);
          context.getCounter(MyCounter.EMIT1).increment(1);

        }

        Set<Character> extKeySet = null;

        // suffix for table0
        System.arraycopy(finger, 2, permutedFingerSuffixs, 0,
                         FINGER_SUFFIX_LEN);
        // suffix for table1
        System.arraycopy(finger, 0, permutedFingerSuffixs,
                         FINGER_SUFFIX_LEN, 2);
        System.arraycopy(finger, 4, permutedFingerSuffixs,
                         FINGER_SUFFIX_LEN + 2, 4);
        // suffix for table2
        System.arraycopy(finger, 0, permutedFingerSuffixs,
                         FINGER_SUFFIX_LEN * 2, 4);
        System.arraycopy(finger, 6, permutedFingerSuffixs,
                         FINGER_SUFFIX_LEN * 2 + 4, 2);
        // suffix for table3
        System.arraycopy(finger, 0, permutedFingerSuffixs,
                         FINGER_SUFFIX_LEN * 3, 6);
        dupUrlidSet.clear();
        for (int i = 0; i < TABLE_SIZE; i++) {
          currentExtTable = extTables.get(i);
          extKeySet = currentExtTable.keySet();
          System.arraycopy(finger, 2 * i, currentPfxArray, 0,
                           FINGER_PREFIX_LEN);
          currentPfx = Character.valueOf(MemTable
                                         .byteArrayToChar(currentPfxArray));
          if (extKeySet.contains(currentPfx)) { // current finger
            // prefix is matched
            // with the extTable
            prefixMatchedTableValue = currentExtTable
                                      .get(currentPfx);
            for (int j = 0; j < prefixMatchedTableValue.length
                         / URLID_FINGERSUFFIX_SIZE; j++) {
              if (lessKHammingDistance(3,
                                       prefixMatchedTableValue, URLID_LEN
                                       + URLID_FINGERSUFFIX_SIZE * j,
                                       permutedFingerSuffixs, FINGER_SUFFIX_LEN
                                       * i, FINGER_SUFFIX_LEN)
                  && !(urlidEquals(urlid,
                                   prefixMatchedTableValue,
                                   URLID_FINGERSUFFIX_SIZE * j))) {
                System.arraycopy(prefixMatchedTableValue,
                                 URLID_FINGERSUFFIX_SIZE * j,
                                 probedUrlid, 0, URLID_LEN);
                dupUrlid.set(probedUrlid);
                if (!dupUrlidSet.contains(dupUrlid)) {
                  if (Bytes.equals(probedUrlid, allzero)) {
                    context.getCounter("dumapper", "zero")
                        .increment(1);
                  }
                  dupUrlidSet.add(dupUrlid);
                  value.set(n2, n1);
                  combineKey.set(dupUrlid, value);
                  if (Arrays.equals(nf, NEW)) {
                    if (dupUrlid.compareTo(urlid) > 0) {
                      context.write(combineKey, value);
                    }
                  } else {
                    context.write(combineKey, value);
                  }
                  context.getCounter(MyCounter.EMIT2).increment(1);
                }
              }
            }
          }
        }
      } else {
        //进入到这个逻辑，包含是在前面被杀掉的指纹出现超常的docid，被当作spam排掉了
        Log.info("error rowkey key=" + n1.toString() + "\t"
                 + Bytes.toInt(columns.getValue(columnFamily, n)));
        context.getCounter("dedup", "wrong_record").increment(1);
      }
    } catch (Exception ex) {
      ex.getStackTrace();
      for(StackTraceElement el : ex.getStackTrace()) {
        Log.info("tracke element" + el.getMethodName() + "\t" +
                 el.getLineNumber() + "\t" + el.getClassName());
      }
      context.getCounter("dedup", "erorr_record").increment(1);
      Log.info("error message" + ex.getMessage()+"\trowkey key="+NumberUtil.getHexString(row.get()));
    }
  }


  /**
   * Describe <code>lessKHammingDistance</code> method here.
   *
   * @param k an <code>int</code> value
   * @param s a <code>byte</code> value
   * @param ss an <code>int</code> value
   * @param d a <code>byte</code> value
   * @param ds an <code>int</code> value
   * @param offset an <code>int</code> value
   * @return a <code>boolean</code> value
   */
  private boolean lessKHammingDistance(int k, final byte[] s, int ss,
                                       final byte[] d, int ds, int offset) {
    byte x;
    int ones = 0;
    int n = 0;
    for (int i = 0; i < offset; i++) {
      x = (byte) ((s[ss + i] ^ d[ds + i]) & 0x000000ff);
      n = 0;
      while (x != 0) {
        ++n;
        x &= (x - 1);
      }
      ones += n;
      if (ones > k) {
        return false;
      }
    }
    return true;
  }

  private boolean urlidEquals(final byte[] left, final byte[] right,
                              int startPos) {

    for (int i = 0; i < URLID_LEN; i++) {
      if (left[i] != right[startPos + i]) {
        return false;
      }
    }

    return true;
  }

}
