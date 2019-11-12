package com.zhongsou.incload;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.mortbay.log.Log;

public class KeyValuePair implements WritableComparable<KeyValuePair> {
  private static final int URLID_LEN = 8;
  private static final int DUPURLID_LEN = 8;
  private static final int N1_LEN = 1;
  private static final int N2_LEN = 1;
  private ImmutableBytesWritable dupUrlid = new ImmutableBytesWritable();
  private DupPair dp = new DupPair();

  public KeyValuePair() {
  }

  public KeyValuePair(ImmutableBytesWritable left, DupPair right) {
    set(left, right);
  }

  public void set(ImmutableBytesWritable left, DupPair right) {
    dupUrlid = left;
    dp = right;
  }

  public ImmutableBytesWritable getDupUrlid() {
    return dupUrlid;
  }

  public DupPair getDp() {
    return dp;
  }

  public void readFields(DataInput in) throws IOException {
    dupUrlid.readFields(in);
    dp.readFields(in);
  }


  public void write(DataOutput out) throws IOException {
    dupUrlid.write(out);
    dp.write(out);
  }

  @Override
    public String toString() {
    return "" + dupUrlid + "|" + dp;
  }
  
  @Override
    public int hashCode() {
    return dupUrlid.hashCode() * 157 + dp.hashCode();
  }

  @Override
    public boolean equals(Object right) {
    if (right instanceof KeyValuePair) {
      KeyValuePair r = (KeyValuePair) right;
      return dupUrlid.equals(r.dupUrlid) && dp.equals(r.dp);
    } else {
      return false;
    }
  }

  public static class Comparator extends WritableComparator {
    // private ImmutableBytesWritable.Comparator comparator =
    // new ImmutableBytesWritable.Comparator();
    
    public Comparator() {
      super(KeyValuePair.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      // Log.info("compare of KeyValuePair b1.length" + b1.length + "s1:" + s1 + "l1:" + l1);
      // Log.info("compare of KeyValuePair b2.length" + b2.length + "s2:" + s2 + "l2:" + l2);      
      int cmp = WritableComparator.compareBytes(b1, s1+4, DUPURLID_LEN, b2, s2+4, DUPURLID_LEN);

      if (cmp != 0) {
        return cmp;
      }

      return  -WritableComparator.compareBytes(b1, s1+12, N1_LEN, b2, s2+12, N1_LEN);
    }
  }

  static {                              // register this comparator
    WritableComparator.define(KeyValuePair.class, new Comparator());
  }

  public int compareTo(KeyValuePair o) {
    int cmp = dupUrlid.compareTo(o.dupUrlid);
    if (cmp != 0) {
      return cmp;
    } 
    return dp.compareTo(o.dp);
  }
  
}

