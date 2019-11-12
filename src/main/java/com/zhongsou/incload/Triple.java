package com.zhongsou.incload;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.StringUtils;

/**
 * Describe class <code>FourTuple</code> here.
 *
 * @author <a href="mailto:dingje@zhongsou.com">Ding Jingen</a>
 * @version 1.0
 */
public class Triple
    implements WritableComparable<Triple> {
  private ImmutableBytesWritable key;
  private byte[] score;
  private DupPair dupPair;
  
  public Triple() {
    set(new ImmutableBytesWritable(), 
        new byte[4], 
        new DupPair());
  }

  public Triple(ImmutableBytesWritable key, 
                byte [] score,
                DupPair dupPair) {
    set(key, score, dupPair);
  }

  public void set(ImmutableBytesWritable key, 
                  byte[] score,
                  DupPair dupPair) {
    this.key = key;
    this.score = score;
    this.dupPair = dupPair;
  }

  public ImmutableBytesWritable getKey() {
    return key;
  }
  
  public byte[] getScore() {
    return score;
  }

  public DupPair getDupPair() {
    return dupPair;
  }
  
  @Override
    public void write(final DataOutput out)
      throws IOException {
    key.write(out);
    out.write(score, 0, 4);
    dupPair.write(out);
  }

  @Override
    public void readFields(final DataInput in)
      throws IOException {
    key.readFields(in);
    in.readFully(score, 0, 4);
    dupPair.readFields(in);
  }

  @Override
    public int hashCode() {
    final int prime = 31;
    int result = 1;

    result = result * prime + ((key == null) ? 0 : key.hashCode());
    result = result * prime + ((score == null) ? 0 : Arrays.hashCode(score));
    result = result * prime + ((dupPair == null) ? 0 : dupPair.hashCode());

    return result;
  }

  @Override
    public boolean equals(Object o) {
    if (o instanceof Triple) {
      Triple tp = (Triple)o;
      return this.key.equals(tp.key) && 
          Arrays.equals(score, tp.score) &&
          dupPair.equals(tp.dupPair);
    }
    return false;
  }

  @Override
    public int compareTo(Triple tp) {
    int cmp = key.compareTo(tp.key);
    if (cmp != 0) {
      return cmp;
    }

    cmp = new Float(Bytes.toFloat(score)).
          compareTo(new Float(Bytes.toFloat(tp.getScore())));
    if (cmp != 0) {
      return cmp;
    }

    cmp = StringUtils.byteToHexString(dupPair.getN1().getUrlid()).
          compareTo(StringUtils.byteToHexString(tp.dupPair.getN1().getUrlid()));
    if (cmp != 0) {
      return cmp;
    }

    return (StringUtils.byteToHexString(dupPair.getN2().getUrlid()).
            compareTo(StringUtils.byteToHexString(tp.dupPair.getN2().getUrlid())));
  }

  @Override
    public String toString() {
    return StringUtils.byteToHexString(key.get()) + "\t" +
        StringUtils.byteToHexString(score) + "\t" +
        dupPair.toString();
  }
}
