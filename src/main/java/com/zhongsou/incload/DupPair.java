package com.zhongsou.incload;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.mortbay.log.Log;

/**
 * Describe class <code>DupPair</code> here.
 *
 * @author <a href="mailto:dingje@zhongsou.com">Ding Jingen</a>
 * @version 1.0
 */
public class DupPair
    implements WritableComparable<DupPair> {
  private PageNode n1;
  private PageNode n2;

  public DupPair() {}

  public DupPair(PageNode n1, PageNode n2) {
    this.n1 = n1;
    this.n2 = n2;
  }

  @Override
    public void write(final DataOutput out)
      throws IOException {
			
    if (n1 != null) {
      out.writeByte(1);
      n1.write(out);
    } else {
      out.writeByte(0);
    } 

    if (n2 != null) {
      out.writeByte(1);
      n2.write(out);
    } else {
      out.writeByte(0);
    }
				
  }

  @Override
    public void readFields(final DataInput in)
      throws IOException {
				
    if (in.readByte() == 1) {
      n1 = new PageNode();
      n1.readFields(in);
      if (in.readByte() == 1) {
        n2 = new PageNode();
        n2.readFields(in);
      } else {
        n2 = null;
      }
    } else {
      n1 = null;
      if (in.readByte() == 1) {
        n2 = new PageNode();
        n2.readFields(in);
      }
      else
      {
    	  n2=null;
      }
    }
  }

  public int compareTo(DupPair o) {
    int cmp = 0;

    if (n1 != null && n2 == null) {
      if (o.n1 != null && o.n2 == null) {
        return cmp = WritableComparator.compareBytes(n1.getUrlid(), 0, 8, o.n1.getUrlid(), 0, 8);
      } else {
        return 1;
      } 
    } else if (n1 == null && n2 != null) {
      if (o.n1 == null && o.n2 != null) {
        return cmp = WritableComparator.compareBytes(n2.getUrlid(), 0, 8, o.n2.getUrlid(), 0, 8);
      } else {
        return -1;
      }
    }
    return cmp;
    /*
      if (n1 != null && n2 != null &&
      o.n1 != null && o.n2 != null) {
      // cmp = StringUtils.byteToHexString(n1.getUrlid()).
      // compareTo(StringUtils.byteToHexString(o.n1.getUrlid()));
      cmp = WritableComparator.compareBytes(n1.getUrlid(), 0, 8, o.n1.getUrlid(), 0, 8);
      if (cmp != 0) {
      return cmp;
      }

      // cmp = StringUtils.byteToHexString(n2.getUrlid()).
      // compareTo(StringUtils.byteToHexString(o.n2.getUrlid()));
      cmp = WritableComparator.compareBytes(n2.getUrlid(), 0, 8, o.n2.getUrlid(), 0, 8);      
      return cmp;
      } else if (n1 != null && n2 == null &&
      o.n1 != null && o.n2 == null) {
      // cmp = StringUtils.byteToHexString(n1.getUrlid()).
      // compareTo(StringUtils.byteToHexString(o.n1.getUrlid()));
      cmp = WritableComparator.compareBytes(n1.getUrlid(), 0, 8, o.n1.getUrlid(), 0, 8);      
      return cmp;
      } else if (n1 == null && n2 != null &&
      o.n1 == null && o.n2 != null) {
      // cmp = StringUtils.byteToHexString(n2.getUrlid()).
      // compareTo(StringUtils.byteToHexString(o.n2.getUrlid()));
      cmp = WritableComparator.compareBytes(n2.getUrlid(), 0, 8, o.n2.getUrlid(), 0, 8);      
      return cmp;
      } else if (n1 == null && n2 == null &&
      o.n1 == null && o.n2 == null) {
      return 0;
      } else if (n1 != null && n2 != null &&
      o.n1 != null && o.n2 == null) {
      // cmp = StringUtils.byteToHexString(n1.getUrlid()).
      // compareTo(StringUtils.byteToHexString(o.n1.getUrlid()));
      cmp = WritableComparator.compareBytes(n1.getUrlid(), 0, 8, o.n1.getUrlid(), 0, 8);      
      if (cmp != 0) {
      return cmp;
      }
      return 1;
      } else if (n1 != null && n2 != null &&
      o.n1 == null && o.n2 != null) {
      return 1;
      } else if (n1 != null && n2 != null &&
      o.n1 == null && o.n2 == null) {
      return 1;
      } else if (n1 != null && n2 == null &&
      o.n1 != null && o.n2 != null) {
      // cmp = StringUtils.byteToHexString(n1.getUrlid()).
      // compareTo(StringUtils.byteToHexString(o.n1.getUrlid()));
      cmp = WritableComparator.compareBytes(n1.getUrlid(), 0, 8, o.n1.getUrlid(), 0, 8);      
      if (cmp != 0) {
      return cmp;
      }
      return -1;
      } else if (n1 != null && n2 == null) {
      return 1;
      } else if (n1 == null && n2 != null &&
      o.n1 != null) {
      return -1;
      } else if (n1 == null && n2 != null &&
      o.n1 == null && o.n2 == null) {
      return 1;
      } else if (n1 == null && n2 == null &&
      o.n1 != null) {
      return -1;
      } else if (n1 == null && n2 == null &&
      o.n1 == null && o.n2 != null) {
      return -1;
      } else {
      return 0;
      }*/
  }

  public PageNode getN1() {
    return n1;
  }

  public void setN1(PageNode node) {
    this.n1 = node;
  }

  public PageNode getN2() {
    return n2;
  }

  public void setN2(PageNode node) {
    this.n2 = node;
  }

  public void set(PageNode n1, PageNode n2) {
    this.n1 = n1;
    this.n2 = n2;
  }
  
  @Override
    public String toString() {
    StringBuilder sb = new StringBuilder();
    if (n1 != null) {
      sb.append(n1.toString());
    } else {
      sb.append("(null) ");
    }

    if (n2 != null) {
      sb.append(n2.toString());
    } else {
      sb.append(" [null]");
    }
    return sb.toString();
  }

  @Override
    public boolean equals(Object o) {
    if (!(o instanceof DupPair)) {
      return false ;
    }
    DupPair other = (DupPair)o;

    if (this.n1 != null && this.n2 != null &&
        other.n1 != null && other.n2 != null) {
      return this.n1.equals(other.n1) && this.n2.equals(other.n2);
    } else  if (this.n1 == null && this.n2 == null &&
                other.n1 == null && other.n2 == null) {
      return true;
    } else if (this.n1 != null && this.n2 == null &&
               other.n1 != null && other.n2 == null) {
      return this.n1.equals(other.n1);
    } else if (this.n1 == null && this.n2 != null &&
               other.n1 == null && other.n2 != null) {
      return this.n2.equals(other.n2);
    } else {
      return false;
    }
  }

  @Override
    public int hashCode() {
    if (n1 != null && n2 != null) {
      return n1.hashCode() * 31 + n2.hashCode();
    } else if (n1 == null && n2 != null) {
      return n2.hashCode();
    } else if (n1 != null && n2 == null) {
      return n1.hashCode();
    } else {
      return 0;
    }
  }

  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(DupPair.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int cmp;
      Log.info("duppair compare method is working");
      cmp = compareBytes(b1, s1, 1, b2, s2, 1);
      return cmp;
    }
  }

  static {
    WritableComparator.define(DupPair.class, new Comparator());
  }
}
