package com.zhongsou.spider.common.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import org.mortbay.log.Log;

import ucar.unidata.util.Format;

public class NumberUtil {
  private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

  static DateFormat dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss");

  // 将int型转化为C字节数组
  public static byte[] convertIntToC(int num) {
    byte a[] = new byte[4];
    for (int i = 0; i < 4; i++) {
      a[i] = (byte) ((num >> (i * 8)) & 0xff);
    }
    return a;
  }

  public static byte[] convertIntToShortC(int num) {
    byte a[] = new byte[2];
    for (int i = 0; i < 2; i++) {
      a[i] = (byte) ((num >> (i * 8)) & 0xff);
    }
    return a;
  }

  public static int bit_count_sparse(int x)
      // Return number of bits set.
  {
    int n = 0;
    while (x != 0) {
      ++n;
      x &= (x - 1);
    }
    return n;
  }

  public static int readShort(byte a[], int offset) {
    int num = 0;
    for (int i = 0; i <= 1; i++) {
      num += ((a[offset + i] & 0xff) << (i * 8));
    }
    return num;
  }

  public static int readInt(byte a[], int offset) {
    int num = 0;
    for (int i = 0; i <= 3; i++) {
      num += ((a[offset + i] & 0xff) << (i * 8));
    }
    return num;
  }

  public static byte[] convertLongToC(long val) {
    byte[] b = new byte[8];
    for (int i = 0; i < 7; i++) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[7] = (byte) val;
    return b;
  }

  public static float readFloat(byte a[], int offset) {
    int m = readInt(a, offset);
    return Float.intBitsToFloat(m);
  }

  public static long readLong(byte a[], int offset) {
    long num = 0;
    long m = 0;
    for (int i = 0; i <= 7; i++) {
      m = ((a[offset + i] & 0xff));
      m = m << (i * 8);
      num += m;
    }
    return num;
  }

  public static int bytesToInt(byte bytes[], int offset, int length) throws Exception {
    if (length > 4 || offset + length > bytes.length) {
      throw new Exception("bytes to long");
    }
    int n = 0;
    for (int i = offset; i < (offset + length); i++) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }

  public static int calNumReduce(int recordSum, int fetchRecord, int errorBoundary, int minReduce) {
    if (fetchRecord >= recordSum) {
      return 1;
    }
    int onePart = 0;
    int m = minReduce;
    int left = 0;
    int left2 = 0;
    int left3 = 0;
    int left4 = 0;
    for (int i = minReduce; i <= minReduce * 4; i++) {
      onePart = recordSum / i;
      left = recordSum % onePart;
      left2 = fetchRecord % onePart;
      left3 = Math.abs(left2 - onePart);
      left4 = Math.min(left2, left3);
      System.out.println("part=" + onePart + " left=" + left + " left2=" + left2 + " left3=" + Math.abs(left2 - onePart) + " left4=" + left4);
      if (left < errorBoundary && (left2 < errorBoundary)) {
        return i;
      }
    }
    return minReduce;
  }

  public static String getHexString(byte a[]) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; a!=null&&i < a.length; i++) {
      int b = a[i];
      buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
      buf.append(HEX_DIGITS[b & 0xf]);
    }
    return buf.toString();

  }

  public static String getHexString(byte a[], int offset, int len) {
    StringBuffer buf = new StringBuffer();
    for (int i = offset; i < len && offset + i < a.length; i++) {
      int b = a[i];
      buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
      buf.append(HEX_DIGITS[b & 0xf]);
    }
    return buf.toString();

  }

  final static char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
                                 'x', 'y', 'z' };

  public static String getBinaryString(byte a[]) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < a.length; i++) {
      int b = a[i];
      char[] cbuf = new char[8];
      int charPos = 8;
      int radix = 1 << 1;
      int mask = radix - 1;
      do {
        cbuf[--charPos] = digits[b & mask];
        b >>>= 1;
      } while (b != 0);
      for (charPos--; charPos >= 0; charPos--) {
        cbuf[charPos] = '0';
      }
      buf.append(new String(cbuf));
    }
    return buf.toString();
  }

  public static String dateSecondToString(int ds) {
    Date date = new Date(1l * ds * 1000);
    return dateFormat.format(date);
  }

  public static String getCurrentDateString() {
    Date date = new Date();
    return dateFormat.format(date);
  }

  public static int parseDateToSecond(String datestr) throws ParseException {
    try {
      Date d = dateFormat.parse(datestr.trim());
      int m = (int) (d.getTime() / 1000);
      return m;
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      Log.info("error date str " + datestr);
      throw e;
    }

  }

  public static void main(String args[]) {
    int t = NumberUtil.bit_count_sparse(11);
    System.out.println(t);
  }
}
