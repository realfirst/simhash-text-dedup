package com.zhongsou.incload;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class SizeCal {
  private static final byte[] cf = Bytes.toBytes("P");
  private static final byte[] f = Bytes.toBytes("f");
  private static final byte[] p = Bytes.toBytes("p");
  private static final byte[] l = Bytes.toBytes("l");
  private static final byte[] n = Bytes.toBytes("n");
  
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    

    HTable table = new HTable(conf, "webbase");

    byte[] rowkey = StringUtils.hexStringToByte("30d12800f4e5f192");
    Get get = new Get(rowkey);
    Result result = table.get(get);

    byte[] urlid = result.getRow();
    byte[] finger = result.getValue(cf, f);
    byte[] pr = result.getValue(cf, p);
    byte[] lf = result.getValue(cf, l);
    byte[] nf = result.getValue(cf, n);
    PageNode pn1 = new PageNode(urlid, finger, pr, lf, nf);
    PageNode pn2 = new PageNode(pn1);
    DupPair dp = new DupPair(pn1, pn2);
    
    KeyValuePair kvp = new KeyValuePair(new ImmutableBytesWritable(urlid), dp);

    byte[] a = serialize(dp);
    byte[] b = serialize(kvp);
    int count = 0;
    for (byte item : b) {
      count++;
      System.out.println(count + ":" + item);
    }
    System.out.println();
    System.out.println(a.length);
    System.out.println(b.length);
  }

  public static byte[] serialize(Writable writable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    writable.write(dataOut);
    dataOut.close();
    return out.toByteArray();
  }
  
}
// 0008
 // 48-47400-12-27-15-110
// 148-47400-12-27-15-110-1112-35-10357-122631484649514948
// 148-47400-12-27-15-110-1112-35-10357-122631484649514948
/*
1:0
2:0
3:0
4:8
5:48
6:-47
7:40
8:0
9:-12
10:-27
11:-15
12:-110
13:1
14:48
15:-47
16:40
17:0
18:-12
19:-27
20:-15
21:-110
22:-11
23:12
24:-35
25:-103
26:57
27:-122
28:6
29:31
30:48
31:46
32:49
33:51
34:49
35:48
36:1
37:48
38:-47
39:40
40:0
41:-12
42:-27
43:-15
44:-110
45:-11
46:12
47:-35
48:-103
49:57
50:-122
51:6
52:31
53:48
54:46
55:49
56:51
57:49
58:48

46
58
*/
