package com.zhongsou.spider.hbase.test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.MD5.MD5Len;
import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.common.util.URLUtil;

public class CheckFile {
	Configuration conf = new Configuration();
	String seqFile;
	String binaryFile;

	public CheckFile(String sequenceFile, String nameFile) {
		this.seqFile = sequenceFile;
		this.binaryFile = nameFile;
	}

	public boolean checkTwoFile() {
		BufferedInputStream binary = null;
		SequenceFile.Reader reader = null;

		try {
			binary = new BufferedInputStream(new FileInputStream(new File(this.binaryFile)));

			byte buffer[] = new byte[4096];

			int i = 0;
			HashSet<String> set = new HashSet<String>();
			while ((i = binary.read(buffer)) != -1) {
				System.out.println(i);
				for (int k = 0; k < i; k += 16) {
					MD5 md5 = new MD5(buffer, MD5Len.eight, k);
					if (!set.contains(md5.getHexString())) {
						set.add(md5.getHexString());
					} else {
						System.out.println("hext=" + md5.getHexString());
					}

				}

			}
			binary.close();
			FileSystem fs = FileSystem.get(URI.create(this.seqFile), conf);
			Path path = new Path(this.seqFile);
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = new Text();
			Text value = new Text();
			int sum = 0, missing = 0;
			while (reader.next(key, value)) {
				byte a[] = key.getBytes();
				MD5 md5 = new MD5(a, MD5Len.eight, 0);
				if (set.contains(md5.getHexString())) {
					sum++;
				} else {
					System.out.println("no duplicate=" + md5.getHexString());
					missing++;
				}
			}
			System.out.println("set size" + set.size() + " sum=" + sum + "\tmissing=" + missing);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return true;

	}

	public static void main(String args[]) {
		// CheckFile ch = new
		// CheckFile("file:////home/dape/parsed_1/url_finger/part-00000",
		// "/home/dape/parsed_1/text/part-00000");
		// ch.checkTwoFile();
		
		long m=10000l;
		ImmutableBytesWritable k=new ImmutableBytesWritable();
		byte c[]=Bytes.toBytes(m);
		k.set(c);
		byte ac[] = k.get();
		for(int j=0;j<8;j++)
		{
			System.out.println(ac[j]+"\t");
		}
		byte a = ac[7];
		byte b = ac[6];
		System.out.println("a="+a+"b="+b);
		int t = a & 0x000000ff;
		int t2 = b & 0x00000003;
		System.out.println("t2="+t2+"\t"+t);
		int mod= t2*256+ t;
		
		System.out.println("mod="+mod+"\t"+m%1024);
		
//		try {
//			System.out.println(URLUtil.getDomainName("http://en.chinabgao.com/"));
//			byte a[] = StringUtils.hexStringToByte("0800eb8ad492855ecc960311b8eb8a06ebce4cbfe6e1e1faf32dbe340cb3c2b8fef6d4d41d8cd98f00b204");
//			int c = NumberUtil.readShort(a, 0);
//			System.out.println("len" + c);
//			String anchor = new String(a, 2, c, "gbk");
//			System.out.println(anchor);
//			int source = a[2 + c];
//
//			MD5 srcdoc = new MD5(a, MD5Len.eight, 3 + c);
//			MD5 hostdoc = new MD5(a, MD5Len.eight, 11 + c);
//			MD5 targetdoc = new MD5(a, MD5Len.eight, 19 + c);
//			MD5 targethostdoc = new MD5(a, MD5Len.eight, 27 + c);
//
//			System.out.println("source=" + String.valueOf(source) + "\nsrcdoc=" + srcdoc.getHexString() + "\nhostdoc=" + hostdoc.getHexString() + "\ntargetdoc=" + targetdoc.getHexString()
//					+ "\ntargethost=" + targethostdoc.getHexString());
//			
//			String s="2012-10-24 22:15:55";
//			DateFormat format=new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//			Date date=format.parse(s);
//			long m=date.getTime()+10060631;
//			Date t2=new java.util.Date(m);
//			System.out.println(format.format(t2));
//		} catch (MalformedURLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (UnsupportedEncodingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
