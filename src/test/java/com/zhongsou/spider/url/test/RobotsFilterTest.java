package com.zhongsou.spider.url.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zhongsou.spider.common.url.RobotsFilter;
import com.zhongsou.spider.common.url.URLFilterException;

public class RobotsFilterTest {
	// URLFilters filter;
	RobotsFilter filter;
	List<String> urllist = new LinkedList<String>();

  @Before
  public void init() {
    // RobotsProtocol
    // p("/home/dape/robots_output/robotidx-r-00000","/home/dape/robots_output/part-r-00000");

		Configuration conf = HBaseConfiguration.create();
		conf.set("robot_idx_name", "/home/dape/robots_output/robotidx-r-00000");
		conf.set("robot_file_name", "/home/dape/robots_output/part-r-00000");
		// filter = new URLFilters(conf);
		// filter.setConf(conf);
		filter = new RobotsFilter();
		filter.setConf(conf);

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(new File("url.txt"))));
			String line = null;
			while ((line = reader.readLine()) != null) {
				urllist.add(line.trim());
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// try {
		// testURLFromHbase();
		// } catch (URLFilterException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@After
	public void testWeakHashMap() {
		// try {
		// BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
		// new FileOutputStream(new File("url.txt"))));
		// for (String url : this.urllist) {
		// writer.write(url + "\r\n");
		// }
		// writer.close();
		// } catch (FileNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	public void testlongtimeurl() {

	}

	@Test
	public void testurl() throws URLFilterException {

		// Assert.assertNull(filter.filter("http://zz.454.cn/cache/dafa"));
		// Assert.assertNull(filter.filter("http://zz.454.cn/member/"));
		// Assert.assertNull(filter.filter("http://zz.454.cn/upload_files/dafa"));
		// Assert.assertNotNull(filter.filter("http://zz.454.cn/adcache/dafa"));
		//
		// Assert.assertNull(filter.filter("http://zz.bbs.house.sina.com.cn/api/"));
		// Assert.assertNull(filter
		// .filter("http://zz.bbs.house.sina.com.cn/attachment.php"));
		// Assert.assertNull(filter
		// .filter("http://zz.bbs.house.sina.com.cn/core/"));
		// Assert.assertNotNull(filter
		// .filter("http://zz.bbs.house.sina.com.cn/adfaf"));
		//
		// Assert.assertNull(filter.filter("http://zz.bbs.lanfw.com/install/"));
		// Assert.assertNull(filter
		// .filter("http://zz.bbs.lanfw.com/forum.php?mod=redirect"));
		// Assert.assertNull(filter
		// .filter("http://zz.bbs.lanfw.com/adfaf?mod=misc"));
		// Assert.assertNotNull(filter.filter("http://zz.bbs.lanfw.com/amisc.php"));
		//
		// Assert.assertNull(filter
		// .filter("http://zz.zhcoo.com/company.aspx?adaf&page="));
		// Assert.assertNull(filter
		// .filter("http://zz.zhcoo.com/showevaluation.aspx?clsid="));
		// Assert.assertNull(filter
		// .filter("http://zz.zhcoo.com/company/index.aspx"));
		// //
		// Assert.assertNotNull(filter.filter("http://zz.zhcoo.com/products.aspx"));
		//
		// Assert.assertNull(filter.filter("http://zz576880607.ledgb.com/kind/"));
		// Assert.assertNull(filter
		// .filter("http://zz576880607.ledgb.com/aboutus/barrister.html"));
		// Assert.assertNull(filter
		// .filter("http://zz576880607.ledgb.com/GetPassword_first.aspx"));
		// Assert.assertNotNull(filter
		// .filter("http://zz576880607.ledgb.com/adfafa"));
		//
		// Assert.assertNull(filter
		// .filter("http://zzbbs.soufun.com/soufun_forum/post/frm_post_modify.aspx"));
		// Assert.assertNull(filter
		// .filter("http://zzbbs.soufun.com/soufun_forum/get_code.aspx"));
		// Assert.assertNull(filter
		// .filter("http://zzbbs.soufun.com/upload_filesmanager/adfa"));
		// Assert.assertNotNull(filter
		// .filter("http://zzbbs.soufun.com/adcache/dafa"));
		//
		// Assert.assertNull(filter.filter("http://zzchenji732.net114.com/html/"));
		// Assert.assertNull(filter
		// .filter("http://zzchenji732.net114.com/adfassell:afadf.html"));
		// Assert.assertNull(filter
		// .filter("http://zzchenji732.net114.com/citys-ct-adfadf.html"));
		// Assert.assertNull(filter
		// .filter("http://zzchenji732.net114.com/sadfa-c-adfa.html"));
		// Assert.assertNotNull(filter
		// .filter("http://zz.mayi.com/zhongyuan%d6%a3%d6%dd%bb%f0%b3%b5%d5%be_3db6mzd-minju-200-9|999yuanchuangjianzuizao-duanzufang-3d/"));

		Assert.assertNull(filter.filter("http://sanming.bbs.house.sina.com.cn/register.php"));
	}

	public void testURL() throws IOException {
		long t = System.currentTimeMillis();
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(new File("/home/dape/b.txt"))));
		for (String s : urllist) {
			long s1 = System.currentTimeMillis();
			String m = filter.filter(s);
			long s2 = System.currentTimeMillis();
			if (s2 - s1 > 100) {
				System.out.println(s + "\t" + (s2 - s1) + "m=" + m);
			}
			if (m == null) {
				// System.out.println(s);
				try {
					writer.write(s + "\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		writer.close();
		long t2 = System.currentTimeMillis();
		// System.out.println("urlsize " + urllist.size() + "\ttime=" + (t2 -
		// t));
  }

	public void testURLFromHbase() throws URLFilterException {
		Scan scan = new Scan();
		Configuration conf = HBaseConfiguration.create();
		try {
			HTable table = new HTable(conf, "webDB");
			byte a[] = Bytes.toBytes("0000000000000000");
			byte b[] = Bytes.toBytes("2ffffff000000000");
			scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("u"));
			scan.setStartRow(a);
			scan.setStopRow(b);
			scan.setCaching(500);
			ResultScanner scanner = table.getScanner(scan);
			byte F[] = Bytes.toBytes("F");
			byte u[] = Bytes.toBytes("u");
			int i = 0;
			for (Result result : scanner) {
				byte url[] = result.getValue(F, u);
				if (url != null) {
					String turl = new String(url);
					// String r = filter.filter(turl);
					urllist.add(turl);
					if (urllist.size() > 50000)
						break;
				}

			}
			scanner.close();
			System.out.println("url not filtered =" + i);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

}
