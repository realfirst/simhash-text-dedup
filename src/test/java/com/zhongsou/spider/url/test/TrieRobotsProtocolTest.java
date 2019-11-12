package com.zhongsou.spider.url.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.zhongsou.spider.common.url.TrieRobotsProtocol;

public class TrieRobotsProtocolTest {
	TrieRobotsProtocol p;

	public TrieRobotsProtocolTest() {
		p = new TrieRobotsProtocol();
	}

	
	
//	-/user/
//	-/Member/*
//	-/member/*
//	-/floatcar.html?goods_id=*
//	-/index.php
//	-/*.js$
//	-/*.css$
//	-/*%*
//	-/cart.html
//	-/*.css?id=*
//	-/*.js?id=*
//	-/</
//	-/*ACT=JSLANG*
//	-/index.php?act=*
//	-/index.php?app=*
//	-/list-*.html
//	-/for/
//	-/for/tag/
//	-/fashion/
//	-/fashion/edittj/
//	-/PhotoShow/
//	-/*?*
//	-/*javascript*
//	-/view/
//	-/shop/
//	-/index.php?*
//	-/district-*-order-mix_money.html
//	-/district-*-order-mix_num.html
//	-/goods-*-export_taobao.html
//	-/index.php?app=default&act=get_rt_orders
//	-/home.html
//	-/beihuojie
//	-/beihuojie_apply
//	-/beihuojie_remit/
//	-/activity/
	
	
	
	@Before
	public void before() {
		System.out.println("before");
		List<String> tlist = new LinkedList<String>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(new File("/home/dape/robots.txt"))));
			String line = null;

			while ((line = reader.readLine()) != null) {
				tlist.add(line.trim());
			}
			reader.close();
			p.init(tlist.toArray(new String[tlist.size()]));

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//
		// p.init(str);
	}

	@Test
	public void testCheckURL() {
//		Assert.assertFalse(p.checkURL("/user/adfa?"));
//		Assert.assertFalse(p.checkURL("/Member/"));
//		Assert.assertFalse(p.checkURL("/member/adafa"));
//		Assert.assertTrue(p.checkURL("/Member"));
//
//		Assert.assertFalse(p.checkURL("/floatcar.html?goods_id=ada"));
//		Assert.assertFalse(p.checkURL("/index.php"));
//
//		Assert.assertFalse(p.checkURL("/adfafa/adfafa.js"));
//		Assert.assertFalse(p.checkURL("/adfafa/adfaf%/adfadf"));
//
//		Assert.assertFalse(p.checkURL("/adfafaACT=JSLANG"));
//
//		Assert.assertFalse(p.checkURL("/aadfa?ada"));
//
//		Assert.assertFalse(p.checkURL("/goods-*-export_taobao.html"));
//		Assert.assertTrue(p.checkURL("/activity"));
		
		Assert.assertFalse(p.checkURL("/install/"));
		Assert.assertFalse(p.checkURL("/forum.php?mod=redirectadfa"));
		Assert.assertFalse(p.checkURL("/adfaf?mod=misc"));
		Assert.assertTrue(p.checkURL("/amisc.php"));
		
		

	}
	
	
//	@Test
//	public void testMatch()
//	{
//		Assert.assertTrue(p.matchWildCard("/adfafa/adfaf", "*"));
//		Assert.assertTrue(p.matchWildCard("/adfafa/adfaf", "/*"));
//		Assert.assertTrue(p.matchWildCard("/adfafa/adfaf", "/*/adf*af"));
//		Assert.assertTrue(p.matchWildCard("/adfafa/adfaf", "/*af"));
//		Assert.assertFalse(p.matchWildCard("/adfafa/adfaf", "*afd"));
//		
//		
//		
//	}
	
}
