/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zhongsou.spider.common.url;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhongsou.spider.common.util.ObjectCache;
import com.zhongsou.spider.common.util.SpiderConfiguration;
import com.zhongsou.spider.plugin.Plugin;
import com.zhongsou.spider.plugin.Plugin.PluginImplementation;
import com.zhongsou.spider.plugin.PluginParser;

/** Creates and caches {@link URLFilter} implementing plugins. */
public class URLFilters {

  private URLFilter[] filters;
  public static final Logger LOG = LoggerFactory.getLogger(URLFilters.class);

  public URLFilters(Configuration conf) {
    ObjectCache objectCache = ObjectCache.get(conf);
    this.filters = (URLFilter[]) objectCache.getObject(URLFilter.class.getName());
    if (this.filters == null) {
      try {
        PluginParser pluginParser = PluginParser.getInstance();
        Plugin plugin = pluginParser.getPlugin(URLFilter.class.getName());
        Map<Integer, PluginImplementation> map = plugin.getImplementionmap();
        List<URLFilter> urlfilters = new LinkedList<URLFilter>();
        for (Map.Entry<Integer, PluginImplementation> entry : map.entrySet()) {

					URLFilter urlFilter = null;
					try {
						// check to see if we've cached this URLNormalizer
						// instance yet
						urlFilter = (URLFilter) objectCache.getObject(entry.getValue().getClassName().trim());
						if (urlFilter == null) {
							// go ahead and instantiate it and then cache it
							urlFilter = (URLFilter) entry.getValue().newInstance(conf);
							objectCache.setObject(entry.getValue().getClassName().trim(), urlFilter);
						}
						urlfilters.add(urlFilter);
						LOG.info("add a filter,filter className="+entry.getValue().getClassName());
					} catch (Exception e) {
						e.printStackTrace();
						LOG.warn("URLNormalizers:PluginRuntimeException when " + "initializing url normalizer plugin " + entry.getValue().getClassName() + " instance in getURLNormalizers "
								+ "function: attempting to continue instantiating plugins");
					}
				}
			

        objectCache.setObject(URLFilter.class.getName(), urlfilters.toArray(new URLFilter[urlfilters.size()]));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      this.filters = (URLFilter[]) objectCache.getObject(URLFilter.class.getName());
    }
  }

	/** Run all defined filters. Assume logical AND. */
	public String filter(String urlString) throws URLFilterException {
		String src=urlString;
		for (int i = 0; i < this.filters.length; i++) {
			if (urlString == null)
				return null;
			urlString = this.filters[i].filter(urlString);
			if(urlString==null)
			{
				LOG.debug("url "+src+" is filtered by "+filters[i].getClass().getSimpleName()+" filter");
			}
		}
		return urlString;
	}

	public static void main(String args[]) {
		URLFilters urlFilters = new URLFilters(SpiderConfiguration.create());
		try {
			String s="http://shenzhen.baixing.com/zhengzu/?query=%E5%A4%A7%E5%BA%99%E6%9D%91&%E4%BB%B7%E6%A0%BC=%5B4000+TO+%2A%5D";
			System.out.println(s);
			long t1=System.currentTimeMillis();
			String m=urlFilters.filter(s);
			long t2=System.currentTimeMillis();
			long t3=System.currentTimeMillis();
			String m2=urlFilters.filter(s);
			long t4=System.currentTimeMillis();
			System.out.println("time consume="+(t2-t1)+"\tm="+m+"\n"+m2+"\t"+(t4-t3));
		} catch (URLFilterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
