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

import java.net.MalformedURLException;
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

public final class URLNormalizers {

	public static final Logger LOG = LoggerFactory.getLogger(URLNormalizers.class);
	public static String SCOPE_DEFAULT = "default";
	private final URLNormalizer[] EMPTY_NORMALIZERS = new URLNormalizer[0];

	private Configuration conf;

	private URLNormalizer[] normalizers = EMPTY_NORMALIZERS;

	private int loopCount;

	public URLNormalizers(Configuration conf) {
		this.conf = conf;

		ObjectCache objectCache = ObjectCache.get(conf);
		if (normalizers == EMPTY_NORMALIZERS) {
			normalizers = (URLNormalizer[]) objectCache.getObject(URLNormalizer.X_POINT_ID);
			if (normalizers == null) {
				normalizers = getURLNormalizers();
				objectCache.setObject(URLNormalizer.X_POINT_ID, normalizers);
			}
		}

		loopCount = conf.getInt("urlnormalizer.loop.count", 1);
	}

	URLNormalizer[] getURLNormalizers() {
		PluginParser pluginParser = PluginParser.getInstance();
		Plugin plugin = pluginParser.getPlugin(URLNormalizer.class.getName());
		Map<Integer, PluginImplementation> map = plugin.getImplementionmap();
		ObjectCache objectCache = ObjectCache.get(conf);
		List<URLNormalizer> normalizers = new LinkedList<URLNormalizer>();
		for (Map.Entry<Integer, PluginImplementation> entry : map.entrySet()) {

			URLNormalizer normalizer = null;
			try {
				// check to see if we've cached this URLNormalizer instance yet
				normalizer = (URLNormalizer) objectCache.getObject(entry.getValue().getClassName());
				if (normalizer == null) {
					// go ahead and instantiate it and then cache it
					normalizer = (URLNormalizer) entry.getValue().newInstance(conf);
					objectCache.setObject(entry.getValue().getClassName(), normalizer);
				}
				normalizers.add(normalizer);
			} catch (Exception e) {
				e.printStackTrace();
				LOG.warn("URLNormalizers:PluginRuntimeException when " + "initializing url normalizer plugin " + entry.getValue().getClassName() + " instance in getURLNormalizers "
						+ "function: attempting to continue instantiating plugins");
			}
		}
		return normalizers.toArray(new URLNormalizer[normalizers.size()]);
	}

	/**
	 * Normalize
	 * 
	 * @param urlString
	 *            The URL string to normalize.
	 * @param scope
	 *            The given scope.
	 * @return A normalized String, using the given <code>scope</code>
	 * @throws MalformedURLException
	 *             If the given URL string is malformed.
	 */
	public String normalize(String urlString, String scope) throws MalformedURLException {
		// optionally loop several times, and break if no further changes
		try {
			String initialString = urlString;
			for (int k = 0; k < loopCount; k++) {
				for (int i = 0; i < this.normalizers.length; i++) {
					if (urlString == null)
						return null;
					urlString = this.normalizers[i].normalize(urlString, scope);
				}
				if (initialString.equals(urlString))
					break;
				initialString = urlString;
			}
			return urlString;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public String normalize(String urlString) throws MalformedURLException {
		return this.normalize(urlString, SCOPE_DEFAULT);
	}

	public static void main(String args[]) {
		URLNormalizers normalizer = new URLNormalizers(SpiderConfiguration.create());
		try {
			System.out.println(normalizer.normalize("http://test.com?&jsessionid=3232&a=#dfaa", URLNormalizers.SCOPE_DEFAULT));
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
