package com.zhongsou.spider.common.util;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

public class SpiderConfiguration extends Configuration {

	public static final String UUID_KEY = "spider.conf.uuid";

	private SpiderConfiguration() {
	}

	private static void setUUID(Configuration conf) {
		UUID uuid = UUID.randomUUID();
		conf.set(UUID_KEY, uuid.toString());
	}

	public static String getUUID(Configuration conf) {
		return conf.get(UUID_KEY);
	}

	public static Configuration create() {
		Configuration conf = new Configuration(false);
		setUUID(conf);
		addSpiderResources(conf);
		return conf;
	}

	public static Configuration create(boolean addNutchResources, Properties nutchProperties) {
		Configuration conf = new Configuration();
		setUUID(conf);
		if (addNutchResources) {
			addSpiderResources(conf);
		}
		for (Entry<Object, Object> e : nutchProperties.entrySet()) {
			conf.set(e.getKey().toString(), e.getValue().toString());
		}
		return conf;
	}

	private static Configuration addSpiderResources(Configuration conf) {
		conf.addResource("spider-default.xml");
		return conf;
	}
}
