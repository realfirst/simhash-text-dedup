package com.zhongsou.spider.common.url;

import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configurable;

/** Interface used to convert URLs to normal form and optionally perform substitutions */
public interface URLNormalizer extends Configurable {
  
  /* Extension ID */
  public static final String X_POINT_ID = URLNormalizer.class.getName();
  
  /* Interface for URL normalization */
  public String normalize(String urlString, String scope) throws MalformedURLException;

}
