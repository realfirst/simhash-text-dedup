package com.zhongsou.spider.bean;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ParserResult {
	byte[] finger;
	byte[] url;
	byte[] urlMd5;
	Set<URLInfo> infoSet;
	byte[] meta;

	public byte[] getMeta() {
		return meta;
	}

	public void setMeta(byte[] meta) {
		this.meta = meta;
	}

	public byte[] getFinger() {
		return finger;
	}

	public void setFinger(byte[] finger, int length) {
		this.finger = finger;
	}

	public byte[] getUrl() {
		return url;
	}

	public void setUrl(byte[] url, int length) {
		this.url = url;

	}

	public Set<URLInfo> getInfoSet() {
		return infoSet;
	}

	public void setInfoSet(Set<URLInfo> infoSet) {
		this.infoSet = infoSet;
	}

	public byte[] getUrlMd5() {
		return urlMd5;
	}

	public void setUrlMd5(byte[] urlMd5, int length) {
		this.urlMd5 = urlMd5;
	}

	public ParserResult() {
		infoSet = new HashSet<URLInfo>();
	}

	public void addURLInfo(URLInfo info) {
		infoSet.add(info);
	}

}
