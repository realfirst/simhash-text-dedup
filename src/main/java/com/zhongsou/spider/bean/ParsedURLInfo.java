package com.zhongsou.spider.bean;

/**
 * 
 * 解析完的文件的信息
 * 
 * @author Xi Qi
 * 
 */
public class ParsedURLInfo implements Comparable<ParsedURLInfo> {
	String metaFolder;
	String urlInfoFolder;
	String seqTime;

	public String getSeqTime() {
		return seqTime;
	}

	public void setSeqTime(String seqTime) {
		this.seqTime = seqTime;
	}

	public String getMetaFolder() {
		return metaFolder;
	}

	public void setMetaFolder(String metaFolder) {
		this.metaFolder = metaFolder;
	}

	public String getUrlInfoFolder() {
		return urlInfoFolder;
	}

	public void setUrlInfoFolder(String urlInfoFolder) {
		this.urlInfoFolder = urlInfoFolder;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.seqTime.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (!(obj instanceof ParsedURLInfo))
			return false;
		else {
			ParsedURLInfo o = (ParsedURLInfo) obj;
			return this.getSeqTime().equals(o.getSeqTime());
		}
	}

	public String toString() {
		return this.metaFolder + "\t" + this.urlInfoFolder;
	}

	@Override
	public int compareTo(ParsedURLInfo o) {
		// TODO Auto-generated method stub
		return this.seqTime.compareTo(o.getSeqTime());
	}

}
