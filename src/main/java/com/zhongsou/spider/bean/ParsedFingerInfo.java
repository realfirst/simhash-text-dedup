package com.zhongsou.spider.bean;

public class ParsedFingerInfo implements Comparable<ParsedFingerInfo> {
	private String urlAndFingerPath;
	private String hfileOutputPath;
	private String seqTime;
	private String newURLAndFingerFile;
	private String loadFilePath;

	public String getLoadFilePath() {
		return loadFilePath;
	}

	public void setLoadFilePath(String loadFilePath) {
		this.loadFilePath = loadFilePath;
	}

	public String getNewURLAndFingerFile() {
		return newURLAndFingerFile;
	}

	public void setNewURLAndFingerFile(String newURLAndFingerFile) {
		this.newURLAndFingerFile = newURLAndFingerFile;
	}

	public String getUrlAndFingerPath() {
		return urlAndFingerPath;
	}

	public void setUrlAndFingerPath(String urlAndFingerPath) {
		this.urlAndFingerPath = urlAndFingerPath;
	}

	public String getHfileOutputPath() {
		return hfileOutputPath;
	}

	public void setHfileOutputPath(String hfileOutputPath) {
		this.hfileOutputPath = hfileOutputPath;
	}

	public String getSeqTime() {
		return seqTime;
	}

	public void setSeqTime(String seqTime) {
		this.seqTime = seqTime;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.seqTime.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (!(obj instanceof ParsedFingerInfo))
			return false;
		ParsedFingerInfo m = (ParsedFingerInfo) obj;
		return this.seqTime.equals(m.getSeqTime());
	}

	public String toString() {
		return this.seqTime + "\t" + hfileOutputPath + "\t" + urlAndFingerPath;
	}

	@Override
	public int compareTo(ParsedFingerInfo o) {
		// TODO Auto-generated method stub
		return this.seqTime.compareTo(o.getSeqTime());
	}

}
