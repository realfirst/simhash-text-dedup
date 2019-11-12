package com.zhongsou.spider.hadoop;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

public class HadoopFileSystemManager {

	private static HadoopFileSystemManager _self = null;
	FileSystem fs = null;

	private HadoopFileSystemManager() {
		fs = HDFSUtil.getFileSystem();
	}

	public FileSystem getFileSystem()
	{
		return fs;
	}
	public static synchronized HadoopFileSystemManager getInstance() {
		if (_self == null) {
			synchronized (HadoopFileSystemManager.class) {
				if (_self == null) {
					_self = new HadoopFileSystemManager();
				}
			}
		}
		return _self;
	}

	public List<FileInfo> listFile(String directory) {
		if (directory == null || directory.equals(""))
			directory = ".";
		LinkedList<FileInfo> tlist = new LinkedList<FileInfo>();
		HDFSUtil.listFile(fs, directory, tlist, false);
		return tlist;
	}

	public List<FileInfo> listAllFiles(String directory) {
		LinkedList<FileInfo> tlist = new LinkedList<FileInfo>();
		HDFSUtil.listFile(fs, directory, tlist, true);
		return tlist;
	}

}
