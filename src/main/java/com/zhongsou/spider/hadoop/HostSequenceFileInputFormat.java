package com.zhongsou.spider.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.mortbay.log.Log;

/**
 * 
 * 指定host文件处理的sequence，文件的前部是host名，后面紧跟着三个下划线， 比如：hadoop-master-83___xxxxx
 * 
 * 将文件名的前一部分作是job的指定的host，从而节省regionserver的网络传输
 * 
 * 
 * @author Xi Qi
 * 
 * @param <K>
 * @param <V>
 */
public class HostSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {

	private static final double SPLIT_SLOP = 1.1;
	private static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		// TODO Auto-generated method stub

		boolean hostInFileName = job.getConfiguration().getBoolean("hostInFileName", false);
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job.getConfiguration());
			long length = file.getLen();
			String[] hosts = null;
			if (hostInFileName) {
				String name = path.getName();
				// 以3个下划线来进行分割
				String[] temp = name.split("___");
				if (temp.length > 0) {
					hosts = new String[1];
					hosts[0] = temp[0];
				}
			}
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
			
			if ((length != 0) && isSplitable(job, path)) {
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(blockSize, minSize, maxSize);

				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
					if (hostInFileName && hosts != null && hosts.length > 0) {
						Log.info(" part file " + path.toString() + " contains host " + hosts[0]);
						splits.add(new FileSplit(path, length - bytesRemaining, splitSize, hosts));
					} else {
						splits.add(new FileSplit(path, length - bytesRemaining, splitSize, blkLocations[blkIndex].getHosts()));
					}
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					if (hostInFileName && hosts != null && hosts.length > 0) {
						Log.info(" file end " + path.toString() + " contains host " + hosts[0]);
						splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, hosts));
					} else {
						splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, blkLocations[blkLocations.length - 1].getHosts()));
					}
				}
			} else if (length != 0) {

				if (hostInFileName && hosts != null && hosts.length > 0) {
					Log.info(" file  " + path.toString() + " contains host " + hosts[0]);
					splits.add(new FileSplit(path, 0, length, hosts));
				} else {
					splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
				}
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}

		// Save the number of input files in the job-conf
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

		Log.info("Host Sequence File Total # of splits: " + splits.size());
		return splits;
	}
}
