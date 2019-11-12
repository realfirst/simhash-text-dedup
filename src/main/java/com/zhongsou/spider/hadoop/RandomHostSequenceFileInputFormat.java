package com.zhongsou.spider.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.net.NetworkTopology;
import org.mortbay.log.Log;

public class RandomHostSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {

	private static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
	private static final double SPLIT_SLOP = 1.1;
	private long minSplitSize = 1;

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		// TODO Auto-generated method stub

		FileStatus[] files = listStatus(job);
		FileSystem dfs = FileSystem.get(job);
		List<String> hostlist = new LinkedList<String>();
		if (dfs instanceof DistributedFileSystem) {
			DistributedFileSystem disfs = (DistributedFileSystem) dfs;
			DatanodeInfo info[] = disfs.getDataNodeStats();
			for (DatanodeInfo datainfo : info) {
				hostlist.add(datainfo.getHostName());
			}
		}
		Log.info("get random host list size=" + hostlist.size());
		Random rand = new Random();
		// Save the number of input files in the job-conf
		job.setLong(NUM_INPUT_FILES, files.length);
		long totalSize = 0; // compute total size
		for (FileStatus file : files) { // check we have valid files
			if (file.isDir()) {
				throw new IOException("Not a file: " + file.getPath());
			}
			totalSize += file.getLen();
		}

		long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
		long minSize = Math.max(job.getLong("mapred.min.split.size", 1), minSplitSize);

		// generate splits
		ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
		NetworkTopology clusterMap = new NetworkTopology();
		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job);
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
			if ((length != 0) && isSplitable(fs, path)) {
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(goalSize, minSize, blockSize);

				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {

					if (hostlist.size() > 0) {
						String splitHosts[] = new String[1];
						splitHosts[0] = hostlist.get(rand.nextInt(hostlist.size()));
						splits.add(new FileSplit(path, length - bytesRemaining, splitSize, splitHosts));
					} else {
						String[] splitHosts = getSplitHosts(blkLocations, length - bytesRemaining, splitSize, clusterMap);
						splits.add(new FileSplit(path, length - bytesRemaining, splitSize, splitHosts));
					}

					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					if (hostlist.size() > 0) {
						String splitHosts[] = new String[1];
						splitHosts[0] = hostlist.get(rand.nextInt(hostlist.size()));
						splits.add(new FileSplit(path, length - bytesRemaining, splitSize, splitHosts));
					} else {
						splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, blkLocations[blkLocations.length - 1].getHosts()));
					}
				}
			} else if (length != 0) {
				if (hostlist.size() > 0) {
					String splitHosts[] = new String[1];
					splitHosts[0] = hostlist.get(rand.nextInt(hostlist.size()));
					splits.add(new FileSplit(path, 0, length, splitHosts));
				} else {
					String[] splitHosts = getSplitHosts(blkLocations, 0, length, clusterMap);
					splits.add(new FileSplit(path, 0, length, splitHosts));
				}
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}
		LOG.debug("Random host Total # of splits: " + splits.size());
		return splits.toArray(new FileSplit[splits.size()]);
	}

	@Override
	protected String[] getSplitHosts(BlockLocation[] blkLocations, long offset, long splitSize, NetworkTopology clusterMap) throws IOException {
		// TODO Auto-generated method stub
		return super.getSplitHosts(blkLocations, offset, splitSize, clusterMap);
	}

}
