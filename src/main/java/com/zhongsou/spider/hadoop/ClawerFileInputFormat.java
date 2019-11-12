package com.zhongsou.spider.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class ClawerFileInputFormat extends FileInputFormat<Text, Text> implements JobConfigurable {
	private final Log LOG = LogFactory.getLog(ClawerFileInputFormat.class);

	public ClawerFileInputFormat() {
		setMinSplitSize(SequenceFile.SYNC_INTERVAL);
		LOG.info("init clawer input format");
	}

	@Override
	protected FileStatus[] listStatus(JobConf job) throws IOException {
		LOG.info("list status");
		FileStatus[] files = super.listStatus(job);
		ArrayList<FileStatus> flist = new ArrayList<FileStatus>();
		for (int i = 0; i < files.length; i++) {
			FileStatus file = files[i];
			if (file.isDir()) {
				FileSystem fs = file.getPath().getFileSystem(job);
				FileStatus f[] = fs.listStatus(file.getPath());
				for (FileStatus fstatus : f) {
					if (fstatus.isDir()) {
						continue;
					} else {
						flist.add(fstatus);
					}
				}
			} else {
				flist.add(file);
			}
		}
		return flist.toArray(new FileStatus[0]);
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		LOG.info("create a new record reader");
		// TODO Auto-generated method stub
		return new ClawerFileReader<Text, Text>(job, (FileSplit) (split));
	}

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		LOG.info("start convert clawer file inputformat");
	}

}
