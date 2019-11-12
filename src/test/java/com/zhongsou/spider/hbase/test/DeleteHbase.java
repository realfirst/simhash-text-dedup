package com.zhongsou.spider.hbase.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.StringUtils;

import com.zhongsou.spider.common.util.NumberUtil;

public class DeleteHbase {
	HTable table;
	File folder;

	public DeleteHbase(String fileName) {
		this.folder = new File(fileName);
		Configuration conf = HBaseConfiguration.create();
		try {
			table = new HTable(conf, "webDB");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void deleteRecord() {
		File f[] = folder.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				// TODO Auto-generated method stub
				if (name.startsWith("part"))
					return true;
				else
					return false;
			}

		});
		List<Delete> tlist = new LinkedList<Delete>();
		Configuration conf = new Configuration();
		FileSystem fs;
		ImmutableBytesWritable key = new ImmutableBytesWritable();
		NullWritable a = NullWritable.get();
		try {
			fs = FileSystem.get(conf);
			int i = 0;
			// SequenceFile.Reader reader = null;
			BufferedReader reader = null;
			for (File tempFile : f) {
				System.out.println(tempFile.getAbsolutePath());
				// reader = new SequenceFile.Reader(fs, new Path("file:////" +
				// tempFile.getAbsolutePath()), conf);
				reader = new BufferedReader(new InputStreamReader(
						new FileInputStream(tempFile.getAbsoluteFile())));
				// while (reader.next(key, a)) {
				// Delete d = new Delete(key.get());
				// tlist.add(d);
				// if (tlist.size() > 1000) {
				// this.table.delete(tlist);
				// // System.out.println(NumberUtil.getHexString(key.get()));
				// i+=tlist.size();
				// tlist.clear();
				//
				// System.out.println("delete"+i);
				// }
				// }

				String line = null;
				byte rowkey[] = null;
				while ((line = reader.readLine()) != null) {
					String s[] = line.split("\t");
					if (s[0].trim().length() == 16) {
						rowkey = StringUtils.hexStringToByte(s[0]);
						if (rowkey.length == 8) {
							Delete d = new Delete(rowkey);
							tlist.add(d);
						}
					}
					if (tlist.size() > 1000) {
						System.out.println("delete one list begin,docid="+NumberUtil.getHexString(tlist.get(0).getRow()));
						this.table.delete(tlist);
						tlist.clear();
						System.out.println("delete one list end");
					}
				}

				this.table.delete(tlist);

				IOUtils.closeStream(reader);

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		DeleteHbase d = new DeleteHbase(args[0]);
		d.deleteRecord();
	}

}
