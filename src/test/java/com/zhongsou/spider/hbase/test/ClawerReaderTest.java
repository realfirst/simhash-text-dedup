package com.zhongsou.spider.hbase.test;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.hadoop.ClawerFileReader;

public class ClawerReaderTest {
	public static void main(String args[]) {
		String uri = null;
		if (args.length == 0)
			uri = "file:////home/dape/clawer_201207191515_part-00011";
		else
			uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			FileStatus status = fs.getFileStatus(path);
		
			System.out.println("path len=" + status.getLen());
			FileSplit split = new FileSplit(path, 0L, status.getLen(), new String[0]);
			ClawerFileReader<Text, Text> reader = new ClawerFileReader<Text, Text>(conf, split);

			Text key = (Text) ReflectionUtils.newInstance(Text.class, conf);
			int convertFaied = 0;
			Text value = (Text) ReflectionUtils.newInstance(Text.class, conf);
			while (reader.next(key, value)) {
				byte[] linebytes = value.getBytes();
				int len;
				int length = linebytes.length;
//				if (length != value.getLength()) {
//					System.out.println("error convert,length="+length+"\t value length="+value.getLength());
//				}
				int times = 0;
				if (linebytes[0] == '0' || linebytes[0] == '2') {
					continue;
				}
				convertFaied++;
				int packet = NumberUtil.readInt(linebytes, 1);

				for (int offset = 5; offset + 4 < length && times < packet;) {
					len = NumberUtil.readInt(linebytes, offset);
					System.out.println("offset=" + offset + "len=" + len);
					offset += 4;

					offset += len;
					times++;
				}
			}
			reader.close();
			System.out.println("convert failed num=" + convertFaied);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
