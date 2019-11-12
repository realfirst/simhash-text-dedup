package com.zhongsou.spider.hadoop;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

import com.zhongsou.spider.common.util.NumberUtil;

public class ClawerFileReader<K, V> extends SequenceFileRecordReader<K, V> {
	private final Log LOG = LogFactory.getLog(FileInputFormat.class);
	ClawerConvert clawerConvert;
	private final static long MAX_CONVERT_LOG_TIME = 20;

	public ClawerFileReader(Configuration conf, FileSplit split) throws IOException {
		super(conf, split);
		LOG.error("new clawer record reader");
		// TODO Auto-generated constructor stub
		this.clawerConvert = new ClawerConvert();

	}

	@Override
	public synchronized boolean next(K key, V val) throws IOException {
		// TODO Auto-generated method stub
		boolean t = super.next(key, val);

		if (t == false)
			return t;
		Text v = (Text) val;
		Text k = (Text) key;
		byte[] b = v.getBytes();
		byte[] newContent = null;
		// 下载成功
		if (b[0] == '1') {
			int packetNum = NumberUtil.readInt(b, 1);
			int packetLen = 0, offset = 5;
			int headerLen = 0, contentLen = 0;
			int urlLen = 0;
			int urloffset = 0;
			if (packetNum == 3) {
				for (int i = 0; i < 2; i++) {
					if (i == 1) {
						urlLen = NumberUtil.readInt(b, offset);
						urloffset = offset + 4;
					}
					packetLen = NumberUtil.readInt(b, offset);
					offset += packetLen + 4;
				}
				contentLen = NumberUtil.readInt(b, offset);
				if (contentLen < 0) {
					LOG.info("error packet");
					return true;
				}
				this.clawerConvert.reset(k.getBytes(), b, headerLen, contentLen, offset + 4, offset + 4);

			} else if (packetNum == 4) {
				for (int i = 0; i < 2; i++) {
					if (i == 1) {
						urlLen = NumberUtil.readInt(b, offset);
						urloffset = offset + 4;
					}
					packetLen = NumberUtil.readInt(b, offset);
					offset += packetLen + 4;
				}

				headerLen = NumberUtil.readInt(b, offset);
				contentLen = NumberUtil.readInt(b, offset + 4 + headerLen);
				this.clawerConvert.reset(k.getBytes(), b, headerLen, contentLen, offset + 4 + headerLen + 4, offset + 4);
			} else {
				LOG.error("error packet num=" + packetNum);
				return true;
			}
			long t1 = System.currentTimeMillis();
			newContent = this.clawerConvert.convert();
			long t2 = System.currentTimeMillis();
			if (t2 - t1 > MAX_CONVERT_LOG_TIME) {
				LOG.error("convert data format to slow,url=" + new String(b, urloffset, urlLen) + " time=" + (t2 - t1) + "\tcontent length=" + contentLen);
			}
			if (newContent != null) {
				byte newbyte[] = new byte[offset + newContent.length];
				newbyte[0] = '2';
				packetNum = 6;
				byte numb[] = NumberUtil.convertIntToC(packetNum);
				System.arraycopy(numb, 0, newbyte, 1, 4);
				System.arraycopy(b, 5, newbyte, 5, offset - 5);
				System.arraycopy(newContent, 0, newbyte, offset, newContent.length);
				v.set(newbyte, 0, newbyte.length);
			} else {
				// 转化失败
				LOG.error("convert data format failed,url=" + new String(b, urloffset, urlLen) + "\turllen=" + urlLen + "\toffset=" + urloffset + "\tlength=" + v.getLength());
				b[0] = '3';
				v.set(b, 0, v.getLength());
			}

			return true;
		}
		return true;

	}
}
