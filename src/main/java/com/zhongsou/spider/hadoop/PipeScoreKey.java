package com.zhongsou.spider.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import com.zhongsou.spider.common.util.NumberUtil;

public class PipeScoreKey implements WritableComparable<PipeScoreKey>, Cloneable {
	byte buffer[];

	public byte[] getBuffer() {
		return buffer;
	}

	public void setBuffer(byte[] buffer) {
		this.buffer = buffer;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeVInt(out, buffer.length);
		out.write(buffer);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		int numBytes = WritableUtils.readVInt(in);
		if (numBytes < 0) {
			System.out.println("bytes=" + numBytes);
		}

		this.buffer = new byte[numBytes];
		in.readFully(buffer);
		// System.out.println("read a object from stream\t" +
		// text.toString().substring(8));
	}

	static { // register this comparator
		WritableComparator.define(PipeScoreKey.class, new PipesScoreComparator());
	}

	public String toString() {
		return NumberUtil.getHexString(this.buffer);
	}

	@Override
	public int compareTo(PipeScoreKey o) {
		// TODO Auto-generated method stub

		byte a[] = this.buffer;
		byte b[] = o.getBuffer();

		int cmp = WritableComparator.compareBytes(a, 0, 8, b, 0, 8);
		if (cmp != 0) {
			return cmp;
		} else {
			// 将主评分放到前面
			cmp = WritableComparator.compareBytes(a, 8, 1, b, 8, 1);
			if (cmp != 0)
				return -cmp;
			else {
				// 读取出评分,倒序
				float f1 = Math.abs(NumberUtil.readFloat(a, 17));
				float f2 = Math.abs(NumberUtil.readFloat(b, 17));
//				System.out.println("key compare f1=" + f1 + "\t" + f2);
				// 比绝对值
				return f1 > f2 ? -1 : ((f1 < f2) ? 1 : 0);

			}

		}
	}
}
