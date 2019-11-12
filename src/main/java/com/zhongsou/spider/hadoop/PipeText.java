package com.zhongsou.spider.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class PipeText implements WritableComparable<PipeText>, Cloneable {
	public Text getText() {
		return text;
	}

	public void setText(Text text) {
		this.text = text;
	}

	private Text text;

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		text.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.text = new Text();
		int numBytes = WritableUtils.readVInt(in);
		if (numBytes < 0) {
			System.out.println("bytes=" + numBytes);
		}
		byte[] buffer;
		buffer = new byte[numBytes];
		in.readFully(buffer);
		((Text) text).set(buffer);
		// System.out.println("read a object from stream\t" +
		// text.toString().substring(8));
	}

	static { // register this comparator
		WritableComparator.define(PipeText.class, new DoubleTextComparator());
	}

	public String toString() {
		return this.text.toString();
	}

	@Override
	public int compareTo(PipeText o) {
		// TODO Auto-generated method stub

		byte a[] = this.text.getBytes();
		byte b[] = o.text.getBytes();
		
		int cmp = WritableComparator.compareBytes(a, 0, 8, b, 0, 8);
		if (cmp != 0) {
			return cmp;
		}
		return WritableComparator.compareBytes(a, 8, 1, b, 8, 1);
	}

}
