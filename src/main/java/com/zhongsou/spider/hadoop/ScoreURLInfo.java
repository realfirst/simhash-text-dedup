package com.zhongsou.spider.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ScoreURLInfo implements WritableComparable<ScoreURLInfo>,
		Cloneable {

	private IntWritable allNum;
	private IntWritable newNum;

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		allNum=new IntWritable();
		newNum=new IntWritable();
		allNum.readFields(arg0);
		newNum.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		allNum.write(arg0);
		newNum.write(arg0);
	}

	public IntWritable getAllNum() {
		return allNum;
	}

	public void setAllNum(IntWritable allNum) {
		this.allNum = allNum;
	}

	public IntWritable getNewNum() {
		return newNum;
	}

	public void setNewNum(IntWritable newNum) {
		this.newNum = newNum;
	}

	@Override
	public int compareTo(ScoreURLInfo o) {
		// TODO Auto-generated method stub
		return allNum.compareTo(o.allNum);
	}

}
