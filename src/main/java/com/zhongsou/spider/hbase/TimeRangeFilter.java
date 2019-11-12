package com.zhongsou.spider.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;

import com.google.common.base.Preconditions;

public class TimeRangeFilter extends FilterBase {

	private long minTimeStamp = Long.MIN_VALUE;
	private long maxTimeStamp = Long.MAX_VALUE;

	public TimeRangeFilter(long minTimeStamp, long maxTimeStamp) {
		Preconditions.checkArgument(maxTimeStamp >= minTimeStamp, "max timestamp %s must be big than min timestamp %s", maxTimeStamp, minTimeStamp);
		this.maxTimeStamp = maxTimeStamp;
		this.minTimeStamp = minTimeStamp;
	}

	@Override
	public ReturnCode filterKeyValue(KeyValue v) {
		if (v.getTimestamp() >= minTimeStamp && v.getTimestamp() <= maxTimeStamp) {
			return ReturnCode.INCLUDE;
		} else if (v.getTimestamp() < minTimeStamp) {
			// The remaining versions of this column are guaranteed
			// to be lesser than all of the other values.
			return ReturnCode.NEXT_COL;
		}
		return ReturnCode.SKIP;
	}

	public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
		long minTime, maxTime;
		if (filterArguments.size() < 2)
			return null;
		minTime = ParseFilter.convertByteArrayToLong(filterArguments.get(0));
		maxTime = ParseFilter.convertByteArrayToLong(filterArguments.get(1));
		return new TimeRangeFilter(minTime, maxTime);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(minTimeStamp);
		out.writeLong(maxTimeStamp);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.minTimeStamp = in.readLong();
		this.maxTimeStamp = in.readLong();
	}

}
