package com.zhongsou.spider.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MultiTableRecordReader implements RecordReader<ImmutableBytesWritable, Result> {
	byte lastSuccessful[] = null;
	private TableRecordReaderImpl recordReaderImpl = new TableRecordReaderImpl();

	@Override
	public boolean next(ImmutableBytesWritable key, Result value) throws IOException {
		// TODO Auto-generated method stub
		boolean t;
		try {
			t = recordReaderImpl.nextKeyValue();
			if (t) {
				ImmutableBytesWritable k = recordReaderImpl.getCurrentKey();
				Result v = recordReaderImpl.getCurrentValue();
				key.set(k.get());
				lastSuccessful = k.get();
				Writables.copyWritable(v, value);
			}
			return t;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("hbase next key throws a excepiton e retry");
			e.printStackTrace();
			recordReaderImpl.restart(lastSuccessful);
			try {
				t = recordReaderImpl.nextKeyValue();
				if (t) {
					ImmutableBytesWritable k = recordReaderImpl.getCurrentKey();
					Result v = recordReaderImpl.getCurrentValue();
					key.set(k.get());
					lastSuccessful = k.get();
					Writables.copyWritable(v, value);
				}
				System.out.println("retry " + t);
				return t;
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (Exception e1) {
				return false;
			}
			System.out.println("retry failed");
			return false;
		}

		return false;
	}

	public void setHTable(HTable htable) {
		recordReaderImpl.setHTable(htable);
	}

	public void setScan(Scan scan) {
		this.recordReaderImpl.setScan(scan);
	}

	public void init(InputSplit inputsplit) throws IOException, InterruptedException {
		this.recordReaderImpl.initialize(inputsplit,null);
	}

	@Override
	public ImmutableBytesWritable createKey() {
		// TODO Auto-generated method stub
		return new ImmutableBytesWritable();
	}

	@Override
	public Result createValue() {
		// TODO Auto-generated method stub
		return new Result();
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		this.recordReaderImpl.close();
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
