package com.zhongsou.spider.hbase;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

public class MultiTableInputFormat implements
		InputFormat<ImmutableBytesWritable, Result>, JobConfigurable {
	private final Log LOG = LogFactory.getLog(TableInputFormat.class);

	/** Job parameter that specifies the input table. */
	public static final String INPUT_TABLE = "hbase.mapreduce.inputtables";
	/**
	 * Base-64 encoded scanner. All other SCAN_ confs are ignored if this is
	 * specified. See {@link TableMapReduceUtil#convertScanToString(Scan)} for
	 * more details.
	 */
	public static final String SCAN = "hbase.mapreduce.scan";
	/** Column Family to Scan */
	public static final String SCAN_COLUMN_FAMILY = "hbase.mapreduce.scan.column.family";
	/** Space delimited list of columns to scan. */
	public static final String SCAN_COLUMNS = "hbase.mapreduce.scan.columns";
	/** The timestamp used to filter columns with a specific timestamp. */
	public static final String SCAN_TIMESTAMP = "hbase.mapreduce.scan.timestamp";
	/**
	 * The starting timestamp used to filter columns with a specific range of
	 * versions.
	 */
	public static final String SCAN_TIMERANGE_START = "hbase.mapreduce.scan.timerange.start";
	/**
	 * The ending timestamp used to filter columns with a specific range of
	 * versions.
	 */
	public static final String SCAN_TIMERANGE_END = "hbase.mapreduce.scan.timerange.end";
	/** The maximum number of version to return. */
	public static final String SCAN_MAXVERSIONS = "hbase.mapreduce.scan.maxversions";
	/** Set to false to disable server-side caching of blocks for this scan. */
	public static final String SCAN_CACHEBLOCKS = "hbase.mapreduce.scan.cacheblocks";
	/** The number of rows for caching that will be passed to scanners. */
	public static final String SCAN_CACHEDROWS = "hbase.mapreduce.scan.cachedrows";

	public static final String SCAN_FILTER_STRING = "hbase.mapreduce.scan.filterstring";

	public static final String COLUMN_LIST = "hbase.mapred.tablecolumns";

	MultiTableRecordReader tableRecordReader;

	private HashMap<String, HTable> tableMap;
	private HashMap<String, Filter> filterMap;
	HashMap<String, String[]> inputColumnMap;
	private HashMap<String, long[]> timeRangeMap;
	int cachedRows;
	byte[] startRow = new byte[0];
	byte[] stopRow = new byte[0];

	/** The reader scanning the table, can be a custom one. */

	public void configure(JobConf job) {
		// Path[] tableNames = FileInputFormat.getInputPaths(job);
		tableMap = new HashMap<String, HTable>();
		String tableNames = job.get(INPUT_TABLE);
		filterMap = new HashMap<String, Filter>();
		inputColumnMap = new HashMap<String, String[]>();
		this.timeRangeMap = new HashMap<String, long[]>();
		String table[] = tableNames.split(",");
		this.cachedRows = job.getInt(SCAN_CACHEDROWS, 100);
		ParseFilter parseFilter = new ParseFilter();
		DateFormat format = new java.text.SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		for (String t : table) {
			t = t.trim();
			if (t != "" && t.length() == 0)
				continue;
			try {
				tableMap.put(t, new HTable(HBaseConfiguration.create(job), t));
				String colArg = job.get(t + "." + COLUMN_LIST);

				String[] colNames = colArg.split(" ");
				if (colNames != null && colNames.length > 0) {
					this.inputColumnMap.put(t, colNames);
					LOG.info("table " + t + " add column " + colArg);
				}
				String timeStampStart = job.get(t + "." + SCAN_TIMERANGE_START);
				String timeStampEnd = job.get(t + "." + SCAN_TIMERANGE_END);
				try {
					Date start = null, end = null;
					long a[] = new long[2];
					if (timeStampStart != null
							&& timeStampStart
									.matches("\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}")) {
						start = format.parse(timeStampStart);
						a[0] = start.getTime();
					} else {
						a[0] = Long.MIN_VALUE;
					}
					if (timeStampEnd != null
							&& timeStampEnd
									.matches("\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}")) {
						end = format.parse(timeStampEnd);
						a[1] = end.getTime();
					} else {
						a[1] = Long.MAX_VALUE;
					}
					if (start != null && end != null)
						LOG.info("table " + t + "+time range,start:"
								+ start.toLocaleString() + " end:"
								+ end.toLocaleString());
					this.timeRangeMap.put(t, a);
				} catch (Exception e) {
					e.printStackTrace();
				}

				LOG.info("time range info,start=" + timeStampStart + " end:"
						+ timeStampEnd);

				String rowFilterStr = job.get(t + "." + SCAN_FILTER_STRING);
				if (rowFilterStr != null && !rowFilterStr.equals("")) {
					try {
						Filter filter = parseFilter
								.parseFilterString(rowFilterStr.trim());
						filterMap.put(t, filter);
						LOG.info("table " + t
								+ " parse file string succeed,filter string="
								+ rowFilterStr);
					} catch (CharacterCodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						LOG.error("table " + t + " parse file string failed="
								+ rowFilterStr);
					}
				}

				String filterTime = job.get(t + ".hbase.filter.bytime");
				String filterColumn = job.get(t + ".hbase.filter.column");
				if (filterTime != null && !filterTime.equals("")
						&& filterColumn != null && !filterColumn.equals("")
						&& filterColumn.contains(":")) {
					int time = 0;
					if (filterTime.equals("now")) {
						time = (int) (System.currentTimeMillis() / 1000);
					} else if (filterTime
							.matches("\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}")) {
						Date ftime = format.parse(filterTime);
						time = (int) (ftime.getTime() / 1000);
					}

					if (time != 0) {
						String columAndF[] = filterColumn.split(":");
						SingleColumnValueFilter lff = new SingleColumnValueFilter(
								Bytes.toBytes(columAndF[0]),
								Bytes.toBytes(columAndF[1]),
								CompareFilter.CompareOp.LESS_OR_EQUAL,
								Bytes.toBytes(time));
						lff.setFilterIfMissing(false);
						lff.setLatestVersionOnly(true);
						Filter f=filterMap.get(t);
						FilterList list=new FilterList();
						if(f!=null)
						{
							list.addFilter(f);
						}
						list.addFilter(lff);
						filterMap.put(t, list);
						Date d = new Date(1l * time * 1000);
						LOG.info("table " + t + " add one time filter time="
								+ format.format(d));

					}

				}

			} catch (Exception e) {
				LOG.error(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * 
	 * 
	 * Test if the given region is to be included in the InputSplit while
	 * splitting the regions of a table.
	 * <p>
	 * This optimization is effective when there is a specific reasoning to
	 * exclude an entire region from the M-R job, (and hence, not contributing
	 * to the InputSplit), given the start and end keys of the same. <br>
	 * Useful when we need to remember the last-processed top record and revisit
	 * the [last, current) interval for M-R processing, continuously. In
	 * addition to reducing InputSplits, reduces the load on the region server
	 * as well, due to the ordering of the keys. <br>
	 * <br>
	 * Note: It is possible that <code>endKey.length() == 0 </code> , for the
	 * last (recent) region. <br>
	 * Override this method, if you want to bulk exclude regions altogether from
	 * M-R. By default, no region is excluded( i.e. all regions are included).
	 * 
	 * 
	 * @param startKey
	 *            Start key of the region
	 * @param endKey
	 *            End key of the region
	 * @return true, if this region needs to be included as part of the input
	 *         (default).
	 * 
	 */
	protected boolean includeRegionInSplit(final byte[] startKey,
			final byte[] endKey) {
		return true;
	}

	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		if (tableMap.size() == 0) {
			throw new IOException("No table was provided");
		}
		int allTableLen = 0;
		List<InputSplit> tlist = new LinkedList<InputSplit>();
		for (HTable table : this.tableMap.values()) {

			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}
			int count = 0;
			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}
				String regionLocation = table.getRegionLocation(
						keys.getFirst()[i]).getHostname();

				// determine if the given start an stop key fall into the region
				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
						.compareTo(startRow, keys.getSecond()[i]) < 0)
						&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
								keys.getFirst()[i]) > 0)) {
					byte[] splitStart = startRow.length == 0
							|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
							.getFirst()[i] : startRow;
					byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
							keys.getSecond()[i], stopRow) <= 0)
							&& keys.getSecond()[i].length > 0 ? keys
							.getSecond()[i] : stopRow;
					InputSplit split = new TableSplit(table.getTableName(),
							splitStart, splitStop, regionLocation);
					tlist.add(split);
					if (LOG.isInfoEnabled())
						LOG.info("getSplits: split -> " + (count++) + " -> "
								+ split + " table name"
								+ new String(table.getTableName()));
				}
			}
		}

		return tlist.toArray(new InputSplit[0]);
	}

	public RecordReader<ImmutableBytesWritable, Result> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		TableSplit tSplit = (TableSplit) split;
		MultiTableRecordReader trr = this.tableRecordReader;
		// if no table record reader was provided use default
		if (trr == null) {
			trr = new MultiTableRecordReader();
		}
		Scan sc = new Scan();
		sc.setStartRow(tSplit.getStartRow());
		sc.setStopRow(tSplit.getEndRow());
		sc.setMaxVersions(1);
		String table = Bytes.toString(tSplit.getTableName());
		String inputColumns[] = this.inputColumnMap.get(table);
		if (inputColumns == null || inputColumns.length == 0) {
			LOG.info("can not find any input columns for table " + table);
		}
		for (String cols : inputColumns) {
			if (cols.equals(""))
				continue;
			if (cols.contains(":")) {
				String f = cols.substring(0, cols.indexOf(":"));
				String col = cols.substring(cols.indexOf(":") + 1);
				if (f.length() > 0 && col.length() > 0) {
					sc.addColumn(Bytes.toBytes(f), Bytes.toBytes(col));
					LOG.info("Table " + table + " add columns family: " + f
							+ " column:" + col);
				} else {
					LOG.error("Table " + table + " bad format" + cols);
				}
			} else {
				sc.addFamily(Bytes.toBytes(cols));
				LOG.info("Table " + table + " add columns family: " + cols);
			}
		}

		long timerange[] = this.timeRangeMap.get(table);
		if (timerange != null && timerange.length == 2) {
			sc.setTimeRange(timerange[0], timerange[1]);
		}
		sc.setFilter(this.filterMap.get(table));
		sc.setCaching(cachedRows);
		trr.setScan(sc);
		trr.setHTable(this.tableMap.get(table));
		try {
			trr.init(null);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return trr;
	}

	public HashMap<String, HTable> getTableMap() {
		return tableMap;
	}

	public void setTableMap(HashMap<String, HTable> tableMap) {
		this.tableMap = tableMap;
	}

	public HashMap<String, Filter> getFilterMap() {
		return filterMap;
	}

	public void setFilterMap(HashMap<String, Filter> filterMap) {
		this.filterMap = filterMap;
	}

	public void validateInput(JobConf job) throws IOException {
		// expecting exactly one path
		String tableNames = job.get(INPUT_TABLE);

		if (tableNames == null || tableNames == "" || this.tableMap.size() == 0) {
			throw new IOException("could not connect to table '" + tableNames
					+ "'");
		}

		String tables[] = tableNames.split(",");
		for (String t : tables) {
			t = t.trim();
			String colArg = job.get(t + "." + COLUMN_LIST);
			if (colArg == null || colArg.length() == 0) {
				throw new IOException("expecting at least one column");
			}
		}
		// expecting at least one column

	}

}
