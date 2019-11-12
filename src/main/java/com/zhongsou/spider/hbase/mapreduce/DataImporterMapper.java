package com.zhongsou.spider.hbase.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zhongsou.spider.hbase.DataParser;
import com.zhongsou.spider.hbase.DataParser.PosAndOffset;

public class DataImporterMapper extends Mapper<Text, Text, ImmutableBytesWritable, Put> {
	private long ts;

	/** Column seperator */
	private String separator;
	final static String NAME = "import_data";
	final static Class DEFAULT_MAPPER = DataImporterMapper.class;
	final static String MAPPER_CONF_KEY = "import_data.mapper.class";
	final static String COLUMNS_CONF_KEY = "import_data.columns";
	final static String SKIP_LINES_CONF_KEY = "import_data.skip.bad.lines";
	final static String BULK_OUTPUT_CONF_KEY = "import_data.bulk.output";
	final static String SEPARATOR_CONF_KEY = "import_data.separator";
	final static String TIMESTAMP_CONF_KEY = "import_data.timestamp";
	final static String DEFAULT_SEPARATOR = ",";
	/** Should skip bad lines */
	private boolean skipBadLines;
	private Counter badLineCount;

	private DataParser parser;

	public long getTs() {
		return ts;
	}

	public boolean getSkipBadLines() {
		return skipBadLines;
	}

	public Counter getBadLineCount() {
		return badLineCount;
	}

	public void incrementBadLineCount(int count) {
		this.badLineCount.increment(count);
	}

	protected void setup(Context context) {
		doSetup(context);

		Configuration conf = context.getConfiguration();

		parser = new DataParser(conf.get(COLUMNS_CONF_KEY), separator);
	}

	/**
	 * Handles common parameter initialization that a subclass might want to
	 * leverage.
	 * 
	 * @param context
	 */
	protected void doSetup(Context context) {
		Configuration conf = context.getConfiguration();

		// If a custom separator has been used,
		// decode it back from Base64 encoding.
		separator = conf.get(SEPARATOR_CONF_KEY);
		if (separator == null) {
			separator = DEFAULT_SEPARATOR;
		} else {
			separator = new String(Base64.decode(separator));
		}

		ts = conf.getLong(TIMESTAMP_CONF_KEY, System.currentTimeMillis());

		skipBadLines = context.getConfiguration().getBoolean(SKIP_LINES_CONF_KEY, true);
		badLineCount = context.getCounter("ImportTsv", "Bad Lines");
	}

	public void map(Text key, Text value, Context context) throws IOException {
		byte[] lineBytes = value.getBytes();

		try {
			List<PosAndOffset> parsedList = parser.parse(lineBytes, value.getLength());
			System.out.println("lineBytes length="+lineBytes.length+" list size="+parsedList.size());
			if (parsedList.size() <= 1)
				return;
		//	PosAndOffset p = parsedList.get(0);
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(key.getBytes(),0, key.getLength());
			Put put = new Put(rowKey.copyBytes());
			for (int i = 0; i < parsedList.size(); i++) {
				PosAndOffset q = parsedList.get(i);
				KeyValue kv = new KeyValue(key.getBytes(),0, key.getLength(), parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0, parser.getQualifier(i).length, ts,
						KeyValue.Type.Put, lineBytes, q.getStart(), q.getLen());
				put.add(kv);
			}
			context.write(rowKey, put);
		} catch (IllegalArgumentException e) {
			if (skipBadLines) {
				System.err.println("Bad line at offset:  "+ e.getMessage());
				incrementBadLineCount(1);
				return;
			} else {
				throw new IOException(e);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException, ClassNotFoundException {

		// Support non-XML supported characters
		// by re-encoding the passed separator as a Base64 string.
		String actualSeparator = conf.get(SEPARATOR_CONF_KEY);
		if (actualSeparator != null) {
			conf.set(SEPARATOR_CONF_KEY, new String(Base64.encodeBytes(actualSeparator.getBytes())));
		}

		// See if a non-default Mapper was set
		String mapperClassName = conf.get(MAPPER_CONF_KEY);
		Class mapperClass = mapperClassName != null ? Class.forName(mapperClassName) : DEFAULT_MAPPER;

		String tableName = args[0];
		Path inputDir = new Path(args[1]);
		Job job = new Job(conf, NAME + "_" + tableName);
		job.setJarByClass(mapperClass);
		FileInputFormat.setInputPaths(job, inputDir);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(mapperClass);

		String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
		if (hfileOutPath != null) {
			HTable table = new HTable(conf, tableName);
			job.setReducerClass(PutSortReducer.class);
			Path outputDir = new Path(hfileOutPath);
			FileOutputFormat.setOutputPath(job, outputDir);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
			HFileOutputFormat.configureIncrementalLoad(job, table);
		} else {
			// No reducers. Just write straight to table. Call
			// initTableReducerJob
			// to set up the TableOutputFormat.
			TableMapReduceUtil.initTableReducerJob(tableName, null, job);
			job.setNumReduceTasks(0);
		}

		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.addDependencyJars(job.getConfiguration(), com.google.common.base.Function.class /*
																											 * Guava																							 */);
		return job;
	}

	/*
	 * @param errorMsg Error message. Can be null.
	 */
	private static void usage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			System.err.println("ERROR: " + errorMsg);
		}
		String usage = "Usage: " + NAME + " -Dimport_data.columns=a,b,c <tablename> <inputdir>\n" + "\n" + "Imports the given input directory of TSV data into the specified table.\n" + "\n"
				+ "The column names of the TSV data must be specified using the -Dimport_data.columns\n" + "option. This option takes the form of comma-separated column names, where each\n"
				+ "column name is either a simple column family, or a columnfamily:qualifier. The special\n" + "column name HBASE_ROW_KEY is used to designate that this column should be used\n"
				+ "as the row key for each imported record. You must specify exactly one column\n" + "to be the row key, and you must specify a column name for every column that exists in the\n"
				+ "input data.\n" + "\n" + "By default import_data will load data directly into HBase. To instead generate\n" + "HFiles of data to prepare for a bulk data load, pass the option:\n"
				+ "  -D" + BULK_OUTPUT_CONF_KEY + "=/path/for/output\n" + "  Note: if you do not use this option, then the target table must already exist in HBase\n" + "\n"
				+ "Other options that may be specified with -D include:\n" + "  -D" + SKIP_LINES_CONF_KEY + "=false - fail if encountering an invalid line\n" + "  '-D" + SEPARATOR_CONF_KEY
				+ "=|' - eg separate on pipes instead of tabs\n" + "  -D" + TIMESTAMP_CONF_KEY + "=currentTimeAsLong - use the specified timestamp for the import\n" + "  -D" + MAPPER_CONF_KEY
				+ "=my.Mapper - A user-defined Mapper to use instead of " + DEFAULT_MAPPER.getName() + "\n";

		System.err.println(usage);
	}

	/**
	 * Main entry point.
	 * 
	 * @param args
	 *            The command line parameters.
	 * @throws Exception
	 *             When running the job fails.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			usage("Wrong number of arguments: " + otherArgs.length);
			System.exit(-1);
		}

		// Make sure columns are specified
		String columns[] = conf.getStrings(COLUMNS_CONF_KEY);
		if (columns == null) {
			usage("No columns specified. Please specify with -D" + COLUMNS_CONF_KEY + "=...");
			System.exit(-1);
		}


		// Make sure one or more columns are specified
		if (columns.length < 2) {
			usage("One or more columns in addition to the row key are required");
			System.exit(-1);
		}

		Job job = createSubmittableJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
