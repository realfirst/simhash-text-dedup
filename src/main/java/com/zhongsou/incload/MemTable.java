package com.zhongsou.incload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Describe class <code>MemTable</code> here. the class contians buildTable and
 * buildExtTable method
 * 
 * @author <a href="mailto:dingje@zhongsou.com">Ding Jingen</a>
 * @version 1.0
 */
public class MemTable {

	static final int URLID_LEN = 8;
	static final int FINGER_LEN = 8;
	static final int FINGER_PREFIX_LEN = 2;
	static final int FINGER_SUFFIX_LEN = 6;
	static final int TABLE_SIZE = 4;
	static final int URLID_FINGERSUFFIX_BYTE_SIZE = 14;
	static final int TABLE_CAPACITY = (int) Math.pow(2, 16);
	private static byte[][] permutedFinger = null;

	private Map<Character, Integer> slotMap0 = null;
	private Map<Character, Integer> slotMap1 = null;
	private Map<Character, Integer> slotMap2 = null;
	private Map<Character, Integer> slotMap3 = null;

	List<Map<Character, Integer>> smList = null;

	// private static Map<ImmutableBytesWritable, Integer> fingerCounterMap =
	// new HashMap<ImmutableBytesWritable, Integer>(2 *(int)Math.pow(10, 7));
	// private static Set<ImmutableBytesWritable> fingerSet = null ;

	FileSystem fs;
	Configuration conf;

	public MemTable(Configuration conf) {
		try {
			this.fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.conf = conf;
		slotMap0 = new HashMap<Character, Integer>(TABLE_CAPACITY);
		slotMap1 = new HashMap<Character, Integer>(TABLE_CAPACITY);
		slotMap2 = new HashMap<Character, Integer>(TABLE_CAPACITY);
		slotMap3 = new HashMap<Character, Integer>(TABLE_CAPACITY);
		smList = new ArrayList<Map<Character, Integer>>(4);
		smList.add(slotMap0);
		smList.add(slotMap1);
		smList.add(slotMap2);
		smList.add(slotMap3);
	}

	/**
	 * Describe <code>buildTable</code> method here.
	 * 
	 * @return a <code>Map<ImmutableBytesWritable,ImmutableBytesWritable></code>
	 *         value
	 * @exception IOException
	 *                if an error occurs
	 */
	public Map<ImmutableBytesWritable, ImmutableBytesWritable> buildTable(String rPath) throws IOException {

		SequenceFile.Reader reader = null;
		byte[] prefixArray = new byte[FINGER_PREFIX_LEN];
		Character prefix = null;
		Map<Character, Integer> prefixCountMap = null;
		try {
			Map<ImmutableBytesWritable, ImmutableBytesWritable> map = new HashMap<ImmutableBytesWritable, ImmutableBytesWritable>();
			ImmutableBytesWritable mapKey = null;
			byte[] mapKeyArray = null;
			ImmutableBytesWritable mapValue = null;
			byte[] mapValueArray = null;
			List<Path> paths = getPaths(rPath);
			for (Path path : paths) {
				if (reader != null) {
					IOUtils.closeStream(reader);
					reader = null;
				}
				reader = new SequenceFile.Reader(fs, path, conf); // Reads
																	// key/value
																	// pairs
																	// from
				// a sequence-format file
				ImmutableBytesWritable key = (ImmutableBytesWritable) // key --
																		// urlid
				ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				ImmutableBytesWritable value = (ImmutableBytesWritable) // value
																		// --
																		// finger
				ReflectionUtils.newInstance(reader.getValueClass(), conf);

				while (reader.next(key, value)) {
					mapKeyArray = new byte[URLID_LEN];
					System.arraycopy(key.get(), 0, mapKeyArray, 0, URLID_LEN);
					mapKey = new ImmutableBytesWritable(mapKeyArray);
					mapValueArray = new byte[FINGER_LEN];
					System.arraycopy(value.get(), 0, mapValueArray, 0, FINGER_LEN);
					mapValue = new ImmutableBytesWritable(mapValueArray);
					// Log.info("key:" + key + "\n" + "value:" + value);
					if (!map.containsKey(mapKey)) {
						map.put(mapKey, mapValue);

						// ****************************************************
						// fingerSet = fingerCounterMap.keySet();
						// if (fingerSet.contains(mapValue)) {
						// fingerCounterMap.put(mapValue, new
						// Integer(fingerCounterMap.get(mapValue) + 1));
						// } else {
						// fingerCounterMap.put(mapValue, new Integer(1));
						// }

						// ****************************************************

						for (int i = 0; i < TABLE_SIZE; i++) {
							System.arraycopy(mapValueArray, 2 * i, prefixArray, 0, FINGER_PREFIX_LEN);
							prefix = Character.valueOf(byteArrayToChar(prefixArray));
							prefixCountMap = smList.get(i);
							if (prefixCountMap.keySet().contains(prefix)) {
								prefixCountMap.put(prefix, Integer.valueOf(prefixCountMap.get(prefix) + 1));
							} else {
								prefixCountMap.put(prefix, Integer.valueOf(1));
							}
						}
					}
				}
			}
			return map;
		} finally {
			if (reader != null) {
				IOUtils.closeStream(reader);
			}
		}
	}

	public List<Map<Character, byte[]>> buildExtTable(Map<ImmutableBytesWritable, ImmutableBytesWritable> map) throws IOException {
		// Map<ImmutableBytesWritable, ImmutableBytesWritable> map =
		// buildTable();
		List<Map<Character, byte[]>> result = new ArrayList<Map<Character, byte[]>>(TABLE_SIZE);

		Map<Character, byte[]> tb0 = new HashMap<Character, byte[]>(TABLE_CAPACITY);
		result.add(tb0);
		Map<Character, byte[]> tb1 = new HashMap<Character, byte[]>(TABLE_CAPACITY);
		result.add(tb1);
		Map<Character, byte[]> tb2 = new HashMap<Character, byte[]>(TABLE_CAPACITY);
		result.add(tb2);
		Map<Character, byte[]> tb3 = new HashMap<Character, byte[]>(TABLE_CAPACITY);
		result.add(tb3);

		byte[] bkey = new byte[URLID_LEN];
		byte[] bvalue = new byte[FINGER_LEN];
		byte[] prefixArray = new byte[FINGER_PREFIX_LEN];
		byte[] suffixArray = new byte[FINGER_SUFFIX_LEN];
		Character prefix = null;
		byte[] urlidSuffixArray = null;
		Map<Character, byte[]> table = null;
		Map<Character, Integer> commonSuffixCountMap = null;
		int commonSuffixCount = 0;
		for (ImmutableBytesWritable key : map.keySet()) {
			System.arraycopy(key.get(), 0, bkey, 0, URLID_LEN);
			System.arraycopy(map.get(key).get(), 0, bvalue, 0, FINGER_LEN);
			for (int i = 0; i < TABLE_SIZE; i++) {
				System.arraycopy(bvalue, 2 * i, prefixArray, 0, FINGER_PREFIX_LEN);
				prefix = Character.valueOf(byteArrayToChar(prefixArray));
				table = result.get(i);
				commonSuffixCountMap = smList.get(i);
				commonSuffixCount = commonSuffixCountMap.get(prefix);
				if (table.keySet().contains(prefix)) {
					urlidSuffixArray = table.get(prefix);
				} else {
					urlidSuffixArray = new byte[commonSuffixCount * URLID_FINGERSUFFIX_BYTE_SIZE];
				}

				System.arraycopy(bkey, 0, urlidSuffixArray, urlidSuffixArray.length - commonSuffixCount * URLID_FINGERSUFFIX_BYTE_SIZE, URLID_LEN);
				if (i == 0) {
					System.arraycopy(bvalue, 2, urlidSuffixArray, (urlidSuffixArray.length - commonSuffixCount * URLID_FINGERSUFFIX_BYTE_SIZE) + URLID_LEN, FINGER_SUFFIX_LEN);

				} else if (i == 1) {
					System.arraycopy(bvalue, 0, suffixArray, 0, 2);
					System.arraycopy(bvalue, 4, suffixArray, 2, 4);
					System.arraycopy(suffixArray, 0, urlidSuffixArray, (urlidSuffixArray.length - commonSuffixCount * URLID_FINGERSUFFIX_BYTE_SIZE) + URLID_LEN, FINGER_SUFFIX_LEN);
				} else if (i == 2) {
					System.arraycopy(bvalue, 0, suffixArray, 0, 4);
					System.arraycopy(bvalue, 6, suffixArray, 4, 2);
					System.arraycopy(suffixArray, 0, urlidSuffixArray, (urlidSuffixArray.length - commonSuffixCount * URLID_FINGERSUFFIX_BYTE_SIZE) + URLID_LEN, FINGER_SUFFIX_LEN);
				} else {
					System.arraycopy(bvalue, 0, urlidSuffixArray, (urlidSuffixArray.length - commonSuffixCount * URLID_FINGERSUFFIX_BYTE_SIZE) + URLID_LEN, FINGER_SUFFIX_LEN);
				}
				commonSuffixCountMap.put(prefix, Integer.valueOf(commonSuffixCount - 1));
				table.put(prefix, urlidSuffixArray);
			}
		}
		return result;
	}

	private static List<Path> getPaths(String rPath) {
		List<Path> pl = new ArrayList<Path>();
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(rPath));

			for (int i = 0; i < status.length; i++) {
				pl.add(status[i].getPath());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return pl;
	}

	static char byteArrayToChar(byte[] prefix) {
		short sv = Bytes.toShort(prefix);
		if (sv < 0) {
			sv = (short) (sv + Math.abs(Short.MIN_VALUE));
		}
		return (char) (Bytes.toShort(prefix) + Math.abs(Short.MIN_VALUE));
	}

	/**
	 * Describe <code>permuteFinger</code> method here.
	 * 
	 * @param finger
	 *            a <code>byte</code> value
	 * @return a <code>byte[][]</code> value
	 */
	public static byte[][] permuteFinger(byte[] finger) {

		for (int i = 0; i < 3; i++) {
			permutedFinger[i][0] = finger[(i + 1) * 2];
			permutedFinger[i][1] = finger[(i + 1) * 2 + 1];
			for (int j = 2; j < 8; j++) {
				if (j == (i + 1) * 2) {
					permutedFinger[i][j] = finger[0];
					continue;
				}
				if (j == ((i + 1) * 2) + 1) {
					permutedFinger[i][j] = finger[1];
					continue;
				}
				permutedFinger[i][j] = finger[j];
			}
		}
		return permutedFinger;
	}

	private static final long MEGABYTE = 1024L * 1024L;

	public static long bytesToMegabytes(long bytes) {
		return bytes / MEGABYTE;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
	}
	// public static void main(String[] args)
	// throws Exception {

	// // for (Path path : getPaths()) {
	// // System.out.println("" + path);
	// // }
	// Runtime runtime = Runtime.getRuntime();

	// long start = runtime.totalMemory();
	// Map<ImmutableBytesWritable, ImmutableBytesWritable> tablemap =
	// buildTable(URIC);
	// Log.info("the size of the memtable is:" + tablemap.size());

	// int i = 0, j = 0;
	// for (Map<Character, Integer> table : smList) {
	// System.out.println("for table" + i);
	// System.out.println("____________________________________________________________");
	// for (Character prefix : table.keySet()) {
	// System.out.println("the " + j + " of " + i + " table| prefix: " + "->" +
	// table.get(prefix));
	// j++;
	// }
	// i++;
	// j = 0;
	// }
	// for (ImmutableBytesWritable key : tablemap.keySet()) {
	// System.out.println("key:" + StringUtils.byteToHexString(key.get()) + "\t"
	// +
	// "value:" + StringUtils.byteToHexString(tablemap.get(key).get()));
	// }
	// Log.info("build table finishing...");

	// List<Map<ImmutableBytesWritable, List<byte[]>>> testlist =
	// MemTable.buildExtTable(tablemap);

	// List<byte[]> pilist = null;

	// for (Map<ImmutableBytesWritable, List<byte[]>> map : testlist) {
	// for (ImmutableBytesWritable comm : map.keySet()) {
	// System.out.println("common perfix: " +
	// StringUtils.byteToHexString(comm.get()));
	// pilist = map.get(comm);
	// for (byte[] pi : pilist) {
	// System.out.println("suffix: " +
	// StringUtils.byteToHexString(Arrays.copyOfRange(pi, 8, 14)));
	// }
	// }
	// }
	// List<Map<Character, byte[]>> extTable = MemTable.buildExtTable(tablemap);
	// for (int k = 0; k < extTable.size(); k++) {
	// for (Character prefix : extTable.get(k).keySet()) {
	// System.out.println("prefix:" + prefix +
	// "urlidsuffix:" +
	// StringUtils.byteToHexString(extTable.get(k).get(prefix)));
	// }
	// }

	// long finish = runtime.totalMemory();
	// // runtime.gc();
	// System.out.println(bytesToMegabytes(runtime.totalMemory()));
	// System.out.println(bytesToMegabytes(runtime.freeMemory()));
	// System.out.println(bytesToMegabytes(runtime.maxMemory()));
	// long memory = finish - start;
	// System.out.println("used memory is bytes: " + memory);
	// System.out.println("used memory is megabytes: " +
	// bytesToMegabytes(memory));
	// }
}
