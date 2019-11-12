package com.zhongsou.spider.common.url;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.mortbay.log.Log;

import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.MD5.MD5Len;
import com.zhongsou.spider.common.util.NumberUtil;

public class RobotsFilter implements URLFilter {
	final static byte[] header = "########".getBytes();
	final static byte[] end = "@@@@@@@@".getBytes();
	File idxFile;
	RandomAccessFile robotFile;
	FileChannel roChannel;
	MappedByteBuffer roBuf;
	HashMap<MD5, OffAndLen> hostMap = new HashMap<MD5, OffAndLen>(100000);
	LRUCacheMap<MD5, RobotsProtocol> robotsMap = new LRUCacheMap<MD5, RobotsProtocol>(
			5000, 0.75f, true);
	final static long MAX_MAP_FILE_SIZE = 1024l * 1024 * 512;
	int max_lru_size;
	byte buffer[] = new byte[1024 * 1024];
	boolean useMemFile = false;

	static class OffAndLen {
		long offset;
		int len;

		public OffAndLen(long offset, int len) {
			this.offset = offset;
			this.len = len;
		}
	}

	class LRUCacheMap<K, V> extends LinkedHashMap<K, V> {

		public LRUCacheMap() {
			super();
			// TODO Auto-generated constructor stub
		}

		public LRUCacheMap(int initialCapacity, float loadFactor,
				boolean accessOrder) {
			super(initialCapacity, loadFactor, accessOrder);
			// TODO Auto-generated constructor stub
		}

		public LRUCacheMap(int initialCapacity, float loadFactor) {
			super(initialCapacity, loadFactor);
			// TODO Auto-generated constructor stub
		}

		public LRUCacheMap(int initialCapacity) {
			super(initialCapacity);
			// TODO Auto-generated constructor stub
		}

		public LRUCacheMap(Map<? extends K, ? extends V> m) {
			super(m);
			// TODO Auto-generated constructor stub
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean removeEldestEntry(Entry<K, V> eldest) {
			// TODO Auto-generated method stub
			if (size() > max_lru_size)
				return true;
			else
				return false;
		}

	};

	public int getWeakRobotsMapSize() {
		return this.robotsMap.size();
	}

	public int getRobotsMapSize() {
		return this.hostMap.size();
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.max_lru_size = conf.getInt("host_lru_size", 5000);
		String idxName = conf.get("robot_idx_name");
		String robotFileName = conf.get("robot_file_name");
		useMemFile = conf.getBoolean("use_memmap_file", true);
		if (idxName != null && !idxName.equals("") && robotFileName != null
				&& !robotFileName.equals("")) {
			idxFile = new File(idxName);
			try {
				robotFile = new RandomAccessFile(robotFileName, "r");
				this.roChannel = robotFile.getChannel();
				// 如果内存映射文件过大，不再进行内存映射，使用seek
				if (this.roChannel.size() > MAX_MAP_FILE_SIZE) {
					this.useMemFile = false;
				}
				if (this.useMemFile) {
					roBuf = this.roChannel.map(MapMode.READ_ONLY, 0,
							roChannel.size());
//					this.roBuf.load();
//					System.out.println("roBuf capacity" + roBuf.capacity());
				}

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

  public int getWeakRobotsMapSize() {
    return this.robotsMap.size();
  }
  public int getRobotsMapSize() {
    return this.hostMap.size();
  }

					for (int j = 0; j + 20 < i; j += 20) {
						try {
							MD5 md = new MD5(a, MD5Len.eight, j);
							long offset = NumberUtil.readLong(a, j + 8);
							int len = NumberUtil.readInt(a, j + 16);
							OffAndLen o = new OffAndLen(offset, len);
							hostMap.put(md, o);
							robotsMap.put(md, null);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

    public OffAndLen(long offset, int len) {
      this.offset = offset;
      this.len = len;
    }
  }

  @Override
  public void setConf(Configuration conf) {
    boolean useDistributeCache = conf.getBoolean("useDistributeCache", false);
    if (useDistributeCache) {
      String idxName = conf.get("robot_idx_name");
      String robotFileName = conf.get("robot_file_name");
      if (idxName != null && !idxName.equals("") &&
          robotFileName != null && !robotFileName.equals("")) {
        idxFile = new File(idxName);
        try {
          robotFile = new RandomAccessFile(robotFileName, "r");
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        }
      }
    } else {
      String idxName = conf.get("robot_idx_name");
      String robotFileName = conf.get("robot_file_name");
      if (idxName != null && !idxName.equals("") &&
          robotFileName != null && !robotFileName.equals("")) {
        idxFile = new File(idxName);
        try {
          robotFile = new RandomAccessFile(robotFileName, "r");
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        }
      }
    }

    if (idxFile != null && robotFile != null) {
      BufferedInputStream input = null;
      // 把索引加载到内存
      try {
        input = new BufferedInputStream(new FileInputStream(idxFile));
        byte a[] = new byte[4096];
        int i = 0;

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					input.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			Log.info("succeed load host map size"+hostMap.size());
		}

          for (int j = 0; j + 20 < i; j += 20) {
            try {
              MD5 md5 = new MD5(a, MD5Len.eight, j);
              long offset = NumberUtil.readLong(a, j + 8);
              int len = NumberUtil.readInt(a, j + 16);
              OffAndLen o = new OffAndLen(offset, len);
              Pair<OffAndLen, RobotsProtocol> p = new Pair<OffAndLen, RobotsProtocol>(o, null);
              // System.out
              // .println("md5="
              // + NumberUtil.getHexString(md
              // .getDigest()) + "\toffset="
              // + o.offset + "\tlen=" + o.len);
              robotsMap.put(md5, p);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }

	private RobotsProtocol loadFromMemFile(OffAndLen o) {
		RobotsProtocol res = null;
		try {
			this.roBuf.position((int) o.offset);
			if (o.len < 0 || o.offset < 0 || o.offset > this.robotFile.length()
					|| o.offset + o.len > this.robotFile.length()) {
				Log.info("read error off=" + o.offset + " len=" + o.len);
				return null;
			}
			// System.out.println("file size"+this.roChannel.size());
			roBuf.get(buffer, 0, o.len);
			byte a[] = buffer;
			// this.robotFile.readFully(a);

      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
  private RobotsProtocol loadFromFile(OffAndLen o) { // 根据偏移和长度加载文件
    RobotsProtocol res = null;
    try {
      this.robotFile.seek(o.offset);
      if (o.offset < 0 || o.len < 0 || 
          o.offset > this.robotFile.length() || 
          o.offset + o.len > this.robotFile.length()) {
        Log.info("read error off=" + o.offset + " len=" + o.len);
        return null;
      }
			RobotsProtocol protocol = new PrefixRobotsProtocol();
			if (protocol.init(robots.split("[\r\n]{1,}"))) {
				return protocol;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}

	
	/**
	 * 
	 * 使用文件通道的文件映射方式
	 * @param o
	 * @return
	 */
	private RobotsProtocol loadFromFile(OffAndLen o) {
		RobotsProtocol res = null;
		try {
			// this.robotFile.seek(o.offset);

			if (o.len > this.buffer.length) {
				buffer = new byte[o.len];
			}
			ByteBuffer buf = ByteBuffer.wrap(buffer, 0, o.len);
			int t = this.roChannel.read(buf, o.offset);
			if (o.len < 0 || o.offset < 0 || o.offset > this.robotFile.length()
					|| o.offset + o.len > this.robotFile.length() || t < o.len) {
				Log.info("read error off=" + o.offset + " len=" + o.len + "t="
						+ t);
				return null;
			}

			byte a[] = buf.array();
			// byte a[] = new byte[o.len];
			// this.roBuf.get(a, o.offset, o.len);
			// this.robotFile.readFully(a);

			assert (Bytes.compareTo(a, 0, 8, header, 0, 8) == 0);
			assert (Bytes.compareTo(a, o.len - 8, 8, end, 0, 8) == 0);
			int hostLen = NumberUtil.readInt(a, 16);
			int robotsLen = NumberUtil.readInt(a, 20 + hostLen);
			String robots = new String(a, hostLen + 24, robotsLen);
			RobotsProtocol protocol = new PrefixRobotsProtocol();
			if (protocol.init(robots.split("[\r\n]{1,}"))) {
				return protocol;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

      // assert(memcmp(robotsBuffer,"########",8)==0);
      // assert(memcmp(robotsBuffer+8,(void *)(&md5),8)==0);
      // assert(memcmp(robotsBuffer+len-8,"@@@@@@@@",8)==0);
      // int hostLen=*(int *)(robotsBuffer+16);
      // cout<<"hostLen="<<hostLen;
      //
      // snprintf(host,hostLen+1,"%s",robotsBuffer+20);
      // host[hostLen]=0;
      // fprintf(stdout,"key %ld host=%s\n",md5,host);
      // int robotsLen=*(int *)(robotsBuffer+20+hostLen);
      // string robots(robotsBuffer+24+hostLen,robotsLen);
      // printf("robots protocol=%s\n",robots.c_str());

	@Override
	public String filter(String urlString) {
		// TODO Auto-generated method stub
		if (urlString == null || urlString.equals(""))
			return null;
		RobotsProtocol protocol = null;
		try {
			URL url = new URL(urlString);
			String host = url.getHost();
			String path = url.getFile();

			MD5 md5 = MD5.digest8(host.getBytes());
			// 如果没有相应的robots协议
			if (!this.hostMap.containsKey(md5)) {
				return urlString;
			} else {
				assert (this.hostMap.containsKey(md5));
				if (!this.robotsMap.containsKey(md5)
						|| this.robotsMap.get(md5) == null) {
					// System.out.println("load host from file "+host);
					OffAndLen o = this.hostMap.get(md5);
					// 是否使用内存映射文件
					if (this.useMemFile) {
						protocol = this.loadFromMemFile(o);
					} else {
						protocol = this.loadFromFile(o);
					}
					this.robotsMap.put(md5, protocol);
				}
				protocol = this.robotsMap.get(md5);
				if (protocol != null) {
					if (protocol.checkURL(path)) {
						return urlString;
					} else {
						return null;
					}
				}
			}

  @Override
  public String filter(String urlString) {
    if (urlString == null || urlString.equals(""))
      return null;
    RobotsProtocol protocol = null;
    try {
      URL url = new URL(urlString);
      String host = url.getHost();
      String path = url.getFile();

      MD5 md5 = MD5.digest8(host.getBytes());
      // 如果没有相应的robots协议
      if (!this.robotsMap.containsKey(md5)) {
        return urlString;
      } else {
        Pair<OffAndLen, RobotsProtocol> o = this.robotsMap.get(md5);
        if (o.getSecond() == null) {
          protocol = this.loadFromFile(o.getFirst());
          if (protocol != null) {
            o.setSecond(protocol);
          }
        }

        protocol = o.getSecond();
        if (protocol != null) {
          if (protocol.checkURL(path)) {
            return urlString;
          } else {
            return null;
          }
        }
      }
    } catch (MalformedURLException e) {
      e.printStackTrace();
      return null;
    }

    return urlString;
  }

  public static void main(String args[]) {

  }

  @Override
  public Configuration getConf() {
    return this.getConf();
  }

}
