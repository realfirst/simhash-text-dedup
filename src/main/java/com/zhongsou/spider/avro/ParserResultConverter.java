package com.zhongsou.spider.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.log.Log;

import com.zhongsou.spider.bean.ParserResult;
import com.zhongsou.spider.bean.URLInfo;
import com.zhongsou.spider.common.url.URLFilterException;
import com.zhongsou.spider.common.url.URLFilters;
import com.zhongsou.spider.common.url.URLNormalizers;
import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.NumberUtil;
import com.zhongsou.spider.common.util.SpiderConfiguration;

public class ParserResultConverter {
  public static int MAX_ERROR_TIMES = 10;
  static Schema recordSchema;
  static Schema linkSchema;
  static List<Field> fieldList;
  static Properties p;
  static long oneHourTime =  60 * 60 * 1000;
  final static byte[] zero = new byte[0];
  GenericDatumReader<GenericRecord> recordReader = new GenericDatumReader<GenericRecord>(
      recordSchema);
  GenericDatumWriter<GenericArray<GenericRecord>> arrayWriter = new GenericDatumWriter<GenericArray<GenericRecord>>(
      linkSchema);
  BinaryEncoder arrayEncoder = null;
  BinaryDecoder recordDecoder = null;

  GenericRecord result = null;
  URLNormalizers urlNormalizer;
  URLFilters urlFilter;
  SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  byte newtimestamp[];
  Random rand = new Random();

  public ParserResultConverter(Configuration conf) {
    urlNormalizer = new URLNormalizers(conf);
    urlFilter = new URLFilters(conf);
    if (conf.get("urlnormalizer.regex.file") == null) {
      Log.info("can not load spider defaul config");
    }
  }

  public ParserResultConverter(int timestamp) {
    Configuration conf = SpiderConfiguration.create();
    urlNormalizer = new URLNormalizers(conf);
    urlFilter = new URLFilters(conf);
    if (conf.get("urlnormalizer.regex.file") == null) {
      Log.info("can not load spider defaul config");
    }
    this.newtimestamp = Bytes.toBytes(timestamp);
  }

  public ParserResultConverter(int timestamp,Configuration conf) {
    urlNormalizer = new URLNormalizers(conf);
    urlFilter = new URLFilters(conf);
    if (conf.get("urlnormalizer.regex.file") == null) {
      Log.info("can not load spider defaul config");
    }
    this.newtimestamp = Bytes.toBytes(timestamp);
  }

  static Map<String, Pair<byte[], byte[]>> nameMap = new HashMap<String, Pair<byte[], byte[]>>();
  static {
    try {
      InputStream input = ParserResultConverter.class
                          .getResourceAsStream("resource/parser_data.avro");
      Schema.Parser parser = new Schema.Parser();
      recordSchema = parser.parse(input);
      Field d = recordSchema.getField("parser_link");
      fieldList = recordSchema.getFields();
      linkSchema = d.schema();
      System.out.println(recordSchema.toString(true));
      input.close();
      input = ParserResultConverter.class
              .getResourceAsStream("resource/avrodata2hbase.properties");
      p = new Properties();
      p.load(input);
      Set<Entry<Object, Object>> set = p.entrySet();
      for (Entry entry : set) {
        String value = (String) (entry.getValue());
        value = value.trim();
        if (value.contains(" ")) {
          String a[] = value.split(" ");
          for (String f : a) {
            String b[] = f.split(":");
            if (b.length == 2) {
              byte[] family = Bytes.toBytes(b[0]);
              byte[] qualifier = Bytes.toBytes(b[1]);
              nameMap.put(f, new Pair<byte[], byte[]>(family,
                                                      qualifier));
              System.out.println(f);
            }
          }
        } else {
          String b[] = value.split(":");
          if (b.length == 2) {
            byte[] family = Bytes.toBytes(b[0]);
            byte[] qualifier = Bytes.toBytes(b[1]);
            nameMap.put(value, new Pair<byte[], byte[]>(family,
                                                        qualifier));
            System.out.println(value);
          }
        }
      }

      byte[] ffamily = Bytes.toBytes("F");
      byte[] ferror = Bytes.toBytes("e");
      nameMap.put("F:e", new Pair<byte[], byte[]>(ffamily, ferror));
      byte[] fstatus = Bytes.toBytes("s");
      nameMap.put("F:s", new Pair<byte[], byte[]>(ffamily, fstatus));
      byte[] nextDowntime = Bytes.toBytes("d");
      nameMap.put("F:d", new Pair<byte[], byte[]>(ffamily, nextDowntime));

      byte[] accessTime = Bytes.toBytes("a");
      nameMap.put("F:a", new Pair<byte[], byte[]>(ffamily, accessTime));

      ffamily = Bytes.toBytes("P");
      byte[] perror = Bytes.toBytes("er");
      nameMap.put("P:er", new Pair<byte[], byte[]>(ffamily, perror));

      byte[] pr = Bytes.toBytes("p");
      nameMap.put("P:p", new Pair<byte[], byte[]>(ffamily, pr));

      input.close();

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   *
   * 转化数据，成为hfile
   *
   * @param key
   * @param keylen
   * @param value
   * @param valueLen
   * @param kvList
   * @return
   */
  public ParserResult converter(byte key[], int keylen, byte[] value,
                                int valueLen, List<KeyValue> kvList) {
    ParserResult parseResult = new ParserResult();
    parseResult.setUrlMd5(key, keylen);
    recordDecoder = DecoderFactory.get()
                    .binaryDecoder(value, recordDecoder);
    try {
      // careful,此处不能重用record
      result = recordReader.read(null, this.recordDecoder);
      ByteBuffer status = (ByteBuffer) result.get("down_status");
      String s = new String(status.array());
      // s = s.trim();
      Log.info(s + "\t" + s.length() + "\t" + s.equals("2"));
      // 原数据的格式,old finger(8 int)+newURLToday(4 int)+modifyCount(4
      // int)+errorCount(4 int)+newURLbitmap(4 int)+modifyBitmap(4
      // int)+lastModifyTime(8 int)+no_change_count(4 int)

      // 下载失败
      if (s.equals("0")) {
        ByteBuffer meta = (ByteBuffer) result.get("down_meta");
        int number = NumberUtil.readInt(meta.array(), 16);
        number = number + 1;
        Pair<byte[], byte[]> pair = nameMap.get("F:e");
        KeyValue kv = new KeyValue(key, pair.getFirst(),
                                   pair.getSecond(), Bytes.toBytes(String.valueOf(number)));
        kvList.add(kv);

        // 大于最大的失败尝试次数，停止下载
        if (number > MAX_ERROR_TIMES) {
          Pair<byte[], byte[]> spair = nameMap.get("F:d");
          KeyValue skv = new KeyValue(key, spair.getFirst(),
                                      spair.getSecond(), Bytes.toBytes(Integer.MAX_VALUE));
          kvList.add(skv);
          Log.info("down load url failed ,key="
                   + NumberUtil.getHexString(key) + "\t failed times="
                   + number
                   + " exceed max failed times, stop download ");
        } else {
          Pair<byte[], byte[]> spair = nameMap.get("F:d");
          // max day =(1024+10)/24=43
          long nextTime = System.currentTimeMillis() + oneHourTime
                          *(10+ (long)(Math.pow(2l,number)));
          String str = dateformat
                       .format(new java.util.Date(nextTime));
          Log.info("down load url failed ,key="
                   + NumberUtil.getHexString(key) + "\t failed times="
                   + number + " next time  =" + (nextTime) + "\t"
                   + str);
          KeyValue skv = new KeyValue(key, spair.getFirst(),
                                      spair.getSecond(),
                                      Bytes.toBytes((int) (nextTime / 1000)));
          kvList.add(skv);

        }
        pair = nameMap.get("F:a");
        kv = new KeyValue(key, pair.getFirst(), pair.getSecond(),
                          NumberUtil.convertIntToC((int) (System.currentTimeMillis() / 1000)));
        kvList.add(kv);
      }
      // java文件类型转化失败
      else if (s.equals("3")) {
        ByteBuffer meta = (ByteBuffer) result.get("down_meta");
        ByteBuffer down_content = (ByteBuffer) (result
                                                .get("down_content"));
        String name = p.getProperty("down_content");
        Pair<byte[], byte[]> pair = nameMap.get(name);
        KeyValue kv = new KeyValue(key, pair.getFirst(),
                                   pair.getSecond(), down_content.array());
        int number = NumberUtil.readInt(meta.array(), 16);
        pair = nameMap.get("F:e");
        number = number + 1;
        // 增加下载的失败次数
        kv = new KeyValue(key, pair.getFirst(), pair.getSecond(),
                          Bytes.toBytes(String.valueOf(number)));
        kvList.add(kv);

        // 解析失败
        pair = nameMap.get("P:er");
        kv = new KeyValue(key, pair.getFirst(), pair.getSecond(),
                          Bytes.toBytes("1"));
        kvList.add(kv);
        // 大于最大的失败尝试次数，停止下载
        if (number > MAX_ERROR_TIMES) {
          Pair<byte[], byte[]> spair = nameMap.get("F:d");
          KeyValue skv = new KeyValue(key, spair.getFirst(),
                                      spair.getSecond(), Bytes.toBytes(Integer.MAX_VALUE));
          kvList.add(skv);
          Log.info("convert url failed ,key="
                   + NumberUtil.getHexString(key) + "\t failed times="
                   + number
                   + " exceed max failed times, stop download ");
        } else {
          Pair<byte[], byte[]> spair = nameMap.get("F:d");
          // max
          long nextTime = System.currentTimeMillis() + oneHourTime
                          *(long)(10+Math.pow(2,number));
          String str = dateformat
                       .format(new java.util.Date(nextTime));
          Log.info("convert url failed ,key="
                   + NumberUtil.getHexString(key) + "\t failed times="
                   + number + " next time  =" + (nextTime) + "\t"
                   + str);
          KeyValue skv = new KeyValue(key, spair.getFirst(),
                                      spair.getSecond(),
                                      Bytes.toBytes((int) (nextTime / 1000)));
          kvList.add(skv);
        }
        pair = nameMap.get("F:a");
        kv = new KeyValue(key, pair.getFirst(), pair.getSecond(),
                          NumberUtil.convertIntToC((int) (System
                                                          .currentTimeMillis() / 1000)));
        kvList.add(kv);

      }
      // 文本解析完成
      else if (s.equals("2")) {
        ByteBuffer meta = (ByteBuffer) result.get("down_meta");
        Log.info("meta length=" + meta.array().length + "\t"
                 + StringUtils.byteToHexString(meta.array()));
        parseResult.setMeta(meta.array());
        ByteBuffer pl = (ByteBuffer) result.get("parser_lang");
        byte lang[] = pl.array();
        int parseLang = 0;
        if (lang != null && lang.length > 0
            && pl.arrayOffset() < lang.length) {
          parseLang = lang[pl.arrayOffset()];
        }
        Pair<byte[], byte[]> pair = nameMap.get("P:p");

        // 临时方案，随机的pr，用于测试
        float pr = rand.nextFloat();
        KeyValue kv = new KeyValue(key, pair.getFirst(),
                                   pair.getSecond(), Bytes.toBytes(pr));
        kvList.add(kv);

        for (Field field : fieldList) {
          if (field.name().equals("down_meta")
              || field.name().equals("down_status")
              || field.name().equals("down_url"))
            continue;
          else if (field.name().equals("parser_link")) {
            GenericArray parser_link = (GenericArray) result
                                       .get("parser_link");
            ByteBuffer c = (ByteBuffer) result.get("down_encode");
            String charset = new String(c.array());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            arrayEncoder = EncoderFactory.get().binaryEncoder(out,
                                                              arrayEncoder);
            this.arrayWriter.write(parser_link, arrayEncoder);
            arrayEncoder.flush();
            pair = nameMap.get(p.get("parser_link"));
            kv = new KeyValue(key, pair.getFirst(),
                              pair.getSecond(), out.toByteArray());
            kvList.add(kv);

            int size = parser_link.size();

            // 只提取中文网页之中的链接?
            // 1==中文 2/其它
            if (parseLang == 1) {
              for (int i = 0; i < size; i++) {
                Object o = parser_link.get(i);
                if (o instanceof GenericRecord) {
                  ByteBuffer urlbuffer = (ByteBuffer) (((GenericRecord) o)
                                                       .get("url"));
                  //TODO 针对于某些网站的字符集识别错误的情况，这个转码有可能造成url错误
                  // 从解析过来的字符串的编码都是gbk，因此，针对于未编码的中文url，需要将转化
                  String url = new String(urlbuffer.array(),
                                          "gbk");
                  String srcurl = url;
                  try {
                    // 先进行url过滤，再进行标准化
                    url = this.urlFilter.filter(url);
                    url = this.urlNormalizer.normalize(url);

                    if (url != null
                        && url.trim().length() > 0) {
                      URLInfo info = new URLInfo();
                      info.setSrcURLmd5(key);
                      // 处理中文的url，用原网页的编码进行反编码
                      if (charset != null
                          && charset.length() > 0
                          && charset.length() < 5
                          && !(charset
                               .equalsIgnoreCase("utf8") || charset
                               .equalsIgnoreCase("utf-8"))) {
                        byte newurlbyte[] = url
                                            .getBytes(charset
                                                      .toLowerCase()
                                                      .trim());
                        String a = new String(
                            newurlbyte,
                            charset.toLowerCase()
                            .trim());
                        if (a.equals(url)) {
                          info.setUrl(newurlbyte);
                        } else {
                          Log.info("error url="
                                   + url);
                        }
                      } else {
                        info.setUrl(url.getBytes());
                      }
                      MD5 md5 = MD5.digest8(info
                                            .getUrl());
                      info.setUrlmd5(md5.getDigest());
                      parseResult.addURLInfo(info);

                    } else {
                      Log.debug("url " + srcurl
                                + " is filterd ");
                    }
                  } catch (URLFilterException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                  }
                }
              }
            }

          } else if (field.name().equals("simhash")) {
            // 处理simhash，需要填两个字段
            ByteBuffer hashbuffer = (ByteBuffer) result
                                    .get("simhash");
            parseResult.setFinger(hashbuffer.array(), 8);
            byte[] simhash = hashbuffer.array();
            if (simhash == null || simhash.length != 8) {
              Log.info("simhash length error " + simhash.length
                       + "\t url" + Bytes.toString(key));
            } else {
              // 分析出指纹，加上新的标记的时间戳
              pair = nameMap.get(p.get("parser_newflag"));
              kv = new KeyValue(key, pair.getFirst(),
                                pair.getSecond(), newtimestamp);
              kvList.add(kv);

              // String m = this.p.getProperty("simhash");
              // String a[] = m.split("\\s{1,}");
              // if (a != null || a.length > 0) {
              // for (String fa : a) {
              // //
              // F　family之中finger放到urlmeta的那个步骤之中进行一并导入,这里只导入P族的simhash
              // if (fa.equals("F:f")) {
              // continue;
              // }
              // pair = nameMap.get(fa);
              // kv = new KeyValue(key, pair.getFirst(),
              // pair.getSecond(),
              // hashbuffer.array());
              // kvList.add(kv);
              // }
              // }

              // simhash 不再立即入库，最后入库

            }
          } else {
            // 处理其它的域
            ByteBuffer infobuffer = (ByteBuffer) result.get(field
                                                            .name());
            if (infobuffer != null && infobuffer.array().length > 0) {
              pair = nameMap.get(p.getProperty(field.name()));
              if (pair != null) {
                kv = new KeyValue(key, pair.getFirst(),
                                  pair.getSecond(), infobuffer.array());
                kvList.add(kv);
              } else {
                Log.info("error "
                         + field.name()
                         + " can't find match hbase family and qualifier");
              }
            } else {
              pair = nameMap.get(p.getProperty(field.name()));
              if (pair != null) {
                kv = new KeyValue(key, pair.getFirst(),
                                  pair.getSecond(), zero);
                kvList.add(kv);
              } else {
                Log.info("error "
                         + field.name()
                         + " can't find match hbase family and qualifier");
              }
            }
          }
        }

      } else {
        Log.info("unkown error status" + s);
      }

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return parseResult;
  }

  public static void main(String args[]) {
    //  ParserResultConverter converter = new ParserResultConverter();

  }

}
