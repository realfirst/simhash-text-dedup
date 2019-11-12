package com.zhongsou.spider.hadoop;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.ProfilingHandler;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.txt.CharsetDetector;
import org.apache.tika.parser.txt.CharsetMatch;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.zhongsou.spider.common.util.MD5;
import com.zhongsou.spider.common.util.NumberUtil;

public class ClawerConvert {
	byte[] content;
	int contentStart;
	int headerStart;
	int headerLen;
	int contentLen;

	static Logger log = Logger.getLogger(ClawerConvert.class.getName());
	Tika tika;
	final static int MAX_FILE_LENGTH = (1 << 24);
	TikaConfig config;
	AutoDetectParser autoParser;
	Pattern pattern = Pattern.compile("([^\r\n]+)");

	public ClawerConvert() {
		config = TikaConfig.getDefaultConfig();
		tika = new Tika(config);
		autoParser = new AutoDetectParser(config);
	}

	public boolean reset(byte[] key, byte[] newContent, int headerLen,
			int contentLen, int contentStart, int headerStart) {
		if (contentLen > MAX_FILE_LENGTH) {
			MD5 md5;
			try {
				md5 = new MD5(key, MD5.MD5Len.eight, 0);
				log.error("cotent length to long,key=" + md5.getHexString());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return false;
		}
		this.content = newContent;
		this.headerLen = headerLen;
		this.contentLen = contentLen;
		this.contentStart = contentStart;
		this.headerStart = headerStart;
		return true;
	}

	public byte[] convert() {
		byte[] newContent = null;
		String mimeType = null;
		Metadata metadata = new Metadata();
		if (headerLen > 0) {
			String header = new String(content, headerStart, headerLen);
			String[] headers = header.split("[\r\n]{1,}");
			if (headers.length > 0) {
				String key, value;
				int i;
				for (String head : headers) {
					if ((i = head.indexOf(":")) != -1) {
						key = head.substring(0, i).trim();
						value = head.substring(i + 1).trim();
						metadata.add(key, value);
					}
				}
			}
		}

		// metadata.add("Content-Type", "iso-8859-1");
		Parser parser = config.getParser();
		CharsetDetector charsetDetector = new CharsetDetector();
		Detector mimeTypeDetector = config.getDetector();
		com.cybozu.labs.langdetect.Detector languageDetector = null;
		try {
			languageDetector = DetectorFactory.create();
			long start = System.currentTimeMillis();
			MediaType type = mimeTypeDetector.detect(new ByteArrayInputStream(
					content, this.contentStart, this.contentLen), metadata);
			long end = System.currentTimeMillis();
			// System.out.println("type identify time=" + (end - start));
			// 纯文本txt,html,xml等等，识别语言，转码

			if (type.getBaseType().getSubtype().matches("(?i).*html.*|.*rss.*")) {
				// System.out.println("text type=" +
				// type.getBaseType().getSubtype());
				String incomingCharset = metadata
						.get(Metadata.CONTENT_ENCODING);
				String incomingType = metadata.get(Metadata.CONTENT_TYPE);
				if (incomingCharset == null && incomingType != null) {
					// TIKA-341: Use charset in content-type
					MediaType mt = MediaType.parse(incomingType);
					if (mt != null) {
						incomingCharset = mt.getParameters().get("charset");
					}
				}

				if (incomingCharset != null) {
					try {
						charsetDetector.setDeclaredEncoding(incomingCharset);
					} catch (Exception e) {
						// System.out.println("incoming charset=" +
						// incomingCharset);
					}
				}
				long end1 = System.currentTimeMillis();
				charsetDetector.setText(new ByteArrayInputStream(content,
						this.contentStart, this.contentLen));
				for (CharsetMatch match2 : charsetDetector.detectAll()) {
					if (Charset.isSupported(match2.getName())) {
						metadata.set(Metadata.CONTENT_ENCODING,
								match2.getName());
						break;
					}
				}
				long chend = System.currentTimeMillis();

				String encoding = metadata.get(Metadata.CONTENT_ENCODING);
				if (encoding == null) {
					throw new TikaException(
							"Text encoding could not be detected and no encoding"
									+ " hint is available in document metadata");
				}
				String content_type = type.getBaseType().toString();

				String s = new String(content, contentStart, contentLen,
						encoding);
				s = s.replaceAll("[\r\n]{1,}", "");
				s = s.replaceAll("<script.*?</script>", "");
				s = s.replaceAll("<style.*?</style>", "");
				s = s.replaceAll("<[^<>]*?>", "");
				s = s.replaceAll("\\s{2,}", " ");
				languageDetector.append(s);
				String language = "en";
				try {
					language = languageDetector.detect();
				} catch (Exception e) {
					// e.printStackTrace();
				}

				// System.out.println("language=" + language + "\tcontent-type="
				// + content_type + "\tcontent_encoding=" + encoding);
				if (content_type == null)
					content_type = "";

				byte b_content[] = new String(content, this.contentStart,
						this.contentLen, encoding).getBytes("utf-8");
				byte b_type[] = content_type.getBytes();
				byte b_lange[] = language.getBytes();
				byte b_encoding[] = encoding.getBytes();

				// System.out.println("array length=" + b_content.length + "\t"
				// + b_type.length + "\t" + b_lange.length + "\t" +
				// b_encoding.length);
				byte temp[] = new byte[4 * 4 + b_type.length + b_lange.length
						+ b_encoding.length + b_content.length];
				int offset = 0;
				byte num[] = NumberUtil.convertIntToC(b_type.length);
				System.arraycopy(num, 0, temp, offset, 4);
				offset += 4;
				System.arraycopy(b_type, 0, temp, offset, b_type.length);
				offset += b_type.length;

				num = NumberUtil.convertIntToC(b_lange.length);
				System.arraycopy(num, 0, temp, offset, 4);
				offset += 4;
				System.arraycopy(b_lange, 0, temp, offset, b_lange.length);
				offset += b_lange.length;

				num = NumberUtil.convertIntToC(b_encoding.length);
				System.arraycopy(num, 0, temp, offset, 4);
				offset += 4;
				System.arraycopy(b_encoding, 0, temp, offset, b_encoding.length);
				offset += b_encoding.length;

				num = NumberUtil.convertIntToC(b_content.length);
				System.arraycopy(num, 0, temp, offset, 4);
				offset += 4;
				System.arraycopy(b_content, 0, temp, offset, b_content.length);
				offset += b_content.length;

				return temp;
			} else {
				// 其它格式，进行格式转化，识别语言，转码
				// System.out.println("other type=" +
				// type.getBaseType().getSubtype());
				StringWriter textBuffer = new StringWriter();
				ProfilingHandler phandler = new ProfilingHandler();
				long s = System.currentTimeMillis();
				ContentHandler handler = new TeeContentHandler(
						getTextContentHandler(textBuffer), phandler);
				ParseContext context = new ParseContext();
				autoParser.parse(new ByteArrayInputStream(content,
						this.contentStart, this.contentLen), handler, metadata,
						context);
				end = System.currentTimeMillis();
				// Log.info("parse succeed!time consumed=" + (end - s) +
				// " new content-length=" + textBuffer.toString().length());
				String convertContent = textBuffer.toString();
				convertContent = convertContent.replaceAll("[\r\n]{1,}", "");
				convertContent = convertContent.replaceAll(
						"<script.*?</script>", "");
				convertContent = convertContent.replaceAll("<style.*?</style>",
						"");
				convertContent = convertContent.replaceAll("<[^<>]*?>", "");
				convertContent = convertContent.replaceAll("\\s{2,}", " ");
				languageDetector.append(convertContent);
				String language = "en";
				try {
					language = languageDetector.detect();
				} catch (Exception e) {
					// e.printStackTrace();
				}
				StringBuffer buffer = new StringBuffer();
				String[] keys = metadata.names();
				for (String key : keys) {
					buffer.append(key + ":" + metadata.get(key) + "\r\n");
					// System.out.println("key=" + key + "\t" +
					// metadata.get(key));
				}
				String content_type = type.getBaseType().toString();

				String meta_info = buffer.toString();

				if (content_type == null)
					content_type = "";
				if (meta_info == null)
					meta_info = "";
				// String textContent = textBuffer.toString();

				byte b_content[] = textBuffer.toString().getBytes("utf-8");
				byte b_type[] = content_type.getBytes();
				byte b_lange[] = language.getBytes();
				byte b_meta_info[] = meta_info.getBytes("utf-8");

				byte temp[] = new byte[4 * 4 + b_type.length + b_lange.length
						+ b_meta_info.length + b_content.length];
				int offset = 0;
				byte num[] = NumberUtil.convertIntToC(b_type.length);
				System.arraycopy(num, 0, temp, offset, 4);
				offset += 4;
				System.arraycopy(b_type, 0, temp, offset, b_type.length);
				offset += b_type.length;

				num = NumberUtil.convertIntToC(b_lange.length);
				System.arraycopy(num, 0, temp, offset, 4);
				offset += 4;
				System.arraycopy(b_lange, 0, temp, offset, b_lange.length);
				offset += b_lange.length;

				num = NumberUtil.convertIntToC(b_meta_info.length);
				System.arraycopy(num, 0, temp, offset, 4);
				offset += 4;
				System.arraycopy(b_meta_info, 0, temp, offset,
						b_meta_info.length);
				offset += b_meta_info.length;

				num = NumberUtil.convertIntToC(b_content.length);
				System.arraycopy(num, 0, temp, offset, 4);
				offset += 4;
				System.arraycopy(b_content, 0, temp, offset, b_content.length);
				offset += b_content.length;

				return temp;
			}
		} catch (Exception e) {

			e.printStackTrace();
		}

		return newContent;
	}

	private ContentHandler getHtmlHandler(Writer writer)
			throws TransformerConfigurationException {
		SAXTransformerFactory factory = (SAXTransformerFactory) SAXTransformerFactory
				.newInstance();
		TransformerHandler handler = factory.newTransformerHandler();
		handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "html");
		handler.setResult(new StreamResult(writer));
		return new ContentHandlerDecorator(handler) {
			@Override
			public void startElement(String uri, String localName, String name,
					Attributes atts) throws SAXException {
				if (XHTMLContentHandler.XHTML.equals(uri)) {
					uri = null;
				}
				if (!"head".equals(localName)) {
					if ("img".equals(localName)) {
						AttributesImpl newAttrs;
						if (atts instanceof AttributesImpl) {
							newAttrs = (AttributesImpl) atts;
						} else {
							newAttrs = new AttributesImpl(atts);
						}

						for (int i = 0; i < newAttrs.getLength(); i++) {
							if ("src".equals(newAttrs.getLocalName(i))) {
								String src = newAttrs.getValue(i);

							}
						}
						super.startElement(uri, localName, name, newAttrs);
					} else {
						super.startElement(uri, localName, name, atts);
					}
				}
			}

			@Override
			public void endElement(String uri, String localName, String name)
					throws SAXException {
				if (XHTMLContentHandler.XHTML.equals(uri)) {
					uri = null;
				}
				if (!"head".equals(localName)) {
					super.endElement(uri, localName, name);
				}
			}

			@Override
			public void startPrefixMapping(String prefix, String uri) {
			}

			@Override
			public void endPrefixMapping(String prefix) {
			}
		};
	}

	private ContentHandler getTextContentHandler(Writer writer) {
		return new BodyContentHandler(writer);
	}

	private ContentHandler getTextMainContentHandler(Writer writer) {
		return new BoilerpipeContentHandler(writer);
	}

	private ContentHandler getXmlContentHandler(Writer writer)
			throws TransformerConfigurationException {
		SAXTransformerFactory factory = (SAXTransformerFactory) SAXTransformerFactory
				.newInstance();
		TransformerHandler handler = factory.newTransformerHandler();
		handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "xml");
		handler.setResult(new StreamResult(writer));
		return handler;
	}

	public static void main(String args[]) {
//		String file1 = "/tmp/1344.pdf";
//
//		byte a[] = new byte[1024 * 1024 * 5];
//		try {
//			BufferedInputStream input = new BufferedInputStream(
//					new FileInputStream(new File(file1)));
//			int b = 0;
//			byte buf[] = new byte[4096];
//			int sum = 0;
//			BufferedOutputStream output = new BufferedOutputStream(
//					new FileOutputStream(new File("/tmp/a.txt")));
//			while ((b = input.read(buf)) != -1) {
//				if (sum + b < a.length) {
//					System.arraycopy(buf, 0, a, sum, b);
//					sum += b;
//				} else {
//					// System.out.println("new allocate memory");
//					byte c[] = new byte[a.length * 2];
//					System.arraycopy(a, 0, c, 0, sum);
//					a = c;
//					System.arraycopy(buf, 0, a, sum, b);
//					sum += b;
//				}
//				// System.out.println("read sum=" + sum);
//			}
//			System.out.println("read sum=" + sum);
//			Pattern MAIL_REGEX = Pattern
//					.compile("[-_.0-9A-Za-z]+@[-_0-9A-Za-z]+[-_.0-9A-Za-z]+");
//			Matcher m = MAIL_REGEX.matcher(new String(a, 0, sum));
//			String t = MAIL_REGEX.matcher(new String(a, 0, sum))
//					.replaceAll(" ");
//			// System.out.println(t);
//			while (m.find()) {
//				System.out.println(m.group());
//			}
//			ClawerConvert converter = new ClawerConvert();
//			converter.reset(null, a, 0, sum, 0, 0);
//			
//			ByteArrayInputStream is=new ByteArrayInputStream(a,0,sum);
////			PdfboxTest test=new PdfboxTest();
////			test.convert(is);
//			long s = System.currentTimeMillis();
//			byte d[] = converter.convert();
//			long e = System.currentTimeMillis();
//			System.out.println("time consume=" + (e - s));
//			int num = 0, offset = 0;
//			for (int i = 0; i < 4; i++) {
//				num = NumberUtil.readInt(d, offset);
//				offset += 4;
//				String m1 = new String(d, offset, num, "utf-8");
//				offset += num;
//				// if (i == 3) {
//				// output.write(d, offset - num, num);
//				// } else {
//				System.out.println("i=" + i + "\t" + m1);
//				// }
//				System.out.println("num=" + num);
//
//			}
//			output.close();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}

}
