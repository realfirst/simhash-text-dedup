package com.zhongsou.spider.hbase.test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.tika.parser.txt.CharsetDetector;
import org.apache.tika.parser.txt.CharsetMatch;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

public class LanguageTest {

	public static void main(String args[]) {

		StringBuffer buffer = new StringBuffer();
		String file = "/home/dape/a.txt";
		BufferedInputStream input = null;

		try {
			Detector detector = DetectorFactory.create();
			input = new BufferedInputStream(new FileInputStream(new File(file)));
			int i = 0;
			byte[] buf = new byte[4096];

			int sum = 0;
			byte a[] = new byte[1024 * 1024 * 5];
			while ((i = input.read(buf)) != -1) {
				if (sum + i < 1024 * 1024 * 5) {
					System.arraycopy(buf, 0, a, sum, i);
					sum += i;
				} else {
					// System.out.println("new allocate memory");
					byte c[] = new byte[a.length * 2];
					System.arraycopy(a, 0, c, 0, sum);
					a = c;
					System.arraycopy(buf, 0, a, sum, i);
					sum += i;
				}
				// System.out.println("read sum=" + sum);
			}

			String charset = "";
			long start = System.currentTimeMillis();
			CharsetDetector chdetector = new CharsetDetector();
			chdetector.setText(new ByteArrayInputStream(a, 0, sum));
			for (CharsetMatch match2 : chdetector.detectAll()) {
				if (Charset.isSupported(match2.getName())) {
					charset = match2.getName();
					break;
				}
			}
			long end = System.currentTimeMillis();
			System.out.println("charset=" + charset + " time consume=" + (end - start));
			if (charset == "")
				return;
			String s = new String(a, 0, sum, charset);
			s = s.replaceAll("[\r\n]{1,}", "");
			s = s.replaceAll("<script.*?</script>", "");
			s = s.replaceAll("<style.*?</style>", "");
			s = s.replaceAll("<[^<>]*?>", "");
			s = s.replaceAll("\\s{2,}", " ");
			long end2 = System.currentTimeMillis();
			System.out.println("buffer len=" + buffer.length() + " " + s + "\r\n time consume=" + (end2 - end));
			detector.append(s);
			long end3 = System.currentTimeMillis();
			System.out.println("language detect lang=" + detector.detect() + " time consume" + (end3 - end2));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (LangDetectException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
