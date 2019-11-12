package com.zhongsou.spider.hbase.test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.pdfbox.exceptions.CryptographyException;
import org.apache.pdfbox.exceptions.InvalidPasswordException;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.util.PDFTextStripper;

public class PdfboxTest {

	private PDFTextStripper stripper = null;

	public PdfboxTest() {
		try {
			stripper = new PDFTextStripper();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void convert(InputStream is) {

		PDDocument pdfDocument = null;
		try {
			pdfDocument = PDDocument.load(is);

			if (pdfDocument.isEncrypted()) {
				// Just try using the default password and move on
				pdfDocument.decrypt("");
			}

			// create a writer where to append the text content.
			StringWriter writer = new StringWriter();
			if (stripper == null) {
				stripper = new PDFTextStripper();
			} else {
				stripper.resetEngine();
			}
			stripper.writeText(pdfDocument, writer);

			// Note: the buffer to string operation is costless;
			// the char array value of the writer buffer and the content string
			// is shared as long as the buffer content is not modified, which
			// will
			// not occur here.
			String contents = writer.getBuffer().toString();

			System.out.println(contents);

			// StringReader reader = new StringReader(contents);

			// Add the tag-stripped contents as a Reader-valued Text field so it
			// will
			// get tokenized and indexed.
			// addTextField(document, "contents", reader);

			PDDocumentInformation info = pdfDocument.getDocumentInformation();
			System.out.println(info.getAuthor() + "\ttitle=" + info.getTitle());
			// if (info != null) {
			// addTextField(document, "Author", info.getAuthor());
			// try {
			// addTextField(document, "CreationDate",
			// info.getCreationDate());
			// } catch (IOException io) {
			// // ignore, bad date but continue with indexing
			// }
			// addTextField(document, "Creator", info.getCreator());
			// addTextField(document, "Keywords", info.getKeywords());
			// try {
			// addTextField(document, "ModificationDate",
			// info.getModificationDate());
			// } catch (IOException io) {
			// // ignore, bad date but continue with indexing
			// }
			// addTextField(document, "Producer", info.getProducer());
			// addTextField(document, "Subject", info.getSubject());
			// addTextField(document, "Title", info.getTitle());
			// addTextField(document, "Trapped", info.getTrapped());
			// }
			// int summarySize = Math.min(contents.length(), 500);
			// String summary = contents.substring(0, summarySize);
			// // Add the summary as an UnIndexed field, so that it is stored
			// and
			// // returned
			// // with hit documents for display.
			// addUnindexedField(document, "summary", summary);
		} catch (CryptographyException e) {
			// throw new IOException("Error decrypting document("
			// + documentLocation + "): " + e);
		} catch (InvalidPasswordException e) {
			// they didn't suppply a password and the default of "" was wrong.
			// throw new IOException(
			// "Error: The document(  is encrypted and will not be indexed.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (pdfDocument != null) {
				try {
					pdfDocument.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String args[]) {

		byte a[] = new byte[1024 * 1024 * 5];
		String file1 = "/tmp/HNRB20120718B011C.pdf";
		try {
			BufferedInputStream input = new BufferedInputStream(
					new FileInputStream(new File(file1)));
			int b = 0;
			byte buf[] = new byte[4096];
			int sum = 0;
			while ((b = input.read(buf)) != -1) {
				if (sum + b < a.length) {
					System.arraycopy(buf, 0, a, sum, b);
					sum += b;
				} else {
					// System.out.println("new allocate memory");
					byte c[] = new byte[a.length * 2];
					System.arraycopy(a, 0, c, 0, sum);
					a = c;
					System.arraycopy(buf, 0, a, sum, b);
					sum += b;
				}
				// System.out.println("read sum=" + sum);
			}
			System.out.println("read sum=" + sum);
			
			ByteArrayInputStream is=new ByteArrayInputStream(a,0,sum);
			PdfboxTest test=new PdfboxTest();
			test.convert(is);
			input.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}
}
