/**
 * 
 */
package org.apache.tika.parser.warc;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.archive.url.UsableURI;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;


/**
 * 
 * ARC/WARC supporting extractor
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WebARCExtractor {

	private final ContentHandler handler;

	private final Metadata metadata;

	private final EmbeddedDocumentExtractor extractor;
	
	private final ParseContext context;
	
	private boolean isWARC = false;

	public WebARCExtractor(
			ContentHandler handler, Metadata metadata, ParseContext context, boolean isWARC ) {
		this.handler = handler;
		this.metadata = metadata;
		this.context = context;
		this.isWARC = isWARC;

		EmbeddedDocumentExtractor ex = context.get(EmbeddedDocumentExtractor.class);

		if (ex==null) {
			this.extractor = new ParsingEmbeddedDocumentExtractor(context);
		} else {
			this.extractor = ex;
		}

	}

	/* (non-Javadoc)
	 * @see org.apache.tika.parser.AbstractParser#parse(java.io.InputStream, org.xml.sax.ContentHandler, org.apache.tika.metadata.Metadata)
	 */
	//@Override
	public void parse(InputStream stream) throws IOException, SAXException, TikaException {
		XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
		xhtml.startDocument();

		System.out.println("GO: "+metadata.get( Metadata.RESOURCE_NAME_KEY ));
		// Open the ARCReader:
		// This did not work as assumes compressed:
		// ArchiveReaderFactory.get("name.arc", stream, true);
		ArchiveReader ar = null;
		if( isWARC ) {
      ar = WARCReaderFactory.get("dummy-name.warc", stream, true);
} else {
      ar = ARCReaderFactory.get("dummy-name.arc", stream, true);
		}

		// Go through the records:
		if (ar != null) {

			// Also get out the archive format version:
			metadata.set("version",ar.getVersion());

			Iterator<ArchiveRecord> it = ar.iterator();

			while (it.hasNext()) {
				ArchiveRecord entry = it.next();
				ArchiveRecordHeader header = entry.getHeader();
				System.out.println("MD " + metadata);
				String name;
				try {
					name = UsableURI.parseFilename(header.getUrl());
				} catch (URISyntaxException e) {
					System.err.println("Nname!!!");
				  name = header.getUrl();
				}
				System.out.println("Nname " + name);
				// Setup
				Metadata entrydata = new Metadata();
				entrydata.set(Metadata.RESOURCE_NAME_KEY, name );
				for( String k : header.getHeaderFieldKeys()) {
					if( ! "Content-Type".equalsIgnoreCase(k)) {
						entrydata.set(k, header.getHeaderValue(k).toString());
						System.out.println(k+" > "+ header.getHeaderValue(k));
					}
				}
				// Attempt to consume HTTP headers:
				InputStream cis = (WARCRecord) entry;
				String line = HttpParser.readLine(cis, "UTF-8");
				if( line != null ) {
					String firstLine[] = line.split(" ");
					String statusCode = firstLine[1].trim();
					entrydata.set("HTTP-Status-Code", statusCode);
					Header[] headers = HttpParser.parseHeaders(cis, "UTF-8");
					for( Header h : headers ) {
						entrydata.set(h.getName(), h.getValue());
					}
				}
				
				// Now parse it...
				// Use the delegate parser to parse the compressed document
        EmbeddedDocumentExtractor extractor =
            EmbeddedDocumentUtil.getEmbeddedDocumentExtractor(context);
        if (extractor.shouldParseEmbedded(entrydata)) {
        	extractor.parseEmbedded(cis, xhtml, entrydata, true);
        }
			}

		}
		xhtml.endDocument();
	}

}
