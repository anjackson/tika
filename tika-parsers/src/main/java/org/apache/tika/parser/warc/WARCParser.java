/**
 * JHOVE2 - Next-generation architecture for format-aware characterization
 *
 * Copyright (c) 2009 by The Regents of the University of California,
 * Ithaka Harbors, Inc., and The Board of Trustees of the Leland Stanford
 * Junior University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * o Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * o Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * o Neither the name of the University of California/California Digital
 *   Library, Ithaka Harbors/Portico, or Stanford University, nor the names of
 *   its contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.tika.parser.warc;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.archive.url.UsableURI;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * WARC Parsing Tika module
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 */
public class WARCParser implements Parser {

	/**  */
	private static final long serialVersionUID = 7346851876221749615L;

	/** */
	private static final Set<MediaType> SUPPORTED_TYPES = Collections
			.unmodifiableSet(
					new HashSet<MediaType>(Arrays.asList(MediaType.application("warc"))));

	@Override
	public Set<MediaType> getSupportedTypes(ParseContext context) {
		System.err.println("GOT " + SUPPORTED_TYPES);
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(InputStream stream, ContentHandler handler,
			Metadata metadata, ParseContext context)
			throws IOException, SAXException, TikaException {
		XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
		xhtml.startDocument();

		System.out.println("GO: " + metadata.get(Metadata.RESOURCE_NAME_KEY));
		// Open the ARCReader:
		// This did not work as assumes compressed:
		// ArchiveReaderFactory.get("name.arc", stream, true);
		WARCReader ar = (WARCReader) WARCReaderFactory.get("dummy-name.warc", stream, true);
		try {

			// Go through the records:
			if (ar != null) {

				// Also get out the archive format version:
				metadata.set("version", ar.getVersion());

				Iterator<ArchiveRecord> it = ar.iterator();

				while (it.hasNext()) {
					ArchiveRecord entry = it.next();
					ArchiveRecordHeader header = entry.getHeader();
					System.out.println("MD " + metadata);
					String name = header.getUrl();

					// Now parse it...
					// Setup
					Metadata entrydata = new Metadata();
					entrydata.set(Metadata.RESOURCE_NAME_KEY, name);
					for (String k : header.getHeaderFieldKeys()) {
						if (!"Content-Type".equalsIgnoreCase(k)) {
							entrydata.set(k, header.getHeaderValue(k).toString());
							System.out.println(k + " > " + header.getHeaderValue(k));
						}
					}
					if (header.getContentLength() > 0 && 
							WARCConstants.WARCRecordType.response.name().equals(header.getHeaderValue(WARCConstants.HEADER_KEY_TYPE))) {
						// Attempt to consume HTTP header
						InputStream cis = (WARCRecord) entry;
						String line = HttpParser.readLine(cis, "UTF-8");
						if (line != null) {
							System.out.println("LINE " + line);
							String firstLine[] = line.split(" ");
							String statusCode = firstLine[1].trim();
							entrydata.set("HTTP-Status-Code", statusCode);
							Header[] headers = HttpParser.parseHeaders(cis, "UTF-8");
							for (Header h : headers) {
								entrydata.set(h.getName(), h.getValue());
							}
						}
						// Use the delegate parser to parse the compressed document
						EmbeddedDocumentExtractor extractor = EmbeddedDocumentUtil
								.getEmbeddedDocumentExtractor(context);
						if (extractor.shouldParseEmbedded(entrydata)) {
							TikaInputStream tis = TikaInputStream.get(entry);
							extractor.parseEmbedded(tis, xhtml, entrydata, true);
						}
					}
				}

			}
		} finally {
			ar.close();
		}
		xhtml.endDocument();
	}

}
