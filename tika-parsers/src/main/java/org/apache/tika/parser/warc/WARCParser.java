/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tika.parser.warc;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.httpclient.StatusLine;
import org.apache.commons.httpclient.util.EncodingUtil;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.crypto.TSDParser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.archive.format.arc.ARCConstants;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.archive.url.UsableURI;
import org.archive.util.LaxHttpParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
					new HashSet<MediaType>(Arrays.asList(MediaType.application("warc"),
							MediaType.application("x-internet-archive"))));

	/** */
	private static final Logger LOG = LoggerFactory.getLogger(WARCParser.class);

	@Override
	public Set<MediaType> getSupportedTypes(ParseContext context) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(InputStream stream, ContentHandler handler,
			Metadata metadata, ParseContext context)
			throws IOException, SAXException, TikaException {
		XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
		xhtml.startDocument();

		// Open the ARCReader:
		String archiveName = metadata.get(Metadata.RESOURCE_NAME_KEY);
		LOG.info("Opening archive reader for " + archiveName);
		ArchiveReader ar;
		String archiveType;
		if ("application/x-internet-archive"
				.equals(metadata.get(Metadata.CONTENT_TYPE))) {
			ar = ARCReaderFactory.get(archiveName, stream, true);
			archiveType = "ARC";
		} else {
			ar = WARCReaderFactory.get(archiveName, stream, true);
			archiveType = "WARC";
		}

		try {
			if (ar != null) {

				// Get the archive format version:
				metadata.set(archiveType+"-Version", ar.getVersion());

				// Attempt to parse the records:
				Iterator<ArchiveRecord> it = ar.iterator();
				while (it.hasNext()) {
					ArchiveRecord entry = it.next();
					ArchiveRecordHeader header = entry.getHeader();

					// Now prepare for parsing:
					Metadata entrydata = new Metadata();
					String name = header.getUrl();
					entrydata.set(Metadata.RESOURCE_NAME_KEY, name);
					
					// Add the record-level headers for WARCs, or HTTP headers for ARCs:
					if( entry instanceof WARCRecord ) {
						addWARCHeaders(entrydata, header);
					} else {
						addARCHeaders(entrydata, header);
					}
					
					// Only parse ARC records or WARC response records with
					// non-zero-length entity bodies:
					LOG.info("Looking at: " + header);
					if (header.getContentLength() > 0) {
						if (entry instanceof ARCRecord
								|| WARCConstants.WARCRecordType.response.name().equals(
										header.getHeaderValue(WARCConstants.HEADER_KEY_TYPE))) {

							// Consume and apply HTTP Headers for WARCs:
							if (entry instanceof WARCRecord) {
								parseHttpHeader(entry, entrydata);
							}

							// Use the delegate parser to parse the entity body:
							EmbeddedDocumentExtractor extractor = EmbeddedDocumentUtil
									.getEmbeddedDocumentExtractor(context);
							if (extractor.shouldParseEmbedded(entrydata)) {
								TikaInputStream tis = TikaInputStream.get(entry);
								extractor.parseEmbedded(tis, xhtml, entrydata, true);
							}
						}
					}
				}

			}
		} finally {
			ar.close();
		}
		xhtml.endDocument();
	}

	/**
	 * For WARC files, we need to parse the HTTP headers.
	 * 
	 * Based on:
	 * https://github.com/sebastian-nagel/sitemap-performance-test/blob/master/src/main/java/crawlercommons/sitemaps/SiteMapPerformanceTest.java#L69
	 * 
	 * @param record
	 * @return
	 * @throws IOException
	 */
	private void parseHttpHeader(ArchiveRecord record, Metadata entrydata)
			throws IOException {
		byte[] statusBytes = LaxHttpParser.readRawLine(record);
		String statusLineStr = EncodingUtil.getString(statusBytes, 0,
				statusBytes.length, ARCConstants.DEFAULT_ENCODING);
		if ((statusLineStr == null) || !StatusLine.startsWithHTTP(statusLineStr)) {
			LOG.error("Invalid HTTP status line: {}", statusLineStr);
		}
		int status = 0;
		try {
			StatusLine statusLine = new StatusLine(statusLineStr.trim());
			status = statusLine.getStatusCode();
		} catch (HttpException e) {
			LOG.error("Invalid HTTP status line '{}': {}", statusLineStr,
					e.getMessage());
		}
		entrydata.set("HTTP-Status-Code", "" + status);
		Header[] headers = LaxHttpParser.parseHeaders(record,
				ARCConstants.DEFAULT_ENCODING);
		
		// Add the headers to the metadata:
		for (Header h : headers) {
			// save MIME type sent by server
			if (h.getName().equalsIgnoreCase("Content-Type")) {
				entrydata.set("HTTP-Content-Type", h.getValue());
			} else {
				entrydata.set(h.getName(), h.getValue());
			}
		}
		return;
	}
	
	/**
	 * This adds ARC headers as metadata, where HTTP headers have already been parsed.
	 * 
	 * @param entrydata
	 * @param header
	 */
	private static void addARCHeaders(Metadata entrydata, ArchiveRecordHeader header) {
		ARCRecordMetaData arcHeader = (ARCRecordMetaData) header;
		entrydata.set("HTTP-Status-Code", "" + arcHeader.getStatusCode());
		
		for (String k : header.getHeaderFieldKeys()) {
			// Avoid colliding with other Content-Type fields:
			if (!"Content-Type".equalsIgnoreCase(k)) {
				if( header.getHeaderValue(k) != null) {
					entrydata.set(k, header.getHeaderValue(k).toString());
				}
			} else {
				entrydata.set("HTTP-Content-Type",
						header.getHeaderValue(k).toString());
			}
		}

	}

	/**
	 * This adds WARC headers as metadata.
	 * 
	 * @param entrydata
	 * @param map
	 */
	private static void addWARCHeaders(Metadata entrydata, ArchiveRecordHeader header) {
		for (String k : header.getHeaderFieldKeys()) {
			// Avoid colliding with other Content-Type fields:
			if (!"Content-Type".equalsIgnoreCase(k)) {
				if( header.getHeaderValue(k) != null) {
					entrydata.set(k, header.getHeaderValue(k).toString());
				}
			} else {
				entrydata.set("WARC-Content-Type",
						header.getHeaderValue(k).toString());
			}
		}

	}

}
