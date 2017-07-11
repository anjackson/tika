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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tika.TikaTest;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.sax.BasicContentHandlerFactory;
import org.apache.tika.sax.ContentHandlerFactory;
import org.apache.tika.sax.WriteOutContentHandler;
import org.junit.Test;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class WARCParserTest extends TikaTest {


	private List<Metadata> recursiveParserWrapper(String testFile) throws IOException,
	SAXException, TikaException {
		AutoDetectParser p = new AutoDetectParser();
		
		ContentHandlerFactory factory = new BasicContentHandlerFactory(
				BasicContentHandlerFactory.HANDLER_TYPE.HTML, -1);

		RecursiveParserWrapper wrapper = new RecursiveParserWrapper(p, factory);
		Metadata metadata = new Metadata();
		metadata.set(Metadata.RESOURCE_NAME_KEY, testFile);
		ParseContext context = new ParseContext();

		try (InputStream stream = WARCParserTest.class.getResourceAsStream(testFile)) {
			wrapper.parse(stream, new DefaultHandler(), metadata, context);
		}
		return wrapper.getMetadata();
	}
	
	private void checkParseResults(List<Metadata> mlist, boolean isWARC) {
		// Check overall state:
		
		// Switch to hash map for easier checking:
		HashMap<String, Metadata> mmap = new HashMap<String, Metadata>();
		for(Metadata md : mlist) {
			String name = md.get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH);
			if( name != null) {
				mmap.put(name, md);
			}
		}
		
		// Check a specific record:
		// TODO This seems like a mess, as the same thing is getting different names based on extraction method.
		Metadata m = mmap.get("/");
		if( m == null ) {
			m = mmap.get("/testWARCGZ.warc/");
		}
		if( m == null ) {
			m = mmap.get("/testARCGZ.arc/");
		}

		// General tests:
		assertEquals("text/html", m.get("HTTP-Content-Type"));
		assertEquals("200", m.get("HTTP-Status-Code"));
		
		// Format-specific tests:
		if( isWARC ) {
			assertEquals("http://localhost/", m.get("WARC-Target-URI"));
			assertEquals("response", m.get("WARC-Type"));
			assertEquals("application/http; msgtype=response", m.get("WARC-Content-Type"));
		} else {
			assertEquals("http://127.0.0.1/", m.get("subject-uri"));
		}
		
	}

	@Test
	public void testWARC() throws Exception {
		List<Metadata> mlist = this.recursiveParserWrapper("/test-documents/testWARC.warc");
    assertEquals("Unexpected number of records", 7, mlist.size());
		checkParseResults(mlist, true);
	}

	@Test
	public void testWARCGZ() throws Exception {
		List<Metadata> mlist = this.recursiveParserWrapper("/test-documents/testWARCGZ.warc.gz");
    assertEquals("Unexpected number of records", 8, mlist.size());
		checkParseResults(mlist, true);
	}

	@Test
	public void testARC() throws Exception {
		List<Metadata> mlist = this.recursiveParserWrapper("/test-documents/testARC.arc");
    assertEquals("Unexpected number of records", 6, mlist.size());
		checkParseResults(mlist, false);
	}
	
	@Test
	public void testARCGZ() throws Exception {
		List<Metadata> mlist = this.recursiveParserWrapper("/test-documents/testARCGZ.arc.gz");
    assertEquals("Unexpected number of records", 7, mlist.size());
		checkParseResults(mlist, false);
	}

}
