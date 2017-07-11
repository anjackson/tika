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


	public List<Metadata> recursiveParserWrapperExample(String testFile) throws IOException,
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

	@Test
	public void testWARC() throws Exception {
		AutoDetectParser parser = new AutoDetectParser(); // Should auto-detect!
		Metadata metadata = new Metadata();
		List<Metadata> mlist = this.recursiveParserWrapperExample("/test-documents/testWARC.warc");
		for(Metadata m : mlist) {
			System.err.println("M "+m);
		}
		
		fail("Not yet implemented");
	}

	@Test
	public void testWARCGZ() throws Exception {
		List<Metadata> mlist = this.recursiveParserWrapperExample("/test-documents/testWARCGZ.warc.gz");
		for(Metadata m : mlist) {
			System.err.println("MM "+ m.get("X-TIKA:embedded_resource_path")+ " "+ m.get("Content-Type"));
			//System.err.println("M "+m);
		}

		fail("Not yet implemented");
	}

	@Test
	public void testARCGZ() throws Exception {
		List<Metadata> mlist = this.recursiveParserWrapperExample("/test-documents/testARCGZ.arc.gz");
		for(Metadata m : mlist) {
			System.err.println("\nMM "+ m.get("X-TIKA:embedded_resource_path")+ " "+ m.get("Content-Type"));
			System.err.println("M "+m);
		}
		fail("Not yet implemented");
	}


	@Test
	public void testARC() throws Exception {
		List<Metadata> mlist = this.recursiveParserWrapperExample("/test-documents/testARC.arc");
		for(Metadata m : mlist) {
			System.err.println("\nMM "+ m.get("X-TIKA:embedded_resource_path")+ " "+ m.get("Content-Type"));
			System.err.println("M "+m);
		}
		fail("Not yet implemented");
	}
	
}
