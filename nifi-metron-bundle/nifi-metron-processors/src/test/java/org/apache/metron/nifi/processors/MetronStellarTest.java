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
package org.apache.metron.nifi.processors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.FlowFileValidator;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

public class MetronStellarTest {

	Logger log = LoggerFactory.getLogger(getClass());

	private TestRunner testRunner;

	@Before
	public void init() throws IOException {
		testRunner = TestRunners.newTestRunner(MetronStellar.class);

		URL url = getClass().getClassLoader().getResource("config.json");
		String rule = Resources.toString(url, StandardCharsets.UTF_8);
		testRunner.setProperty(MetronStellar.STELLAR_EXPRESSION, rule);
	}

	@Test
	@Ignore // Not yet supporting non-string values
	public void testStellarOnAttributesToAttributes() {
		testRunner.setProperty(MetronStellar.INCLUDE_CONTENT, MetronStellar.DESTINATION_ATTRIBUTE);
		testRunner.setProperty(MetronStellar.DESTINATION, MetronStellar.DESTINATION_ATTRIBUTE);
		testRunner.enqueue(getClass().getClassLoader().getResourceAsStream("input.json"), attributeVersion());

		testRunner.run();
		worked(false, true);
	}

	@Test
	public void testStellarOnContentToAttributes() {
		testRunner.setProperty(MetronStellar.INCLUDE_CONTENT, MetronStellar.DESTINATION_CONTENT);
		testRunner.setProperty(MetronStellar.DESTINATION, MetronStellar.DESTINATION_ATTRIBUTE);
		testRunner.enqueue(getClass().getClassLoader().getResourceAsStream("input.json"));

		testRunner.run();
		worked(false, true);
	}

	@Test
	@Ignore // Not yet supporting non-string values
	public void testStellarOnAttributesToContent() {
		testRunner.setProperty(MetronStellar.INCLUDE_CONTENT, MetronStellar.DESTINATION_ATTRIBUTE);
		testRunner.setProperty(MetronStellar.DESTINATION, MetronStellar.DESTINATION_CONTENT);
		testRunner.enqueue(getClass().getClassLoader().getResourceAsStream("input.json"), attributeVersion());

		testRunner.run();
		worked(true, false);
	}

	@Test
	public void testStellarOnContentToContent() {
		testRunner.setProperty(MetronStellar.INCLUDE_CONTENT, MetronStellar.DESTINATION_CONTENT);
		testRunner.setProperty(MetronStellar.DESTINATION, MetronStellar.DESTINATION_CONTENT);
		testRunner.enqueue(getClass().getClassLoader().getResourceAsStream("input.json"));

		testRunner.run();
		worked(true, false);
	}

	@Test
	public void testStellarOnContentToBoth() {
		testRunner.setProperty(MetronStellar.INCLUDE_CONTENT, MetronStellar.DESTINATION_CONTENT);
		testRunner.setProperty(MetronStellar.DESTINATION, MetronStellar.DESTINATION_BOTH);
		testRunner.enqueue(getClass().getClassLoader().getResourceAsStream("input.json"));

		testRunner.run();
		worked(true, true);
	}

	final HashMap expectedOutput = jsonToMap("output.json");

	private void worked(boolean content, boolean attributes) {
		testRunner.assertAllFlowFilesTransferred(MetronStellar.SUCCESS);
		testRunner.assertAllFlowFiles(new FlowFileValidator() {
			@Override
			public void assertFlowFile(FlowFile f) {
				MockFlowFile mf = (MockFlowFile) f;
				assertNotNull(f);
				if (content) {
					byte[] byteArray = mf.toByteArray();
					try {
						HashMap actual = new ObjectMapper().readValue(byteArray, HashMap.class);
						assertEquals(expectedOutput, actual);
					} catch (IOException e) {
						fail(e.getMessage());
					}
				}
				if (attributes) {
					assertEquals("es", f.getAttribute("stringFuncOut"));
					assertEquals("2.4", f.getAttribute("calcOut"));
					assertEquals("false", f.getAttribute("boolOut"));
				}
			}
		});
	}

	private HashMap jsonToMap(String string) {
		try {
			return new ObjectMapper().readValue(getClass().getClassLoader().getResourceAsStream("output.json"),
					HashMap.class);
		} catch (IOException e) {
			fail(e.getMessage());
		}
		return null;
	}

	private Map<String, String> attributeVersion() {
		return new HashMap<String, String>() {
			private static final long serialVersionUID = 1L;
			{
				put("strField", "Test");
				put("intField", "2");
				put("floatField", "1.2");
			}
		};
	}

}
