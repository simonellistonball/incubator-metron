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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.metron.dataloads.extractor.Extractor;
import org.apache.metron.dataloads.extractor.ExtractorHandler;
import org.apache.metron.enrichment.lookup.LookupKV;
import org.apache.metron.enrichment.lookup.LookupKey;
import org.apache.metron.enrichment.lookup.LookupValue;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "metorn", "enrichment", "put" })
@CapabilityDescription("Processes incoming flow files through a Metron Enrichment Loader, including transformation in Metron")
public class PutMetronEnrichment extends AbstractProcessor {

	public static final PropertyDescriptor METRON_EXTRACTOR = new PropertyDescriptor.Builder().name("METRON_EXTRACTOR")
			.displayName("Extractor Config").description("Configuration for the Metron extractor in json format")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();

	protected static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
			.name("HBase Client Service").description("Specifies the Controller Service to use for accessing HBase.")
			.required(true).identifiesControllerService(HBaseClientService.class).build();
	protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder().name("Table Name")
			.description("The name of the HBase Table to put data into").required(true)
			.expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	protected static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder().name("Column Family")
			.description("The Column Family to use when inserting data into HBase").required(true)
			.expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("A FlowFile is routed to this relationship after it has been successfully stored in HBase")
			.build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("A FlowFile is routed to this relationship if it cannot be sent to HBase").build();

	static final byte[] EMPTY = "".getBytes();

	protected HBaseClientService clientService;

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		clientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(METRON_EXTRACTOR);
		properties.add(HBASE_CLIENT_SERVICE);
		properties.add(TABLE_NAME);
		properties.add(COLUMN_FAMILY);
		return properties;
	}

	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> rels = new HashSet<>();
		rels.add(REL_SUCCESS);
		rels.add(REL_FAILURE);
		return rels;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		boolean failed = false;

		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
		final byte[] columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue()
				.getBytes();

		Collection<LookupKV> extracts = new ArrayList<LookupKV>();
		try {
			// process the Metron extractor config
			final ExtractorHandler handler = ExtractorHandler.load(context.getProperty(METRON_EXTRACTOR).getValue());
			final Extractor e = handler.getExtractor();
			session.read(flowFile, in -> {
				try (final BufferedReader bufferedIn = new BufferedReader(
						new InputStreamReader(in, Charset.defaultCharset()))) {
					// extract with Metron, line by line, and store all found KV pairs
					for (LookupKV extract : e.extract(bufferedIn.readLine())) {
						extracts.add(extract);
					}
				}
			});
		} catch (IOException e) {
			failed = true;
			getLogger().error("Metron Extraction Error", e);
		}

		if (failed) {
			flowFile = session.penalize(flowFile);
			return;
		}

		Collection<PutFlowFile> puts = StreamSupport.stream(extracts.spliterator(), true).map(x -> {
			LookupKey key = x.getKey();
			LookupValue value = x.getValue();
			Collection<PutColumn> columns = StreamSupport.stream(value.toColumns().spliterator(), false)
					.map(column -> new PutColumn(columnFamily, column.getKey(), column.getValue()))
					.collect(Collectors.toList());
			return new PutFlowFile(tableName, key.toBytes(), columns, null);

		}).collect(Collectors.toList());

		try {
			clientService.put(tableName, puts);
			session.getProvenanceReporter().send(flowFile,
					String.format("%s", new Object[] { clientService.toTransitUri(tableName, "") }));
			session.transfer(flowFile, REL_SUCCESS);
		} catch (IOException e) {
			getLogger().error("HBase put failed", e);
			session.transfer(flowFile, REL_FAILURE);
		}
	}

	protected String getTransitUri(String table, String row) {
		return clientService.toTransitUri(table, row);
	}
}
