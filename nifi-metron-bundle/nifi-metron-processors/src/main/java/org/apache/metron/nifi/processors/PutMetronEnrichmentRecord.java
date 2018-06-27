/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.metron.enrichment.converter.EnrichmentKey;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
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
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"metorn", "enrichment", "put", "record"})
@CapabilityDescription("Takes records from a reader and pushes them into a Metron Enrichment table")
public class PutMetronEnrichmentRecord extends AbstractProcessor {

	static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder().name("record-reader")
			.displayName("Record Reader")
			.description(
					"Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
			.identifiesControllerService(RecordReaderFactory.class).required(true).build();
	protected static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
			.name("HBase Client Service").description("Specifies the Controller Service to use for accessing HBase.")
			.required(true).identifiesControllerService(HBaseClientService.class).build();
	protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder().name("Table Name")
			.description("The name of the HBase Table to put data into").required(true)
			.expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	protected static final PropertyDescriptor ENRICHMENT_TYPE = new PropertyDescriptor.Builder().name("Enrichment Name")
			.description("The name of the Metron Enrichment type").required(true).expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	protected static final PropertyDescriptor KEY_FIELD = new PropertyDescriptor.Builder().name("Key Field")
			.description("Specifies the field to use as the lookup key for Metron Enrichment").required(true)
			.expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	protected static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder().name("Column Family")
			.description("The Column Family to use when inserting data into HBase").required(true)
			.expressionLanguageSupported(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder().name("Batch Size")
			.description("The maximum number of FlowFiles to process in a single execution. The FlowFiles will be "
					+ "grouped by table, and a single Put per table will be performed.")
			.required(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).defaultValue("25").build();

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
		properties.add(RECORD_READER_FACTORY);
		properties.add(HBASE_CLIENT_SERVICE);
		properties.add(TABLE_NAME);
		properties.add(ENRICHMENT_TYPE);
		properties.add(KEY_FIELD);
		properties.add(COLUMN_FAMILY);
		properties.add(BATCH_SIZE);
		return properties;
	}

	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> rels = new HashSet<>();
		rels.add(REL_SUCCESS);
		rels.add(REL_FAILURE);
		return rels;
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
				.asControllerService(RecordReaderFactory.class);
		List<PutFlowFile> flowFiles = new ArrayList<>();
		final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
		final String enrichmentType = context.getProperty(ENRICHMENT_TYPE).evaluateAttributeExpressions(flowFile)
				.getValue();
		final String keyField = context.getProperty(KEY_FIELD).evaluateAttributeExpressions(flowFile).getValue();
		final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile)
				.getValue();

		final long start = System.nanoTime();
		int index = 0;
		int columns = 0;
		boolean failed = false;
		String startIndexStr = flowFile.getAttribute("restart.index");
		int startIndex = -1;
		if (startIndexStr != null) {
			startIndex = Integer.parseInt(startIndexStr);
		}

		PutFlowFile last = null;
		try (final InputStream in = session.read(flowFile);
				final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {
			Record record;
			if (startIndex >= 0) {
				while (index++ < startIndex && (reader.nextRecord()) != null) {
				}
			}

			while ((record = reader.nextRecord()) != null) {
				PutFlowFile putFlowFile = createPut(context, record, reader.getSchema(), flowFile, enrichmentType,
						keyField, columnFamily);
				if (putFlowFile.getColumns().size() == 0) {
					continue;
				}
				flowFiles.add(putFlowFile);
				index++;

				if (flowFiles.size() == batchSize) {
					columns += addBatch(tableName, flowFiles);
					last = flowFiles.get(flowFiles.size() - 1);
					flowFiles = new ArrayList<>();
				}
			}
			if (flowFiles.size() > 0) {
				columns += addBatch(tableName, flowFiles);
				last = flowFiles.get(flowFiles.size() - 1);
			}
		} catch (Exception ex) {
			getLogger().error("Failed to put records to HBase.", ex);
			failed = true;
		}

		if (!failed) {
			if (columns > 0) {
				sendProvenance(session, flowFile, columns, System.nanoTime() - start, last);
			}
			flowFile = session.removeAttribute(flowFile, "restart.index");
			session.transfer(flowFile, REL_SUCCESS);
		} else {
			String restartIndex = Integer.toString(index - flowFiles.size());
			flowFile = session.putAttribute(flowFile, "restart.index", restartIndex);
			if (columns > 0) {
				sendProvenance(session, flowFile, columns, System.nanoTime() - start, last);
			}
			flowFile = session.penalize(flowFile);
			session.transfer(flowFile, REL_FAILURE);
		}

		session.commit();
	}

	private int addBatch(String tableName, List<PutFlowFile> flowFiles) throws IOException {
		int columns = 0;
		clientService.put(tableName, flowFiles);
		for (PutFlowFile put : flowFiles) {
			columns += put.getColumns().size();
		}
		return columns;
	}

	private void sendProvenance(ProcessSession session, FlowFile flowFile, int columns, long time, PutFlowFile pff) {
		final String details = String.format("Put %d cells to HBase.", columns);
		session.getProvenanceReporter().send(flowFile, getTransitUri(pff), details, time);
	}

	protected String getTransitUri(PutFlowFile putFlowFile) {
		return clientService.toTransitUri(putFlowFile.getTableName(),
				new String(putFlowFile.getRow(), StandardCharsets.UTF_8));
	}

	/**
	 * Create a Metron Enrichment Key and return the bytes
	 * 
	 * @param type
	 *            The enrichment type
	 * @param row
	 * @return
	 */
	protected byte[] rowKey(final String type, final String row) {
		return new EnrichmentKey(type, row).toBytes();
	}

	protected PutFlowFile createPut(ProcessContext context, Record record, RecordSchema schema, FlowFile flowFile,
			String enrichmentType, String keyField, String columnFamily) {
		final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
		final byte[] columnFamilyBytes = clientService.toBytes(columnFamily);
		List<String> fieldNames = schema.getFieldNames();
		// take all the fields except the key, convert to strings and put in PutColum
		// containers
		List<PutColumn> columns = fieldNames
				.stream().filter(n -> !n.equals(keyField)).map(n -> new PutColumn(columnFamilyBytes,
						clientService.toBytes(n), clientService.toBytes(record.getAsString(n))))
				.collect(Collectors.toList());
		// use the Metron row key builder
		byte[] rowId = rowKey(enrichmentType, record.getAsString(keyField));
		return new PutFlowFile(tableName, rowId, columns, flowFile);
	}
}
