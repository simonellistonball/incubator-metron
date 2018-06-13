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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

@Tags({ "metron", "parser", "cybersecurity" })
@CapabilityDescription("Provides a Record based interface to allow use of Metron parsers within NiFi. This allows parsers to be run against FlowFile sources instead of Kafka sources. It should primarily be used for routing messages at the edge, while full Metron topologies should be used for high volume sources.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class MetronParser extends AbstractProcessor {

	public static final PropertyDescriptor ZOOKEEPER = new PropertyDescriptor.Builder().name("ZOOKEEPER")
			.displayName("Zookeeper Quorum")
			.description("The zookeeper quorum for Metron configuration as a comma separated list of host:port pairs")
			.build();
	public static final PropertyDescriptor PARSER_CONFIG_NAME = new PropertyDescriptor.Builder()
			.name("PARSER_CONFIG_NAME").displayName("Parser Config Name")
			.description("The name of the parser config in Metron").build();
	public static final PropertyDescriptor PARSER_CONFIG = new PropertyDescriptor.Builder().name("PARSER_CONFIG")
			.displayName("Parser Config")
			.description(
					"The full parser config for the Metron Parser (overrides version loaded via Parser Config Name)")
			.build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("Success")
			.description("Successfully parsed records").build();
	public static final Relationship FAILED = new Relationship.Builder().name("Failed")
			.description("Records Failed to Parse").build();
	public static final Relationship INVALID = new Relationship.Builder().name("Invalid")
			.description("Records Parsed, but did not pass validation").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(ZOOKEEPER);
		descriptors.add(PARSER_CONFIG_NAME);
		descriptors.add(PARSER_CONFIG);

		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(SUCCESS);
		relationships.add(FAILED);
		relationships.add(INVALID);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	/**
	 * Custom validation since we must have either a full JSON config in the
	 * processor, or a Metron connection string and a parse name to pull and listen
	 * to the config from zookeeper.
	 */
	protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
		final Collection<ValidationResult> results = new ArrayList<>();
		if (validationContext.getProperty(PARSER_CONFIG_NAME).isSet()) {
			String zk = validationContext.getProperty(ZOOKEEPER).getValue();

			if (!zk.matches("[\\w\\d]+:\\d{0,5}(?<,[\\w\\d]+:\\d{0,5})*")) {
				results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName()).explanation(
						"You must specify a valid zookeeper connection string if you are using the parser name option.")
						.valid(false).build());
			}
		} else {
			// validate the parser config JSON is valid Metron Parser configuration
			validationContext.getProperty(PARSER_CONFIG).getValue();
		}
		return results;
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		// TODO implement
	}
}
