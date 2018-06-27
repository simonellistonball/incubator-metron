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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.kafka.pubsub.Partitioners;

public abstract class AbstractPublishMetron extends AbstractProcessor {

	public static final PropertyDescriptor METADATA_ATTRIBUTES = new PropertyDescriptor.Builder()
			.name("Metadata attributes")
			.displayName("Regex to match the flow file attributes which are passed to Metron as metadata")
			.description(
					"Metron has a mechanism to receive metadata alongside parseable payload data. This metadata is transmitted through the Kafka messaage key. "
							+ "This allows you to match a regex against the incoming flow files attributes to determine which are passing the in metadata key.")
			.required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(false)
			.build();

	public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder().name("Kafka Topic")
			.displayName("The kafka topic used (Metron sensor name)")
			.description(
					"Which Kafka topic the data is sent to. Note this is usually matched to the name of the sensor in Metron.")
			.required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true)
			.build();

	static final PropertyDescriptor METADATA_WAIT_TIME = new PropertyDescriptor.Builder().name("max.block.ms")
			.displayName("Max Metadata Wait Time")
			.description(
					"The amount of time publisher will wait to obtain metadata or wait for the buffer to flush during the 'send' call before failing the "
							+ "entire 'send' call. Corresponds to Kafka's 'max.block.ms' property")
			.required(true).addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).expressionLanguageSupported(true)
			.defaultValue("5 sec").build();

	static final PropertyDescriptor ACK_WAIT_TIME = new PropertyDescriptor.Builder().name("ack.wait.time")
			.displayName("Acknowledgment Wait Time")
			.description(
					"After sending a message to Kafka, this indicates the amount of time that we are willing to wait for a response from Kafka. "
							+ "If Kafka does not acknowledge the message within this time period, the FlowFile will be routed to 'failure'.")
			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).expressionLanguageSupported(false).required(true)
			.defaultValue("5 secs").build();

	static final PropertyDescriptor MAX_REQUEST_SIZE = new PropertyDescriptor.Builder().name("max.request.size")
			.displayName("Max Request Size")
			.description(
					"The maximum size of a request in bytes. Corresponds to Kafka's 'max.request.size' property and defaults to 1 MB (1048576).")
			.required(true).addValidator(StandardValidators.DATA_SIZE_VALIDATOR).defaultValue("1 MB").build();

	static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("FlowFiles for which all content was sent to Kafka.").build();

	static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("Any FlowFile that cannot be sent to Kafka will be routed to this Relationship").build();
	
	protected static List<PropertyDescriptor> PROPERTIES;
	private static Set<Relationship> RELATIONSHIPS;

	static {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(KafkaProcessorUtils.BOOTSTRAP_SERVERS);
		properties.add(TOPIC);
		properties.add(KafkaProcessorUtils.SECURITY_PROTOCOL);
		properties.add(KafkaProcessorUtils.KERBEROS_CREDENTIALS_SERVICE);
		properties.add(KafkaProcessorUtils.JAAS_SERVICE_NAME);
		properties.add(KafkaProcessorUtils.USER_PRINCIPAL);
		properties.add(KafkaProcessorUtils.USER_KEYTAB);
		properties.add(KafkaProcessorUtils.SSL_CONTEXT_SERVICE);

		properties.add(MAX_REQUEST_SIZE);
		properties.add(ACK_WAIT_TIME);
		properties.add(METADATA_WAIT_TIME);

		PROPERTIES = Collections.unmodifiableList(properties);

		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		RELATIONSHIPS = Collections.unmodifiableSet(relationships);
	}

	private PublisherPool publisherPool;

	@Override
	public Set<Relationship> getRelationships() {
		return RELATIONSHIPS;
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return PROPERTIES;
	}

	@Override
	protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
		return new PropertyDescriptor.Builder()
				.description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
				.name(propertyDescriptorName)
				.addValidator(new KafkaProcessorUtils.KafkaConfigValidator(ProducerConfig.class)).dynamic(true).build();
	}

	@SuppressWarnings("unchecked")
	protected Map<String, String> getMetadata(FlowFile flowFile, String attributesRegex) {
		if (attributesRegex == null || attributesRegex.isEmpty()) {
			return Collections.EMPTY_MAP;
		}
		return StreamSupport.stream(flowFile.getAttributes().entrySet().spliterator(), true)
				.filter(a -> a.getKey().matches(attributesRegex))
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
	}
	
	
	@Override
	protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
		return KafkaProcessorUtils.validateCommonProperties(validationContext);
	}

	@OnStopped
	public void closePool() {
		if (publisherPool != null) {
			publisherPool.close();
		}

		publisherPool = null;		
	}
	
	protected synchronized PublisherPool getPublisherPool(final ProcessContext context) {
		PublisherPool pool = publisherPool;
		if (pool != null) {
			return pool;
		}

		return publisherPool = createPublisherPool(context);
	}

	protected PublisherPool createPublisherPool(final ProcessContext context) {
		final int maxMessageSize = context.getProperty(MAX_REQUEST_SIZE).asDataSize(DataUnit.B).intValue();
		final long maxAckWaitMillis = context.getProperty(ACK_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS)
				.longValue();

		final Map<String, Object> kafkaProperties = new HashMap<>();
		
		// sensible defaults for Metron
		kafkaProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioners.RoundRobinPartitioner.class.getName());
		kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "1");
		
		KafkaProcessorUtils.buildCommonKafkaProperties(context, ProducerConfig.class, kafkaProperties);
		kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		kafkaProperties.put("max.request.size", String.valueOf(maxMessageSize));

		return new PublisherPool(kafkaProperties, getLogger(), maxMessageSize, maxAckWaitMillis);
	}	
}
