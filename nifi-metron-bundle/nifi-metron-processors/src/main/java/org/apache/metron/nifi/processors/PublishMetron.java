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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "metron", "put", "send", "kafka" })
@CapabilityDescription("Publish log data to Apache Metron Kafka")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.", description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
		+ " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
		+ " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration. ")
public class PublishMetron extends AbstractPublishMetron {
	protected static final String MSG_COUNT = "msg.count";

	static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder().name("message-demarcator")
			.displayName("Message Demarcator").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages within "
					+ "a single FlowFile. If not specified, the entire content of the FlowFile will be used as a single message. If specified, the "
					+ "contents of the FlowFile will be split on this delimiter and each section sent as a separate Kafka message. "
					+ "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
			.build();

	private MetronMetadataSerializer metaSerializer = new MetronMetadataSerializer();

	static {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(MESSAGE_DEMARCATOR);
		properties.addAll(AbstractPublishMetron.PROPERTIES);
		PROPERTIES = Collections.unmodifiableList(properties);
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return PROPERTIES;
	}

	@Override
	protected void init(final ProcessorInitializationContext context) {
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final boolean useDemarcator = context.getProperty(MESSAGE_DEMARCATOR).isSet();

		final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(250, DataUnit.KB, 500));
		if (flowFiles.isEmpty()) {
			return;
		}

		final PublisherPool pool = getPublisherPool(context);
		if (pool == null) {
			context.yield();
			return;
		}

		final String securityProtocol = context.getProperty(KafkaProcessorUtils.SECURITY_PROTOCOL).getValue();
		final String bootstrapServers = context.getProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS)
				.evaluateAttributeExpressions().getValue();
		final String attributesRegex = context.getProperty(AbstractPublishMetron.METADATA_ATTRIBUTES).getValue();

		final long startTime = System.nanoTime();
		try (final PublisherLease lease = pool.obtainPublisher()) {
			// Send each FlowFile to Kafka asynchronously.
			for (final FlowFile flowFile : flowFiles) {
				if (!isScheduled()) {
					// If stopped, re-queue FlowFile instead of sending it
					session.transfer(flowFile);
					continue;
				}

				final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();

				final Map<String, String> metronMetadata = getMetadata(flowFile, attributesRegex);
				final byte[] messageKey = metaSerializer.serialize(topic, metronMetadata);

				final byte[] demarcatorBytes;
				if (useDemarcator) {
					demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).evaluateAttributeExpressions(flowFile)
							.getValue().getBytes(StandardCharsets.UTF_8);
				} else {
					demarcatorBytes = null;
				}

				session.read(flowFile, new InputStreamCallback() {
					@Override
					public void process(final InputStream rawIn) throws IOException {
						try (final InputStream in = new BufferedInputStream(rawIn)) {
							lease.publish(flowFile, in, messageKey, demarcatorBytes, topic);
						}
					}
				});
			}

			// Complete the send
			final PublishResult publishResult = lease.complete();

			// Transfer any successful FlowFiles.
			final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
			for (FlowFile success : publishResult.getSuccessfulFlowFiles()) {
				final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(success).getValue();

				final int msgCount = publishResult.getSuccessfulMessageCount(success);
				success = session.putAttribute(success, MSG_COUNT, String.valueOf(msgCount));
				session.adjustCounter("Messages Sent", msgCount, true);

				final String transitUri = KafkaProcessorUtils.buildTransitURI(securityProtocol, bootstrapServers,
						topic);
				session.getProvenanceReporter().send(success, transitUri, "Sent " + msgCount + " messages",
						transmissionMillis);
				session.transfer(success, REL_SUCCESS);
			}

			// Transfer any failures.
			for (final FlowFile failure : publishResult.getFailedFlowFiles()) {
				final int successCount = publishResult.getSuccessfulMessageCount(failure);
				if (successCount > 0) {
					getLogger().error(
							"Failed to send some messages for {} to Kafka, but {} messages were acknowledged by Kafka. Routing to failure due to {}",
							new Object[] { failure, successCount, publishResult.getReasonForFailure(failure) });
				} else {
					getLogger().error("Failed to send all message for {} to Kafka; routing to failure due to {}",
							new Object[] { failure, publishResult.getReasonForFailure(failure) });
				}

				session.transfer(failure, REL_FAILURE);
			}
		}
	}
}
