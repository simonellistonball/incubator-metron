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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.metron.stellar.common.StellarProcessor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.MapVariableResolver;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.metron.stellar.dsl.VariableResolver;
import org.apache.metron.stellar.dsl.functions.resolver.FunctionResolver;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

@Tags({ "stellar", "metron", "transform" })
@CapabilityDescription("Can run Stellar transformations on either String based attributes")
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
@SupportsBatching
@SideEffectFree
public class MetronStellar extends AbstractProcessor {

	public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
	public static final String DESTINATION_CONTENT = "flowfile-content";
	public static final String DESTINATION_BOTH = "flowfile-both";

	public static final PropertyDescriptor STELLAR_EXPRESSION = new PropertyDescriptor.Builder().name("STELLAR")
			.displayName("Stellar Statement").description("The stellar statements to execute").required(true)
			.expressionLanguageSupported(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor EXTRA_CLASSPATH = new PropertyDescriptor.Builder().name("EXTRA_CLASSPATH")
			.displayName("Stellar Extensions (urls)")
			.description("Comma-separated list of URL to download stellar plugin jars containing custom functions")
			.required(false).expressionLanguageSupported(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder().name("DESTINATION")
			.displayName("Results destination")
			.allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT, DESTINATION_BOTH)
			.expressionLanguageSupported(false).defaultValue(DESTINATION_ATTRIBUTE).build();

	public static final PropertyDescriptor INCLUDE_CONTENT = new PropertyDescriptor.Builder().name("INCLUDE_CONTENT")
			.displayName("Process content")
			.description("Apply stellar on the content of flow files, or just the attributes of the flowfile")
			.expressionLanguageSupported(false).allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT)
			.defaultValue(DESTINATION_ATTRIBUTE).build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").description("Success")
			.build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	private StellarProcessor processor;

	private FunctionResolver functionResolver;

	private Context stellarContext;
	private Map<String, Object> stellarStatements;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(STELLAR_EXPRESSION);
		descriptors.add(INCLUDE_CONTENT);
		descriptors.add(DESTINATION);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);
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
		stellarContext = (Context) new Context.Builder().build();
		processor = new StellarProcessor();
		functionResolver = StellarFunctions.FUNCTION_RESOLVER();
		String rule = context.getProperty(STELLAR_EXPRESSION).getValue();
		try {
			stellarStatements = new ObjectMapper().readValue(rule, HashMap.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		/*
		 * Stellar variable context includes the attributes of the current flow file and
		 * all properties of the processor this allows for dynamic properties to be fed
		 * in as stellar variables.
		 */
		MapVariableResolver variableResolver = new MapVariableResolver(flowFile.getAttributes(),
				context.getAllProperties());

		final AtomicReference<Map<String, Object>> result = new AtomicReference<Map<String, Object>>();

		Map<String, String> attributes = new HashMap<String, String>();

		if (context.getProperty(INCLUDE_CONTENT).getValue() == DESTINATION_CONTENT) {
			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(InputStream in) throws IOException {
					HashMap variables = new ObjectMapper().readValue(in, HashMap.class);
					variableResolver.add(variables);
					Map<String, Object> results = processStatements(stellarStatements, processor, variables,
							variableResolver, functionResolver, stellarContext);
					result.set(results);
				}
			});
		} else {
			Map<String, Object> results = processStatements(stellarStatements, processor, Collections.EMPTY_MAP,
					variableResolver, functionResolver, stellarContext);
			result.set(results);
		}

		String destination = context.getProperty(DESTINATION).getValue();
		if (destination == DESTINATION_CONTENT || destination == DESTINATION_BOTH) {
			// serialise all the output variables as JSON
			flowFile = session.write(flowFile, new OutputStreamCallback() {
				@Override
				public void process(OutputStream out) throws IOException {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					new ObjectMapper().writeValue(baos, result.get());
					attributes.put("mime.type", "application/json");
					byte[] bytes = baos.toByteArray();
					IOUtils.copy(new ByteArrayInputStream(bytes), out);
				}
			});
		}
		if (destination != DESTINATION_CONTENT) {
			attributes.putAll(Maps.transformValues(result.get(), x -> x.toString()));
		}

		// put the output variables into attributes
		flowFile = session.putAllAttributes(flowFile, attributes);
		session.transfer(flowFile, SUCCESS);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> processStatements(Map<String, Object> stellarStatements,
			StellarProcessor processor, Map variables, VariableResolver variableResolver,
			FunctionResolver functionResolver, Context stellarContext) {
		Map<String, Object> allResults = new HashMap<String, Object>(variables);
		for (Entry<String, Object> kv : stellarStatements.entrySet()) {
			Object o = processor.parse(kv.getValue().toString(), variableResolver, functionResolver, stellarContext);
			if (o != null) {
				if (o instanceof Map) {
					for (Entry<String, String> nkv : ((Map<String, String>) o).entrySet()) {
						allResults.put(nkv.getKey(), nkv.getValue());
					}
				} else {
					if (o instanceof String) {
						allResults.put(kv.getKey(), o.toString());
					} else {
						allResults.put(kv.getKey(), o.toString());
					}
				}
			}
		}
		return allResults;
	}
}
