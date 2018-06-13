package org.apache.metron.nifi.record;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

public class MetronParserReader extends AbstractControllerService implements RecordReaderFactory {

	@Override
	public RecordReader createRecordReader(Map<String, String> variables, InputStream in, ComponentLog logger)
			throws MalformedRecordException, IOException, SchemaNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}
	
}
