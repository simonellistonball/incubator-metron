package org.apache.metron.nifi.processors;

import java.io.IOException;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.util.TestRunner;

public class PutMetronEnrichmentRecordTest {
	
	private void generateTestData(TestRunner runner) throws IOException {

        final MockRecordParser parser = new MockRecordParser();
        try {
            runner.addControllerService("parser", parser);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.enableControllerService(parser);
        runner.setProperty(PutMetronEnrichmentRecord.RECORD_READER_FACTORY, "parser");
        
        
        
	}
}
