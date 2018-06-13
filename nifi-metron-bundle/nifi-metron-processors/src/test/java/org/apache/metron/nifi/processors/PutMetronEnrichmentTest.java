package org.apache.metron.nifi.processors;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.junit.Test;

public class PutMetronEnrichmentTest {

	private static final String DEFAULT_TABLE_NAME = "test";
	private static final String DEFAULT_COLUMN_FAMILY = "t";
	
	static String testCSVConfig = "{\"config\": "
			+ "{\"columns\": {\"host\": 0,\"meta\": 2},\"indicator_column\": \"host\",\"type\": \"threat\",\"separator\": \",\"},"
			+ "\"extractor\": \"CSV\"};";

	
	@Test 
	public void testExtractor() throws InitializationException {
		final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
		
		getHBaseClientService(runner);
	}


	private TestRunner getTestRunner(String defaultTableName, String defaultColumnFamily, String string) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private MockHBaseClientService getHBaseClientService(TestRunner runner) throws InitializationException {
        final MockHBaseClientService hBaseClient = new MockHBaseClientService();
        runner.addControllerService("hbaseClient", hBaseClient);
        runner.enableControllerService(hBaseClient);
        return hBaseClient;
    }
}
