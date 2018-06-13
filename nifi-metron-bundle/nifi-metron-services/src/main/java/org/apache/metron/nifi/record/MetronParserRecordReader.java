package org.apache.metron.nifi.record;

import java.io.IOException;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

public class MetronParserRecordReader implements RecordReader {

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public RecordSchema getSchema() throws MalformedRecordException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record nextRecord(boolean arg0, boolean arg1) throws IOException, MalformedRecordException {
		// TODO Auto-generated method stub
		return null;
	}

}
