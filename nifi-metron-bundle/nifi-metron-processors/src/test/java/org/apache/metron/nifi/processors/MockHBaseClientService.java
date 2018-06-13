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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultHandler;
/**
 * 
 * @author Simon Elliston Ball <simon@simonellistonball.com>
 * @see org.apache.nifi.hbase.MockHBaseClientService for inspiration
 */
public class MockHBaseClientService extends AbstractControllerService implements HBaseClientService {

	@Override
	public void put(String tableName, Collection<PutFlowFile> puts) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void put(String tableName, byte[] rowId, Collection<PutColumn> columns) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean checkAndPut(String tableName, byte[] rowId, byte[] family, byte[] qualifier, byte[] value,
			PutColumn column) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(String tableName, byte[] rowId) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(String tableName, List<byte[]> rowIds) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void scan(String tableName, Collection<Column> columns, String filterExpression, long minTime,
			ResultHandler handler) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void scan(String tableName, byte[] startRow, byte[] endRow, Collection<Column> columns,
			ResultHandler handler) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void scan(String tableName, String startRow, String endRow, String filterExpression, Long timerangeMin,
			Long timerangeMax, Integer limitRows, Boolean isReversed, Collection<Column> columns, ResultHandler handler)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
    public byte[] toBytes(final boolean b) {
        return new byte[] { b ? (byte) -1 : (byte) 0 };
    }

    @Override
    public byte[] toBytes(float f) {
        return toBytes((double)f);
    }

    @Override
    public byte[] toBytes(int i) {
        return toBytes((long)i);
    }

    @Override
    public byte[] toBytes(long l) {
        byte [] b = new byte[8];
        for (int i = 7; i > 0; i--) {
          b[i] = (byte) l;
          l >>>= 8;
        }
        b[0] = (byte) l;
        return b;
    }

    @Override
    public byte[] toBytes(final double d) {
        return toBytes(Double.doubleToRawLongBits(d));
    }

    @Override
    public byte[] toBytes(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] toBytesBinary(String s) {
       return convertToBytesBinary(s);
    }
    private byte[] convertToBytesBinary(String in) {
        byte[] b = new byte[in.length()];
        int size = 0;

        for(int i = 0; i < in.length(); ++i) {
            char ch = in.charAt(i);
            if (ch == '\\' && in.length() > i + 1 && in.charAt(i + 1) == 'x') {
                char hd1 = in.charAt(i + 2);
                char hd2 = in.charAt(i + 3);
                if (isHexDigit(hd1) && isHexDigit(hd2)) {
                    byte d = (byte)((toBinaryFromHex((byte)hd1) << 4) + toBinaryFromHex((byte)hd2));
                    b[size++] = d;
                    i += 3;
                }
            } else {
                b[size++] = (byte)ch;
            }
        }

        byte[] b2 = new byte[size];
        System.arraycopy(b, 0, b2, 0, size);
        return b2;
    }
    
    private static boolean isHexDigit(char c) {
        return c >= 'A' && c <= 'F' || c >= '0' && c <= '9';
    }

    private static byte toBinaryFromHex(byte ch) {
        return ch >= 65 && ch <= 70 ? (byte)(10 + (byte)(ch - 65)) : (byte)(ch - 48);
    }

}
