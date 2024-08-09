// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.sink.batch;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBatchBufferStream {

    @Test
    public void testRead() throws Exception {
        BatchRecordBuffer recordBuffer = mockBuffer();
        byte[] expectedData = mergeByteArrays(recordBuffer.getBuffer());
        Assert.assertEquals(recordBuffer.getNumOfRecords(), 1000);

        byte[] actualData = new byte[(int) recordBuffer.getBufferSizeBytes()];
        try (BatchBufferStream inputStream = new BatchBufferStream(recordBuffer.getBuffer())) {
            int len = inputStream.read(actualData, 0, actualData.length);
            assertEquals(actualData.length, len);
            assertArrayEquals(expectedData, actualData);
        }
    }

    @Test
    public void testReadBufLen() throws Exception {
        BatchRecordBuffer recordBuffer = mockBuffer();
        byte[] expectedData = mergeByteArrays(recordBuffer.getBuffer());
        Assert.assertEquals(recordBuffer.getNumOfRecords(), 1000);

        byte[] actualData = new byte[(int) recordBuffer.getBufferSizeBytes()];
        try (BatchBufferStream inputStream = new BatchBufferStream(recordBuffer.getBuffer())) {
            int pos = 0;
            while (pos < actualData.length) {
                // mock random length
                int maxLen = new Random().nextInt(actualData.length - pos) + 1;
                int len = inputStream.read(actualData, pos, maxLen);
                if (len == -1) {
                    break;
                }
                assertTrue(len > 0 && len <= maxLen);
                pos += len;
            }
            assertEquals(actualData.length, pos);
            assertArrayEquals(expectedData, actualData);
        }
    }

    public static BatchRecordBuffer mockBuffer() {
        BatchRecordBuffer recordBuffer = new BatchRecordBuffer();
        for (int i = 0; i < 1000; i++) {
            recordBuffer.insert((UUID.randomUUID() + "," + i).getBytes());
        }
        return recordBuffer;
    }

    public static byte[] mergeByteArrays(List<byte[]> listOfByteArrays) {
        int totalLength = 0;
        for (byte[] byteArray : listOfByteArrays) {
            totalLength += byteArray.length;
        }

        byte[] mergedArray = new byte[totalLength];

        int currentPosition = 0;
        for (byte[] byteArray : listOfByteArrays) {
            System.arraycopy(byteArray, 0, mergedArray, currentPosition, byteArray.length);
            currentPosition += byteArray.length;
        }

        return mergedArray;
    }
}
