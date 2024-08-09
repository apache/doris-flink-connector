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

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBatchBufferHttpEntity {

    @Test
    public void testWrite() throws Exception {
        BatchRecordBuffer recordBuffer = TestBatchBufferStream.mockBuffer();
        byte[] expectedData = TestBatchBufferStream.mergeByteArrays(recordBuffer.getBuffer());
        Assert.assertEquals(recordBuffer.getNumOfRecords(), 1000);

        BatchBufferHttpEntity entity = new BatchBufferHttpEntity(recordBuffer);
        assertTrue(entity.isRepeatable());
        assertFalse(entity.isStreaming());
        assertEquals(entity.getContentLength(), expectedData.length);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        entity.writeTo(outputStream);
        assertArrayEquals(expectedData, outputStream.toByteArray());
    }
}
