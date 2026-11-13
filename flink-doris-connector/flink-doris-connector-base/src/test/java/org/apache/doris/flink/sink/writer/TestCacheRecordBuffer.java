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

package org.apache.doris.flink.sink.writer;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/** test for CacheRecordBuffer. */
public class TestCacheRecordBuffer {

    @Test
    public void testRead() throws Exception {
        CacheRecordBuffer recordBuffer = new CacheRecordBuffer(16, 4);
        recordBuffer.startBufferData();
        recordBuffer.write("This is Test for CacheRecordBuffer!".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(2, recordBuffer.getReadQueueSize());
        Assert.assertEquals(1, recordBuffer.getWriteQueueSize());
        Assert.assertEquals(0, recordBuffer.getBufferCacheSize());
        byte[] buffer = new byte[16];
        int nRead = recordBuffer.read(buffer);
        Assert.assertEquals(1, recordBuffer.getReadQueueSize());
        Assert.assertEquals(2, recordBuffer.getWriteQueueSize());
        Assert.assertEquals(1, recordBuffer.getBufferCacheSize());
        Assert.assertEquals(16, nRead);
        Assert.assertArrayEquals("This is Test for".getBytes(StandardCharsets.UTF_8), buffer);

        recordBuffer.write("Continue to write the last one.".getBytes(StandardCharsets.UTF_8));
        buffer = new byte[12];
        nRead = recordBuffer.read(buffer);
        Assert.assertEquals(12, nRead);
        Assert.assertArrayEquals(" CacheRecord".getBytes(StandardCharsets.UTF_8), buffer);
        Assert.assertEquals(2, recordBuffer.getReadQueueSize());
        Assert.assertEquals(0, recordBuffer.getWriteQueueSize());
        Assert.assertEquals(1, recordBuffer.getBufferCacheSize());
    }

    @Test
    public void testRecycleCache() throws Exception {
        CacheRecordBuffer recordBuffer = new CacheRecordBuffer(16, 4);
        recordBuffer.startBufferData();
        recordBuffer.write(
                "This is Test for CacheRecordBuffer.recycleCache!"
                        .getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(3, recordBuffer.getReadQueueSize());
        Assert.assertEquals(1, recordBuffer.getWriteQueueSize());
        Assert.assertEquals(0, recordBuffer.getBufferCacheSize());

        byte[] buffer = new byte[50];
        recordBuffer.read(buffer);
        recordBuffer.read(buffer);
        recordBuffer.read(buffer);
        Assert.assertEquals(0, recordBuffer.getReadQueueSize());
        Assert.assertEquals(4, recordBuffer.getWriteQueueSize());
        Assert.assertEquals(3, recordBuffer.getBufferCacheSize());

        recordBuffer.recycleCache();
        Assert.assertEquals(3, recordBuffer.getBufferPoolSize());
        Assert.assertEquals(0, recordBuffer.getBufferCacheSize());
    }
}
