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

package org.apache.doris.flink.cfg;

import org.apache.doris.flink.sink.writer.WriteMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class DorisExecutionOptionsTest {

    @Test
    public void testBuild() {
        Properties properties = new Properties();
        properties.put("format", "json");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder().setStreamLoadProp(properties).build();
        Properties actual = executionOptions.getStreamLoadProp();

        Properties expected = new Properties();
        expected.put("format", "json");
        expected.put("read_json_by_line", true);
        Assert.assertTrue(actual.size() == 2);
        Assert.assertTrue(actual.get("format").equals(expected.get("format")));
        Assert.assertTrue(
                actual.get("read_json_by_line").equals(expected.get("read_json_by_line")));
    }

    @Test
    public void testEquals() {
        DorisExecutionOptions exceptOptions =
                DorisExecutionOptions.builder()
                        .setBufferCount(3)
                        .setBufferSize(10)
                        .setMaxRetries(1)
                        .setWriteMode(WriteMode.STREAM_LOAD)
                        .setLabelPrefix("doris")
                        .enable2PC()
                        .setBufferFlushMaxBytes(10485760)
                        .setBufferFlushIntervalMs(10000)
                        .setBufferFlushMaxRows(10000)
                        .setCheckInterval(10)
                        .setIgnoreCommitError(true)
                        .setDeletable(true)
                        .setBatchMode(false)
                        .setUseCache(true)
                        .setFlushQueueSize(2)
                        .setIgnoreUpdateBefore(true)
                        .build();

        DorisExecutionOptions.Builder builder =
                DorisExecutionOptions.builder()
                        .setBufferCount(3)
                        .setBufferSize(10)
                        .setMaxRetries(1)
                        .setWriteMode(WriteMode.STREAM_LOAD)
                        .setLabelPrefix("doris")
                        .enable2PC()
                        .setBufferFlushMaxBytes(10485760)
                        .setBufferFlushIntervalMs(10000)
                        .setBufferFlushMaxRows(10000)
                        .setCheckInterval(10)
                        .setIgnoreCommitError(true)
                        .setDeletable(true)
                        .setBatchMode(false)
                        .setUseCache(true)
                        .setFlushQueueSize(2)
                        .setIgnoreUpdateBefore(true);

        Assert.assertNotEquals(exceptOptions, null);
        Assert.assertEquals(exceptOptions, exceptOptions);
        Assert.assertEquals(exceptOptions, builder.build());

        builder.setBufferCount(1);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setBufferCount(3);

        builder.setBufferSize(11);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setBufferSize(10);

        builder.setMaxRetries(11);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setBufferSize(1);

        builder.setWriteMode(WriteMode.STREAM_LOAD_BATCH);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setWriteMode(WriteMode.STREAM_LOAD);

        builder.setLabelPrefix("doris-1");
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setLabelPrefix("doris");

        builder.disable2PC();
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.enable2PC();

        builder.setBufferFlushMaxBytes(104857601);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setBufferFlushMaxBytes(10485760);

        builder.setBufferFlushIntervalMs(100001);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setBufferFlushIntervalMs(10000);

        builder.setBufferFlushMaxRows(10000);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setBufferFlushMaxRows(10000);

        builder.setCheckInterval(11);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setCheckInterval(10);

        builder.setIgnoreUpdateBefore(false);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setIgnoreUpdateBefore(true);

        builder.setDeletable(false);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setDeletable(true);

        builder.setBatchMode(true);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setBatchMode(false);

        builder.setUseCache(false);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setUseCache(true);

        builder.setFlushQueueSize(3);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setFlushQueueSize(2);

        builder.setIgnoreUpdateBefore(false);
        Assert.assertNotEquals(exceptOptions, builder.build());
        builder.setIgnoreUpdateBefore(true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxRetry() {
        DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder().setMaxRetries(-1);
        builder.build();
    }
}
