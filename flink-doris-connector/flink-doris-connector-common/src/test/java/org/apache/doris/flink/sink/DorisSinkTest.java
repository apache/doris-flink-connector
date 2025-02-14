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

package org.apache.doris.flink.sink;

import org.apache.flink.api.connector.sink2.Sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.batch.DorisBatchWriter;
import org.apache.doris.flink.sink.copy.DorisCopyWriter;
import org.apache.doris.flink.sink.writer.DorisAbstractWriter;
import org.apache.doris.flink.sink.writer.DorisWriter;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class DorisSinkTest {

    private MockedStatic<BackendUtil> backendUtilMockedStatic;

    @Before
    public void setUp() throws Exception {
        backendUtilMockedStatic = mockStatic(BackendUtil.class);
        backendUtilMockedStatic.when(() -> BackendUtil.tryHttpConnection(any())).thenReturn(true);
    }

    @Test
    public void testDorisSink() {
        DorisOptions dorisOptions = OptionUtils.buildDorisOptions();
        DorisReadOptions dorisReadOptions = OptionUtils.buildDorisReadOptions();
        DorisRecordSerializer<String> serializer = new SimpleStringSerializer();
        Sink.InitContext initContext = mock(Sink.InitContext.class);

        DorisExecutionOptions dorisExecutionOptions =
                DorisExecutionOptions.builder().disable2PC().build();
        DorisSink<String> dorisSink =
                new DorisSink<String>(
                        dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        DorisAbstractWriter dorisAbstractWriter =
                dorisSink.getDorisAbstractWriter(initContext, Collections.emptyList());
        Assert.assertTrue(dorisAbstractWriter instanceof DorisWriter);

        dorisExecutionOptions =
                DorisExecutionOptions.builder().setBatchMode(true).disable2PC().build();
        dorisSink =
                new DorisSink<String>(
                        dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        dorisAbstractWriter =
                dorisSink.getDorisAbstractWriter(initContext, Collections.emptyList());
        Assert.assertTrue(dorisAbstractWriter instanceof DorisBatchWriter);

        dorisExecutionOptions =
                DorisExecutionOptions.builder().disable2PC().setWriteMode(WriteMode.COPY).build();
        dorisSink =
                new DorisSink<String>(
                        dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        dorisAbstractWriter =
                dorisSink.getDorisAbstractWriter(initContext, Collections.emptyList());
        Assert.assertTrue(dorisAbstractWriter instanceof DorisCopyWriter);
    }

    @After
    public void after() {
        if (backendUtilMockedStatic != null) {
            backendUtilMockedStatic.close();
        }
    }
}
