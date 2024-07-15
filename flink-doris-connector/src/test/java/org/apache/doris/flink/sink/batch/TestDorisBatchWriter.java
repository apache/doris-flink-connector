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

import org.apache.flink.api.connector.sink2.Sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class TestDorisBatchWriter {

    @Rule public ExpectedException thrown = ExpectedException.none();

    private MockedStatic<BackendUtil> backendUtilMockedStatic;

    @Before
    public void setUp() throws Exception {
        backendUtilMockedStatic = mockStatic(BackendUtil.class);
        backendUtilMockedStatic.when(() -> BackendUtil.tryHttpConnection(any())).thenReturn(true);
    }

    @Test
    public void testInit() {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("db.tbl.a")
                        .build();
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("tableIdentifier input error");
        DorisBatchWriter batchWriter = new DorisBatchWriter(null, null, options, null, null);
    }

    @Test
    public void testWriteOneDorisRecord() throws Exception {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("db.tbl")
                        .build();
        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder().build();
        Sink.InitContext context = mock(Sink.InitContext.class);
        SimpleStringSerializer simpleStringSerializer = new SimpleStringSerializer();
        DorisBatchWriter batchWriter =
                new DorisBatchWriter(
                        context, simpleStringSerializer, options, readOptions, executionOptions);
        batchWriter.writeOneDorisRecord(null);
        batchWriter.writeOneDorisRecord(DorisRecord.of(null));
        batchWriter.writeOneDorisRecord(DorisRecord.of("db", "tbl", "zhangsan,1".getBytes()));
        batchWriter.close();
    }

    @After
    public void after() {
        if (backendUtilMockedStatic != null) {
            backendUtilMockedStatic.close();
        }
    }
}
