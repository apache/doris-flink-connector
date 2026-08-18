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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.S3TvfOptions;
import org.apache.doris.flink.sink.batch.DorisBatchWriterAdapter;
import org.apache.doris.flink.sink.copy.DorisCopyWriterAdapter;
import org.apache.doris.flink.sink.writer.DorisAbstractWriter;
import org.apache.doris.flink.sink.writer.DorisWriterAdapter;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.doris.flink.sink.writer.tvf.S3TvfCommittableSerializer;
import org.apache.doris.flink.sink.writer.tvf.S3TvfCommitter;
import org.apache.doris.flink.sink.writer.tvf.S3TvfRowDataSerializer;
import org.apache.doris.flink.sink.writer.tvf.S3TvfWriterAdapter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DorisSinkTest {

    private MockedStatic<BackendUtil> backendUtilMockedStatic;

    @Before
    public void setUp() throws Exception {
        backendUtilMockedStatic = mockStatic(BackendUtil.class);
        backendUtilMockedStatic
                .when(() -> BackendUtil.tryHttpConnection(any(), any()))
                .thenReturn(true);
    }

    @Test
    public void testDorisSink() {
        DorisOptions dorisOptions = OptionUtils.buildDorisOptions();
        DorisReadOptions dorisReadOptions = OptionUtils.buildDorisReadOptions();
        DorisRecordSerializer<String> serializer = new SimpleStringSerializer();
        WriterInitContext initContext = mock(WriterInitContext.class);
        TaskInfo taskInfo = mock(TaskInfo.class);
        when(initContext.getTaskInfo()).thenReturn(taskInfo);

        DorisExecutionOptions dorisExecutionOptions =
                DorisExecutionOptions.builder().disable2PC().build();
        DorisSink<String> dorisSink =
                new DorisSink<String>(
                        dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        DorisAbstractWriter dorisAbstractWriter =
                dorisSink.getDorisAbstractWriter(initContext, Collections.emptyList());
        Assert.assertTrue(dorisAbstractWriter instanceof DorisWriterAdapter);

        dorisExecutionOptions =
                DorisExecutionOptions.builder().setBatchMode(true).disable2PC().build();
        dorisSink =
                new DorisSink<String>(
                        dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        dorisAbstractWriter =
                dorisSink.getDorisAbstractWriter(initContext, Collections.emptyList());
        Assert.assertTrue(dorisAbstractWriter instanceof DorisBatchWriterAdapter);

        dorisExecutionOptions =
                DorisExecutionOptions.builder().disable2PC().setWriteMode(WriteMode.COPY).build();
        dorisSink =
                new DorisSink<String>(
                        dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        dorisAbstractWriter =
                dorisSink.getDorisAbstractWriter(initContext, Collections.emptyList());
        Assert.assertTrue(dorisAbstractWriter instanceof DorisCopyWriterAdapter);
    }

    @Test
    public void testTvfWriter() throws Exception {
        DorisOptions dorisOptions = OptionUtils.buildDorisOptions();
        S3TvfOptions s3TvfOptions =
                S3TvfOptions.builder()
                        .setEndpoint("https://s3.example.com")
                        .setRegion("us-east-1")
                        .setBucket("bucket")
                        .setPrefix("prefix")
                        .setAccessKey("ak")
                        .setSecretKey("sk")
                        .build();
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .disable2PC()
                        .setWriteMode(WriteMode.TVF)
                        .setLabelPrefix("label")
                        .setS3TvfOptions(s3TvfOptions)
                        .build();
        S3TvfRowDataSerializer serializer =
                new S3TvfRowDataSerializer(
                        new String[] {"id"},
                        new DataType[] {DataTypes.INT()},
                        Collections.singletonList("id"),
                        false);
        DorisSink<RowData> sink =
                new DorisSink<>(
                        dorisOptions,
                        DorisReadOptions.builder().build(),
                        executionOptions,
                        serializer);
        WriterInitContext initContext = mock(WriterInitContext.class);
        TaskInfo taskInfo = mock(TaskInfo.class);
        when(initContext.getTaskInfo()).thenReturn(taskInfo);

        DorisAbstractWriter writer =
                sink.getDorisAbstractWriter(initContext, Collections.emptyList());

        Assert.assertTrue(writer instanceof S3TvfWriterAdapter);
        Assert.assertTrue(sink.createCommitter(null) instanceof S3TvfCommitter);
        Assert.assertTrue(sink.getCommittableSerializer() instanceof S3TvfCommittableSerializer);
        writer.close();
    }

    @After
    public void after() {
        if (backendUtilMockedStatic != null) {
            backendUtilMockedStatic.close();
        }
    }
}
