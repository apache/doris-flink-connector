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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.LabelAlreadyExistsException;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/** test for DorisWriter. */
public class TestDorisWriter {
    DorisOptions dorisOptions;
    DorisReadOptions readOptions;
    DorisExecutionOptions executionOptions;

    private MockedStatic<BackendUtil> backendUtilMockedStatic;
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        dorisOptions = OptionUtils.buildDorisOptions();
        readOptions = OptionUtils.buildDorisReadOptions();
        executionOptions = OptionUtils.buildExecutionOptional();
        backendUtilMockedStatic = mockStatic(BackendUtil.class);
        backendUtilMockedStatic.when(() -> BackendUtil.tryHttpConnection(any())).thenReturn(true);
    }

    @Test
    public void testPrepareCommit() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse =
                HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);
        DorisWriter<String> dorisWriter = initWriter(httpClient);
        dorisWriter.write("doris,1", null);
        Collection<DorisCommittable> committableList = dorisWriter.prepareCommit();
        Assert.assertEquals(1, committableList.size());
        DorisCommittable dorisCommittable = committableList.stream().findFirst().get();
        Assert.assertEquals("local:8040", dorisCommittable.getHostPort());
        Assert.assertEquals("db", dorisCommittable.getDb());
        Assert.assertEquals(2, dorisCommittable.getTxnID());
        Assert.assertFalse(dorisWriter.isLoading());
    }

    @Test
    public void testPrepareCommitLabelExists() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse =
                HttpTestUtil.getResponse(HttpTestUtil.LABEL_EXIST_PRE_COMMIT_TABLE_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);
        DorisWriter<String> dorisWriter = initWriter(httpClient);
        dorisWriter.write("doris,1", null);
        thrown.expect(LabelAlreadyExistsException.class);
        thrown.expectMessage("Exist label abort finished, retry");
        dorisWriter.prepareCommit();
    }

    @Test
    public void testPrepareCommitLabelExistsNoPrecommit() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse =
                HttpTestUtil.getResponse(
                        HttpTestUtil.LABEL_EXIST_PRE_COMMIT_TABLE_FINISH_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);
        DorisWriter<String> dorisWriter = initWriter(httpClient);
        dorisWriter.write("doris,1", null);
        thrown.expect(DorisRuntimeException.class);
        thrown.expectMessage(contains("stream load error"));
        dorisWriter.prepareCommit();
    }

    @Test
    public void testPrepareCommitLabelExistsNo2PC() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse =
                HttpTestUtil.getResponse(
                        HttpTestUtil.LABEL_EXIST_PRE_COMMIT_TABLE_FINISH_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);
        Map<String, DorisStreamLoad> dorisStreamLoadMap = new ConcurrentHashMap<>();
        Map<String, DorisWriteMetrics> dorisWriteMetricsMap = new ConcurrentHashMap<>();
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        SinkWriterMetricGroup sinkWriterMetricGroup = mock(SinkWriterMetricGroup.class);
        when(initContext.getRestoredCheckpointId()).thenReturn(OptionalLong.of(1));
        when(initContext.getSubtaskId()).thenReturn(1);
        DorisWriteMetrics mockWriteMetrics = getMockWriteMetrics(sinkWriterMetricGroup);
        dorisWriteMetricsMap.put(dorisOptions.getTableIdentifier(), mockWriteMetrics);
        // skip abort in init
        dorisOptions.setTableIdentifier("");
        executionOptions.setEnable2PC(false);
        DorisWriter<String> dorisWriter =
                new DorisWriter<String>(
                        initContext,
                        Collections.emptyList(),
                        new SimpleStringSerializer(),
                        dorisOptions,
                        readOptions,
                        executionOptions);
        dorisWriter.setSinkMetricGroup(sinkWriterMetricGroup);
        dorisOptions.setTableIdentifier("db.table");
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "local:8040",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("", true),
                        httpClient);
        dorisStreamLoadMap.put(dorisOptions.getTableIdentifier(), dorisStreamLoad);
        dorisStreamLoad.startLoad("", false);
        dorisWriter.setDorisStreamLoadMap(dorisStreamLoadMap);
        dorisWriter.setDorisMetricsMap(dorisWriteMetricsMap);
        dorisWriter.write("doris,1", null);
        thrown.expect(DorisRuntimeException.class);
        thrown.expectMessage(contains("stream load error"));
        dorisWriter.prepareCommit();
    }

    @Test
    public void testPrepareCommitFail() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse =
                HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_FAIL_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);
        DorisWriter<String> dorisWriter = initWriter(httpClient);
        dorisWriter.write("doris,1", null);
        thrown.expect(DorisRuntimeException.class);
        thrown.expectMessage(contains("stream load error"));
        dorisWriter.prepareCommit();
    }

    private DorisWriter<String> initWriter(CloseableHttpClient httpClient) throws IOException {
        Map<String, DorisStreamLoad> dorisStreamLoadMap = new ConcurrentHashMap<>();
        Map<String, DorisWriteMetrics> dorisWriteMetricsMap = new ConcurrentHashMap<>();
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        SinkWriterMetricGroup sinkWriterMetricGroup = mock(SinkWriterMetricGroup.class);
        when(initContext.getRestoredCheckpointId()).thenReturn(OptionalLong.of(1));
        when(initContext.getSubtaskId()).thenReturn(1);
        DorisWriteMetrics mockWriteMetrics = getMockWriteMetrics(sinkWriterMetricGroup);
        dorisWriteMetricsMap.put(dorisOptions.getTableIdentifier(), mockWriteMetrics);
        // skip abort in init
        dorisOptions.setTableIdentifier("");
        DorisWriter<String> dorisWriter =
                new DorisWriter<String>(
                        initContext,
                        Collections.emptyList(),
                        new SimpleStringSerializer(),
                        dorisOptions,
                        readOptions,
                        executionOptions);
        dorisWriter.setSinkMetricGroup(sinkWriterMetricGroup);
        dorisOptions.setTableIdentifier("db.table");
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "local:8040",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("", true),
                        httpClient);
        dorisStreamLoadMap.put(dorisOptions.getTableIdentifier(), dorisStreamLoad);
        dorisStreamLoad.startLoad("", false);
        dorisWriter.setDorisStreamLoadMap(dorisStreamLoadMap);
        dorisWriter.setDorisMetricsMap(dorisWriteMetricsMap);
        return dorisWriter;
    }

    @Test
    public void testSnapshot() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse =
                HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);
        DorisWriter<String> dorisWriter = initWriter(httpClient);
        BackendUtil mock = mock(BackendUtil.class);
        when(mock.tryHttpConnection(any())).thenReturn(true);
        dorisWriter.setBackendUtil(mock);
        List<DorisWriterState> writerStates = dorisWriter.snapshotState(1);

        Assert.assertEquals(1, writerStates.size());
        Assert.assertEquals("doris", writerStates.get(0).getLabelPrefix());
        Assert.assertTrue(!dorisWriter.isLoading());
    }

    public DorisWriteMetrics getMockWriteMetrics(SinkWriterMetricGroup sinkWriterMetricGroup) {
        DorisWriteMetrics dorisWriteMetrics =
                new DorisWriteMetrics(sinkWriterMetricGroup, dorisOptions.getTableIdentifier());
        Counter mockCounter = mock(Counter.class);
        Histogram mockHistogram = mock(Histogram.class);
        when(mockCounter.getCount()).thenReturn(0L);
        when(mockHistogram.getCount()).thenReturn(0L);
        dorisWriteMetrics.setTotalFlushLoadBytes(mockCounter);
        dorisWriteMetrics.setTotalFlushNumberTotalRows(mockCounter);
        dorisWriteMetrics.setTotalFlushFilteredRows(mockCounter);
        dorisWriteMetrics.setTotalFlushLoadedRows(mockCounter);
        dorisWriteMetrics.setTotalFlushFailedTimes(mockCounter);
        dorisWriteMetrics.setTotalFlushNumberTotalRows(mockCounter);
        dorisWriteMetrics.setTotalFlushUnselectedRows(mockCounter);
        dorisWriteMetrics.setTotalFlushSucceededTimes(mockCounter);
        dorisWriteMetrics.setTotalFlushTimeMs(mockCounter);
        dorisWriteMetrics.setBeginTxnTimeHistogramMs(mockHistogram);
        dorisWriteMetrics.setCommitAndPublishTimeHistogramMs(mockHistogram);
        dorisWriteMetrics.setWriteDataTimeHistogramMs(mockHistogram);
        dorisWriteMetrics.setStreamLoadPutTimeHistogramMs(mockHistogram);
        dorisWriteMetrics.setWriteDataTimeHistogramMs(mockHistogram);
        dorisWriteMetrics.setCommitAndPublishTimeHistogramMs(mockHistogram);
        dorisWriteMetrics.setReadDataTimeHistogramMs(mockHistogram);
        dorisWriteMetrics.setLoadTimeHistogramMs(mockHistogram);
        return dorisWriteMetrics;
    }

    @After
    public void after() {
        if (backendUtilMockedStatic != null) {
            backendUtilMockedStatic.close();
        }
    }
}
