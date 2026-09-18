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

import org.apache.flink.api.common.time.Deadline;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.TestUtil;
import org.apache.doris.flink.sink.writer.LabelGenerator;
import org.apache.doris.flink.sink.writer.LoadConstants;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.doris.flink.sink.batch.TestBatchBufferStream.mergeByteArrays;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDorisBatchStreamLoad {

    private static final Logger LOG = LoggerFactory.getLogger(TestDorisBatchStreamLoad.class);

    private MockedStatic<BackendUtil> backendUtilMockedStatic;

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        backendUtilMockedStatic = mockStatic(BackendUtil.class);
        backendUtilMockedStatic.when(() -> BackendUtil.tryHttpConnection(any())).thenReturn(true);
        backendUtilMockedStatic
                .when(() -> BackendUtil.tryHttpConnection(any(), any()))
                .thenReturn(true);
    }

    @Test
    public void testInit() {
        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder().build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setBenodes("127.0.0.1:9030")
                        .setTableIdentifier("a")
                        .build();

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("tableIdentifier input error");
        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options, readOptions, executionOptions, new LabelGenerator("xx", false), 0);
    }

    @Test
    public void testTlsLoadUrl() {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("fe.example:8030")
                        .setBenodes("be.example:8040")
                        .setTableIdentifier("db.tbl")
                        .setTlsOptions(DorisTlsOptions.builder().setEnabled(true).build())
                        .build();
        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options,
                        DorisReadOptions.defaults(),
                        DorisExecutionOptions.builder().build(),
                        new LabelGenerator("label", false),
                        0);
        try {
            Assert.assertEquals(
                    "https://be.example:8040/api/db/tbl/_stream_load", loader.getLoadUrl());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testLoadFail() throws Exception {
        LOG.info("testLoadFail start");
        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setBufferFlushIntervalMs(1000)
                        .setMaxRetries(1)
                        .build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:1")
                        .setBenodes("127.0.0.1:1")
                        .setTableIdentifier("db.tbl")
                        .build();

        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options,
                        readOptions,
                        executionOptions,
                        new LabelGenerator("label", false),
                        0);
        TestUtil.waitUntilCondition(
                () -> loader.isLoadThreadAlive(),
                Deadline.fromNow(Duration.ofSeconds(10)),
                100L,
                "testLoadFail wait loader start failed.");
        Assert.assertTrue(loader.isLoadThreadAlive());
        BackendUtil backendUtil = mock(BackendUtil.class);
        HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse response =
                HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_FAIL_RESPONSE, true);

        loader.setBackendUtil(backendUtil);
        loader.setHttpClientBuilder(httpClientBuilder);
        when(backendUtil.getAvailableBackend()).thenReturn("127.0.0.1:1");
        when(httpClientBuilder.build()).thenReturn(httpClient);
        when(httpClient.execute(any())).thenReturn(response);
        loader.writeRecord("db", "tbl", "1,data".getBytes());

        thrown.expect(Exception.class);
        thrown.expectMessage("stream load error");
        loader.checkpointFlush();
    }

    // ------------------------------------------------------------------------------------------
    // Retry / duplicate-load protection, driven through a scripted mock of the Doris HTTP API.
    // ------------------------------------------------------------------------------------------

    private static final String TOO_MANY_VERSIONS_RESPONSE =
            "{\"TxnId\": 7, \"Label\": \"x\", \"Status\": \"Fail\", "
                    + "\"Message\": \"[INTERNAL_ERROR]tablet error: [E-235]failed to init rowset builder. version count: 2001, exceed limit: 2000\", "
                    + "\"ErrorURL\": \"http://be:8040/api/_load_error_log?file=x\"}";

    private static final String LABEL_EXIST_RUNNING_RESPONSE =
            "{\"TxnId\": -1, \"Label\": \"x\", \"Status\": \"Label Already Exists\", "
                    + "\"ExistingJobStatus\": \"RUNNING\", "
                    + "\"Message\": \"errCode = 2, detailMessage = Label [x] has already been used, relate to txn [42]\"}";

    private static String loadStateResponse(String state) {
        return "{\"msg\": \"success\", \"code\": 0, \"data\": \"" + state + "\", \"count\": 0}";
    }

    /**
     * Scripted Doris: {@code loadOutcomes} are consumed one per stream load PUT (a {@link
     * CloseableHttpResponse} to return or an {@link Exception} to throw; the last one repeats),
     * {@code labelStates} one per get_load_state GET (the last one repeats).
     */
    private static class MockDoris {
        final Deque<Object> loadOutcomes = new ArrayDeque<>();
        final Deque<String> labelStates = new ArrayDeque<>();
        final List<HttpPut> loads = new ArrayList<>();
        final List<HttpGet> polls = new ArrayList<>();

        MockDoris loads(Object... outcomes) {
            for (Object o : outcomes) {
                loadOutcomes.add(o);
            }
            return this;
        }

        MockDoris states(String... states) {
            for (String s : states) {
                labelStates.add(s);
            }
            return this;
        }

        synchronized Object serve(HttpUriRequest request) throws Exception {
            if (request instanceof HttpPut) {
                loads.add((HttpPut) request);
                Object outcome =
                        loadOutcomes.size() > 1 ? loadOutcomes.poll() : loadOutcomes.peek();
                if (outcome instanceof Exception) {
                    throw (Exception) outcome;
                }
                return outcome;
            }
            if (request instanceof HttpGet
                    && request.getURI().getPath().endsWith("/get_load_state")) {
                polls.add((HttpGet) request);
                String state = labelStates.size() > 1 ? labelStates.poll() : labelStates.peek();
                if (state == null) {
                    state = "UNKNOWN";
                }
                return HttpTestUtil.getResponse(loadStateResponse(state), true);
            }
            throw new IllegalStateException("unexpected request " + request);
        }

        List<String> labels() {
            return loads.stream()
                    .map(
                            r ->
                                    r.getFirstHeader("label") == null
                                            ? null
                                            : r.getFirstHeader("label").getValue())
                    .collect(Collectors.toList());
        }

        List<String> polledLabels() {
            return polls.stream()
                    .map(r -> r.getURI().getQuery().replace("label=", ""))
                    .collect(Collectors.toList());
        }
    }

    private DorisBatchStreamLoad newLoader(MockDoris doris, int maxRetries) throws Exception {
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setBufferFlushIntervalMs(1000)
                        .setMaxRetries(maxRetries)
                        .build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:1")
                        .setBenodes("127.0.0.1:1")
                        .setTableIdentifier("db.tbl")
                        .setUsername("root")
                        .setPassword("secret")
                        .build();
        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options,
                        DorisReadOptions.builder().build(),
                        executionOptions,
                        new LabelGenerator("label", false),
                        0);
        TestUtil.waitUntilCondition(
                () -> loader.isLoadThreadAlive(),
                Deadline.fromNow(Duration.ofSeconds(10)),
                100L,
                "wait loader start failed.");

        BackendUtil backendUtil = mock(BackendUtil.class);
        HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        loader.setBackendUtil(backendUtil);
        loader.setHttpClientBuilder(httpClientBuilder);
        loader.setLabelStatePollIntervalMs(10);
        loader.setLabelStatePollTimeoutMs(2000);
        when(backendUtil.getAvailableBackend(anyInt())).thenReturn("127.0.0.1:1");
        when(backendUtil.getAvailableBackend()).thenReturn("127.0.0.1:1");
        when(httpClientBuilder.build()).thenReturn(httpClient);
        when(httpClient.execute(any(HttpUriRequest.class)))
                .thenAnswer(invocation -> doris.serve(invocation.getArgument(0)));
        return loader;
    }

    private static CloseableHttpResponse ok(String body) {
        return HttpTestUtil.getResponse(body, true);
    }

    private static void assertRetryLabels(List<String> labels) {
        Assert.assertFalse(labels.isEmpty());
        Assert.assertNotNull(labels.get(0));
        for (int i = 1; i < labels.size(); i++) {
            Assert.assertEquals(labels.get(0) + "_" + i, labels.get(i));
        }
    }

    @Test
    public void testDorisFailResponseIsRetriedWithNewLabelWithoutPolling() throws Exception {
        // Doris answered (HTTP 200 + Status Fail): nothing was loaded, retry directly.
        MockDoris doris =
                new MockDoris()
                        .loads(
                                ok(TOO_MANY_VERSIONS_RESPONSE),
                                ok(TOO_MANY_VERSIONS_RESPONSE),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE));
        DorisBatchStreamLoad loader = newLoader(doris, 3);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();
            Assert.assertEquals(3, doris.loads.size());
            assertRetryLabels(doris.labels());
            Assert.assertTrue("must not poll after a definitive failure", doris.polls.isEmpty());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testConnectionRefusedIsRetriedWithoutPolling() throws Exception {
        // The request never reached Doris: no need to check the label, retry directly.
        MockDoris doris =
                new MockDoris()
                        .loads(
                                new HttpHostConnectException(
                                        new java.net.ConnectException("Connection refused"), null),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE));
        DorisBatchStreamLoad loader = newLoader(doris, 3);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();
            Assert.assertEquals(2, doris.loads.size());
            assertRetryLabels(doris.labels());
            Assert.assertTrue(doris.polls.isEmpty());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testLostResponseWithVisibleLabelIsNotResent() throws Exception {
        // Doris committed the data but the response was lost (socket timeout). Before this
        // fix the batch was re-sent under label_1 and loaded twice.
        MockDoris doris =
                new MockDoris()
                        .loads(
                                new SocketTimeoutException("Read timed out"),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE))
                        .states("VISIBLE");
        DorisBatchStreamLoad loader = newLoader(doris, 3);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();
            Assert.assertEquals("data must not be sent twice", 1, doris.loads.size());
            Assert.assertEquals(1, doris.polls.size());
            Assert.assertEquals(doris.labels().get(0), doris.polledLabels().get(0));
            Assert.assertEquals(
                    "Basic cm9vdDpzZWNyZXQ=",
                    doris.polls.get(0).getFirstHeader("Authorization").getValue());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testLostResponseWithAbortedLabelIsRetriedWithNewLabel() throws Exception {
        MockDoris doris =
                new MockDoris()
                        .loads(
                                new SocketTimeoutException("Read timed out"),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE))
                        .states("ABORTED");
        DorisBatchStreamLoad loader = newLoader(doris, 3);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();
            Assert.assertEquals(2, doris.loads.size());
            assertRetryLabels(doris.labels());
            Assert.assertEquals(1, doris.polls.size());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testLostResponseWithUnknownLabelIsRetried() throws Exception {
        // Label never registered: the request died before the transaction began.
        MockDoris doris =
                new MockDoris()
                        .loads(
                                new IOException("Connection reset"),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE))
                        .states("UNKNOWN");
        DorisBatchStreamLoad loader = newLoader(doris, 3);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();
            Assert.assertEquals(2, doris.loads.size());
            assertRetryLabels(doris.labels());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testLostResponsePendingLabelIsPolledUntilFinal() throws Exception {
        MockDoris doris =
                new MockDoris()
                        .loads(
                                new SocketTimeoutException("Read timed out"),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE))
                        .states("PREPARE", "PREPARE", "PRECOMMITTED", "COMMITTED");
        DorisBatchStreamLoad loader = newLoader(doris, 3);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();
            Assert.assertEquals(1, doris.loads.size());
            Assert.assertEquals(4, doris.polls.size());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testUnresolvedLabelStateFailsWithoutResend() throws Exception {
        // The label never reaches a final state: fail instead of re-sending, because the data
        // may already be in.
        MockDoris doris =
                new MockDoris()
                        .loads(
                                new SocketTimeoutException("Read timed out"),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE))
                        .states("PREPARE");
        DorisBatchStreamLoad loader = newLoader(doris, 3);
        loader.setLabelStatePollTimeoutMs(200);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            try {
                loader.checkpointFlush();
                Assert.fail("expected the flush to fail");
            } catch (Exception e) {
                Assert.assertTrue(
                        e.getMessage(), e.getMessage().contains("could not be determined"));
            }
            Assert.assertEquals("must not re-send an unresolved label", 1, doris.loads.size());
            Assert.assertTrue(doris.polls.size() >= 2);
        } finally {
            loader.close();
        }
    }

    @Test
    public void testLabelAlreadyExistsFinishedIsTreatedAsLoaded() throws Exception {
        // A previous attempt with this label finished: the data is already in Doris. Before
        // this fix the batch was re-sent under a new label and loaded twice.
        MockDoris doris =
                new MockDoris()
                        .loads(
                                ok(HttpTestUtil.LABEL_EXIST_FINISHED_TABLE_RESPONSE),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE));
        DorisBatchStreamLoad loader = newLoader(doris, 3);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();
            Assert.assertEquals(1, doris.loads.size());
            Assert.assertTrue(doris.polls.isEmpty());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testLabelAlreadyExistsRunningIsPolledThenSuccess() throws Exception {
        MockDoris doris =
                new MockDoris()
                        .loads(
                                ok(LABEL_EXIST_RUNNING_RESPONSE),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE))
                        .states("PREPARE", "VISIBLE");
        DorisBatchStreamLoad loader = newLoader(doris, 3);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();
            Assert.assertEquals(
                    "must not re-send while the label is running", 1, doris.loads.size());
            Assert.assertEquals(2, doris.polls.size());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testLabelAlreadyExistsRunningThenAbortedIsRetried() throws Exception {
        MockDoris doris =
                new MockDoris()
                        .loads(
                                ok(LABEL_EXIST_RUNNING_RESPONSE),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE))
                        .states("ABORTED");
        DorisBatchStreamLoad loader = newLoader(doris, 3);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();
            Assert.assertEquals(2, doris.loads.size());
            assertRetryLabels(doris.labels());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testMaxRetriesStillBoundsRetries() throws Exception {
        MockDoris doris = new MockDoris().loads(ok(TOO_MANY_VERSIONS_RESPONSE));
        DorisBatchStreamLoad loader = newLoader(doris, 1);
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            try {
                loader.checkpointFlush();
                Assert.fail("expected the flush to fail");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("E-235"));
            }
            Assert.assertEquals(2, doris.loads.size());
            assertRetryLabels(doris.labels());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testGroupCommitLostResponseIsRetriedWithoutPolling() throws Exception {
        // Group commit has no label, so the outcome cannot be checked: keep the old behaviour.
        Properties streamProperties = new Properties();
        streamProperties.setProperty(LoadConstants.GROUP_COMMIT, "sync_mode");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setBufferFlushIntervalMs(1000)
                        .setMaxRetries(2)
                        .setStreamLoadProp(streamProperties)
                        .build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:1")
                        .setBenodes("127.0.0.1:1")
                        .setTableIdentifier("db.tbl")
                        .build();
        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options,
                        DorisReadOptions.builder().build(),
                        executionOptions,
                        new LabelGenerator("label", false),
                        0);
        TestUtil.waitUntilCondition(
                () -> loader.isLoadThreadAlive(),
                Deadline.fromNow(Duration.ofSeconds(10)),
                100L,
                "wait loader start failed.");
        MockDoris doris =
                new MockDoris()
                        .loads(
                                new SocketTimeoutException("Read timed out"),
                                ok(HttpTestUtil.COMMIT_TABLE_RESPONSE));
        BackendUtil backendUtil = mock(BackendUtil.class);
        HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        loader.setBackendUtil(backendUtil);
        loader.setHttpClientBuilder(httpClientBuilder);
        when(backendUtil.getAvailableBackend(anyInt())).thenReturn("127.0.0.1:1");
        when(httpClientBuilder.build()).thenReturn(httpClient);
        when(httpClient.execute(any(HttpUriRequest.class)))
                .thenAnswer(invocation -> doris.serve(invocation.getArgument(0)));
        try {
            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();
            Assert.assertEquals(2, doris.loads.size());
            Assert.assertNull(doris.labels().get(0));
            Assert.assertTrue(doris.polls.isEmpty());
        } finally {
            loader.close();
        }
    }

    @Test
    public void testLoadError() throws Exception {
        LOG.info("testLoadError start");
        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder().setBufferFlushIntervalMs(1000).build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:1")
                        .setBenodes("127.0.0.1:1")
                        .setTableIdentifier("db.tbl")
                        .build();

        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options,
                        readOptions,
                        executionOptions,
                        new LabelGenerator("label", false),
                        0);

        TestUtil.waitUntilCondition(
                () -> loader.isLoadThreadAlive(),
                Deadline.fromNow(Duration.ofSeconds(10)),
                100L,
                "testLoadError wait loader start failed.");
        Assert.assertTrue(loader.isLoadThreadAlive());
        BackendUtil backendUtil = mock(BackendUtil.class);
        HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse response = HttpTestUtil.getResponse("server error 404", false);

        loader.setBackendUtil(backendUtil);
        loader.setHttpClientBuilder(httpClientBuilder);
        when(backendUtil.getAvailableBackend()).thenReturn("127.0.0.1:1");
        when(httpClientBuilder.build()).thenReturn(httpClient);
        when(httpClient.execute(any())).thenReturn(response);
        loader.writeRecord("db", "tbl", "1,data".getBytes());

        thrown.expect(Exception.class);
        thrown.expectMessage("stream load error");
        loader.checkpointFlush();
    }

    @Test
    public void testGroupCommitRetryShouldNotSetLabel() throws Exception {
        LOG.info("testGroupCommitRetryShouldNotSetLabel start");
        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        Properties streamProperties = new Properties();
        streamProperties.setProperty(LoadConstants.GROUP_COMMIT, "sync_mode");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setBufferFlushIntervalMs(1000)
                        .setMaxRetries(1)
                        .setStreamLoadProp(streamProperties)
                        .build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:1")
                        .setBenodes("127.0.0.1:1")
                        .setTableIdentifier("db.tbl")
                        .build();

        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options,
                        readOptions,
                        executionOptions,
                        new LabelGenerator("label", false),
                        0);

        try {
            TestUtil.waitUntilCondition(
                    () -> loader.isLoadThreadAlive(),
                    Deadline.fromNow(Duration.ofSeconds(10)),
                    100L,
                    "testGroupCommitRetryShouldNotSetLabel wait loader start failed.");
            Assert.assertTrue(loader.isLoadThreadAlive());

            BackendUtil backendUtil = mock(BackendUtil.class);
            HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
            CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
            CloseableHttpResponse failResponse =
                    HttpTestUtil.getResponse("server error 404", false);
            CloseableHttpResponse successResponse =
                    HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_TABLE_RESPONSE, true);
            ArgumentCaptor<HttpUriRequest> requestCaptor =
                    ArgumentCaptor.forClass(HttpUriRequest.class);

            loader.setBackendUtil(backendUtil);
            loader.setHttpClientBuilder(httpClientBuilder);
            when(backendUtil.getAvailableBackend(anyInt())).thenReturn("127.0.0.1:1");
            when(httpClientBuilder.build()).thenReturn(httpClient);
            when(httpClient.execute(requestCaptor.capture()))
                    .thenReturn(failResponse, successResponse);

            loader.writeRecord("db", "tbl", "1,data".getBytes(StandardCharsets.UTF_8));
            loader.checkpointFlush();

            List<HttpUriRequest> requests = requestCaptor.getAllValues();
            Assert.assertEquals(2, requests.size());
            Assert.assertNull(requests.get(0).getFirstHeader("label"));
            Assert.assertNull(requests.get(1).getFirstHeader("label"));
            Assert.assertNotNull(requests.get(0).getFirstHeader(LoadConstants.GROUP_COMMIT));
            Assert.assertNotNull(requests.get(1).getFirstHeader(LoadConstants.GROUP_COMMIT));
            Assert.assertEquals(
                    "sync_mode",
                    requests.get(0).getFirstHeader(LoadConstants.GROUP_COMMIT).getValue());
            Assert.assertEquals(
                    "sync_mode",
                    requests.get(1).getFirstHeader(LoadConstants.GROUP_COMMIT).getValue());
        } finally {
            loader.close();
        }
    }

    @After
    public void after() {
        if (backendUtilMockedStatic != null) {
            backendUtilMockedStatic.close();
        }
    }

    @Test
    public void mergeBufferTest() {
        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder().build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setBenodes("127.0.0.1:9030")
                        .setTableIdentifier("db.tbl")
                        .build();

        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options, readOptions, executionOptions, new LabelGenerator("xx", false), 0);

        List<BatchRecordBuffer> bufferList = new ArrayList<>();
        BatchRecordBuffer recordBuffer =
                new BatchRecordBuffer("db", "tbl", "\n".getBytes(StandardCharsets.UTF_8), 0);
        recordBuffer.insert("doris,2".getBytes(StandardCharsets.UTF_8));
        recordBuffer.setLabelName("label2");
        BatchRecordBuffer buffer =
                new BatchRecordBuffer("db", "tbl", "\n".getBytes(StandardCharsets.UTF_8), 0);
        buffer.insert("doris,1".getBytes(StandardCharsets.UTF_8));
        buffer.setLabelName("label1");

        boolean flag = loader.mergeBuffer(bufferList, buffer);
        Assert.assertEquals(false, flag);

        bufferList.add(buffer);
        bufferList.add(recordBuffer);
        flag = loader.mergeBuffer(bufferList, buffer);
        Assert.assertEquals(true, flag);
        byte[] bytes = mergeByteArrays(buffer.getBuffer());
        Assert.assertArrayEquals(bytes, "doris,1\ndoris,2".getBytes(StandardCharsets.UTF_8));

        // multi table
        bufferList.clear();
        bufferList.add(buffer);
        BatchRecordBuffer recordBuffer2 =
                new BatchRecordBuffer("db", "tbl2", "\n".getBytes(StandardCharsets.UTF_8), 0);
        recordBuffer2.insert("doris,3".getBytes(StandardCharsets.UTF_8));
        recordBuffer2.setLabelName("label3");
        bufferList.add(recordBuffer2);
        flag = loader.mergeBuffer(bufferList, buffer);
        Assert.assertEquals(false, flag);
    }

    @Test
    public void mergeBufferNullDelimiterTest() {
        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        Properties streamProperties = new Properties();
        streamProperties.setProperty(
                LoadConstants.FORMAT_KEY, LoadConstants.ARROW); // this makes lineDelimiter null
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder().setStreamLoadProp(streamProperties).build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setBenodes("127.0.0.1:9030")
                        .setTableIdentifier("db.tbl")
                        .build();

        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options, readOptions, executionOptions, new LabelGenerator("xx", false), 0);

        List<BatchRecordBuffer> bufferList = new ArrayList<>();
        BatchRecordBuffer recordBuffer = new BatchRecordBuffer("db", "tbl", null, 0);
        recordBuffer.insert("111".getBytes(StandardCharsets.UTF_8));
        recordBuffer.setLabelName("label2");
        BatchRecordBuffer buffer = new BatchRecordBuffer("db", "tbl", null, 0);
        buffer.insert("222".getBytes(StandardCharsets.UTF_8));
        buffer.setLabelName("label1");

        boolean flag = loader.mergeBuffer(bufferList, buffer);
        Assert.assertEquals(false, flag);

        bufferList.add(buffer);
        bufferList.add(recordBuffer);
        flag = loader.mergeBuffer(bufferList, buffer);
        Assert.assertEquals(true, flag);
        byte[] bytes = mergeByteArrays(buffer.getBuffer());
        Assert.assertArrayEquals(bytes, "222111".getBytes(StandardCharsets.UTF_8));

        // multi table
        bufferList.clear();
        bufferList.add(buffer);
        BatchRecordBuffer recordBuffer2 =
                new BatchRecordBuffer("db", "tbl2", "\n".getBytes(StandardCharsets.UTF_8), 0);
        recordBuffer2.insert("333".getBytes(StandardCharsets.UTF_8));
        recordBuffer2.setLabelName("label3");
        bufferList.add(recordBuffer2);
        flag = loader.mergeBuffer(bufferList, buffer);
        Assert.assertEquals(false, flag);
    }
}
