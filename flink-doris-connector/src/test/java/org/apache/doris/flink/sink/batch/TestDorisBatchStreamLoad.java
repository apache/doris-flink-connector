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
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.TestUtil;
import org.apache.doris.flink.sink.writer.LabelGenerator;
import org.apache.http.client.methods.CloseableHttpResponse;
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
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
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
                        options, readOptions, executionOptions, new LabelGenerator("xx", false));
    }

    @Test
    public void testLoadFail() throws Exception {
        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder().build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:1")
                        .setBenodes("127.0.0.1:1")
                        .setTableIdentifier("db.tbl")
                        .build();

        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options, readOptions, executionOptions, new LabelGenerator("label", false));
        TestUtil.waitUntilCondition(
                () -> loader.isLoadThreadAlive(),
                Deadline.fromNow(Duration.ofSeconds(10)),
                100L,
                "Condition was not met in given timeout.");
        Assert.assertTrue(loader.isLoadThreadAlive());
        BackendUtil backendUtil = mock(BackendUtil.class);
        HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse response =
                HttpTestUtil.getResponse(HttpTestUtil.LABEL_EXIST_FINISHED_TABLE_RESPONSE, true);

        loader.setBackendUtil(backendUtil);
        loader.setHttpClientBuilder(httpClientBuilder);
        when(backendUtil.getAvailableBackend()).thenReturn("127.0.0.1:1");
        when(httpClientBuilder.build()).thenReturn(httpClient);
        when(httpClient.execute(any())).thenReturn(response);
        loader.writeRecord("db", "tbl", "1,data".getBytes());
        loader.flush("db.tbl", true);

        AtomicReference<Throwable> exception = loader.getException();
        Assert.assertTrue(exception.get() instanceof Exception);
        Assert.assertTrue(exception.get().getMessage().contains("stream load error"));
    }

    @Test
    public void testLoadError() throws Exception {
        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder().build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:1")
                        .setBenodes("127.0.0.1:1")
                        .setTableIdentifier("db.tbl")
                        .build();

        DorisBatchStreamLoad loader =
                new DorisBatchStreamLoad(
                        options, readOptions, executionOptions, new LabelGenerator("label", false));

        TestUtil.waitUntilCondition(
                () -> loader.isLoadThreadAlive(),
                Deadline.fromNow(Duration.ofSeconds(10)),
                100L,
                "Condition was not met in given timeout.");
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
        loader.flush("db.tbl", true);

        AtomicReference<Throwable> exception = loader.getException();

        Assert.assertTrue(exception.get() instanceof Exception);
        Assert.assertTrue(exception.get().getMessage().contains("stream load error"));
    }

    @After
    public void after() {
        if (backendUtilMockedStatic != null) {
            backendUtilMockedStatic.close();
        }
    }
}
