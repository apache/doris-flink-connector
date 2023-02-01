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

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.models.BackendV2;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * test for DorisWriter.
 */
@Ignore
public class TestDorisWriter {
    DorisOptions dorisOptions;
    DorisReadOptions readOptions;
    DorisExecutionOptions executionOptions;

    @Before
    public void setUp() {
        dorisOptions = OptionUtils.buildDorisOptions();
        readOptions = OptionUtils.buildDorisReadOptions();
        executionOptions = OptionUtils.buildExecutionOptional();
    }

    @Test
    public void testPrepareCommit() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse = HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);

        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad("local:8040", dorisOptions, executionOptions, new LabelGenerator("", true), httpClient);
        dorisStreamLoad.startLoad("");
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        when(initContext.getRestoredCheckpointId()).thenReturn(OptionalLong.of(1));
        DorisWriter<String> dorisWriter = new DorisWriter<String>(initContext, Collections.emptyList(), new SimpleStringSerializer(), dorisOptions, readOptions, executionOptions);
        dorisWriter.setDorisStreamLoad(dorisStreamLoad);
        List<DorisCommittable> committableList = dorisWriter.prepareCommit(true);

        Assert.assertEquals(1, committableList.size());
        Assert.assertEquals("local:8040", committableList.get(0).getHostPort());
        Assert.assertEquals("db_test", committableList.get(0).getDb());
        Assert.assertEquals(2, committableList.get(0).getTxnID());
        Assert.assertFalse(dorisWriter.isLoading());
    }

    @Test
    public void testSnapshot() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse = HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);

        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad("local:8040", dorisOptions, executionOptions, new LabelGenerator("", true), httpClient);
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        when(initContext.getRestoredCheckpointId()).thenReturn(OptionalLong.of(1));
        DorisWriter<String> dorisWriter = new DorisWriter<String>(initContext, Collections.emptyList(), new SimpleStringSerializer(), dorisOptions, readOptions, executionOptions);
        dorisWriter.setDorisStreamLoad(dorisStreamLoad);
        List<DorisWriterState> writerStates = dorisWriter.snapshotState(1);

        Assert.assertEquals(1, writerStates.size());
        Assert.assertEquals("doris", writerStates.get(0).getLabelPrefix());
        Assert.assertTrue(dorisWriter.isLoading());
    }

    @Test
    public void testGetAvailableBackend() throws Exception{
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        DorisWriter<String> dorisWriter = new DorisWriter<String>(initContext, Collections.emptyList(), new SimpleStringSerializer(), dorisOptions, readOptions, executionOptions);
        List<BackendV2.BackendRowV2> backends = Arrays.asList(
                newBackend("127.0.0.1", 8040),
                newBackend("127.0.0.2", 8040),
                newBackend("127.0.0.3", 8040));
        dorisWriter.setBackends(backends);
        Assert.assertEquals(backends.get(0).toBackendString(), dorisWriter.getAvailableBackend());
        Assert.assertEquals(backends.get(1).toBackendString(), dorisWriter.getAvailableBackend());
        Assert.assertEquals(backends.get(2).toBackendString(), dorisWriter.getAvailableBackend());
        Assert.assertEquals(backends.get(0).toBackendString(), dorisWriter.getAvailableBackend());
    }

    @Test
    public void testTryHttpConnection(){
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        DorisWriter<String> dorisWriter = new DorisWriter<String>(initContext, Collections.emptyList(), new SimpleStringSerializer(), dorisOptions, readOptions, executionOptions);
        boolean flag = dorisWriter.tryHttpConnection("127.0.0.1:8040");
        Assert.assertFalse(flag);
    }

    private BackendV2.BackendRowV2 newBackend(String host, int port){
        BackendV2.BackendRowV2 backend = new BackendV2.BackendRowV2();
        backend.setIp(host);
        backend.setHttpPort(port);
        return backend;
    }


}
