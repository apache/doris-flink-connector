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

import org.apache.flink.api.connector.sink.Sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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

        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad(dorisOptions, executionOptions, new LabelGenerator("", true), httpClient);
        dorisStreamLoad.setHostPort("local:8040");
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

        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad(dorisOptions, executionOptions, new LabelGenerator("", true), httpClient);
        dorisStreamLoad.setHostPort("local:8040");
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        when(initContext.getRestoredCheckpointId()).thenReturn(OptionalLong.of(1));
        DorisWriter<String> dorisWriter = new DorisWriter<String>(initContext, Collections.emptyList(), new SimpleStringSerializer(), dorisOptions, readOptions, executionOptions);
        dorisWriter.setDorisStreamLoad(dorisStreamLoad);
        List<DorisWriterState> writerStates = dorisWriter.snapshotState(1);

        Assert.assertEquals(1, writerStates.size());
        Assert.assertEquals("doris", writerStates.get(0).getLabelPrefix());
        Assert.assertTrue(dorisWriter.isLoading());
    }
}
