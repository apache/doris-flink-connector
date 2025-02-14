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

package org.apache.doris.flink.sink.copy;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.connector.sink2.Sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.doris.flink.sink.TestUtil;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDorisCopyWriter {

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
        HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        when(httpClientBuilder.build()).thenReturn(httpClient);
        CloseableHttpResponse uploadResponse = HttpTestUtil.getResponse("", false, true);
        CloseableHttpResponse preCommitResponse = HttpTestUtil.getResponse("", true, false);
        when(httpClient.execute(any())).thenReturn(uploadResponse).thenReturn(preCommitResponse);
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        // when(initContext.getRestoredCheckpointId()).thenReturn(OptionalLong.of(1));
        DorisCopyWriter<String> copyWriter =
                new DorisCopyWriter<String>(
                        initContext,
                        new SimpleStringSerializer(),
                        dorisOptions,
                        readOptions,
                        executionOptions);
        copyWriter.getBatchStageLoad().setHttpClientBuilder(httpClientBuilder);
        copyWriter.getBatchStageLoad().setCurrentCheckpointID(1);

        TestUtil.waitUntilCondition(
                () -> copyWriter.getBatchStageLoad().isLoadThreadAlive(),
                Deadline.fromNow(Duration.ofSeconds(10)),
                100L,
                "Condition was not met in given timeout.");
        Assert.assertTrue(copyWriter.getBatchStageLoad().isLoadThreadAlive());
        // no data
        Collection<DorisCopyCommittable> committableList = copyWriter.prepareCommit();
        Assert.assertEquals(0, committableList.size());
        // write data
        copyWriter.write("xxx", null);
        copyWriter.flush(true);
        committableList = copyWriter.prepareCommit();
        Assert.assertEquals(1, committableList.size());

        Assert.assertEquals(1, committableList.size());
        DorisCopyCommittable committable = committableList.toArray(new DorisCopyCommittable[0])[0];
        Assert.assertEquals("127.0.0.1:8030", committable.getHostPort());

        Pattern copySql =
                Pattern.compile(
                        "COPY INTO `db`.`table` FROM @~\\('.doris_[0-9a-f]{32}_table_0_1_0}'\\)");
        // todo: compare properties
        Assert.assertTrue(copySql.matcher(committable.getCopySQL()).find());
        copyWriter.close();
    }

    @Test
    public void testSnapshot() throws Exception {
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        // when(initContext.getRestoredCheckpointId()).thenReturn(OptionalLong.of(1));
        DorisCopyWriter<String> copyWriter =
                new DorisCopyWriter<String>(
                        initContext,
                        new SimpleStringSerializer(),
                        dorisOptions,
                        readOptions,
                        executionOptions);
        TestUtil.waitUntilCondition(
                () -> copyWriter.getBatchStageLoad().isLoadThreadAlive(),
                Deadline.fromNow(Duration.ofSeconds(10)),
                100L,
                "Condition was not met in given timeout.");
        Assert.assertTrue(copyWriter.getBatchStageLoad().isLoadThreadAlive());
        List<DorisWriterState> writerStates = copyWriter.snapshotState(1);
        Assert.assertTrue(writerStates.isEmpty());
        copyWriter.close();
    }
}
