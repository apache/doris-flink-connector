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
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** test for DorisStreamLoad. */
public class TestDorisStreamLoad {
    DorisOptions dorisOptions;
    DorisReadOptions readOptions;
    DorisExecutionOptions executionOptions;

    @Before
    public void setUp() throws Exception {
        dorisOptions = OptionUtils.buildDorisOptions();
        readOptions = OptionUtils.buildDorisReadOptions();
        executionOptions = OptionUtils.buildExecutionOptional();
    }

    @Test
    public void testAbortPreCommit() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse existLabelResponse =
                HttpTestUtil.getResponse(HttpTestUtil.LABEL_EXIST_PRE_COMMIT_TABLE_RESPONSE, true);
        CloseableHttpResponse preCommitResponse =
                HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_TABLE_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(existLabelResponse, preCommitResponse);
        DorisStreamLoad dorisStreamLoad =
                spy(
                        new DorisStreamLoad(
                                "",
                                dorisOptions,
                                executionOptions,
                                new LabelGenerator("test001", true, "db.table", 0),
                                httpClient));

        doNothing().when(dorisStreamLoad).abortTransaction(anyLong());
        dorisStreamLoad.abortPreCommit("test001", 1);
    }

    @Test
    public void testAbortTransaction() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortSuccessResponse =
                HttpTestUtil.getResponse(HttpTestUtil.ABORT_SUCCESS_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(abortSuccessResponse);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("test001", true),
                        httpClient);
        dorisStreamLoad.abortTransaction(anyLong());
    }

    @Test(expected = Exception.class)
    public void testAbortTransactionFailed() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortFailedResponse =
                HttpTestUtil.getResponse(HttpTestUtil.ABORT_FAILED_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(abortFailedResponse);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("test001", true),
                        httpClient);
        dorisStreamLoad.abortTransaction(anyLong());
    }

    @Test
    public void testWriteOneRecordInCsv() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse =
                HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);
        byte[] writeBuffer = "test".getBytes(StandardCharsets.UTF_8);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("", true),
                        httpClient);
        dorisStreamLoad.startLoad("1", false);
        dorisStreamLoad.writeRecord(writeBuffer);
        dorisStreamLoad.stopLoad();
        byte[] buff = new byte[4];
        int n = dorisStreamLoad.getRecordStream().read(buff);
        dorisStreamLoad.getRecordStream().read(new byte[4]);

        Assert.assertEquals(4, n);
        Assert.assertArrayEquals(writeBuffer, buff);
    }

    @Test
    public void testWriteTwoRecordInCsv() throws Exception {
        executionOptions = OptionUtils.buildExecutionOptional();
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse =
                HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);
        byte[] writeBuffer = "test".getBytes(StandardCharsets.UTF_8);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("", true),
                        httpClient);
        dorisStreamLoad.startLoad("1", false);
        dorisStreamLoad.writeRecord(writeBuffer);
        dorisStreamLoad.writeRecord(writeBuffer);
        dorisStreamLoad.stopLoad();
        byte[] buff = new byte[9];
        int n = dorisStreamLoad.getRecordStream().read(buff);
        int ret = dorisStreamLoad.getRecordStream().read(new byte[9]);
        Assert.assertEquals(-1, ret);
        Assert.assertEquals(9, n);
        Assert.assertArrayEquals("test\ntest".getBytes(StandardCharsets.UTF_8), buff);
    }

    @Test
    public void testWriteTwoRecordInJson() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("column_separator", "|");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "json");
        executionOptions = OptionUtils.buildExecutionOptional(properties);
        byte[] expectBuffer = "{\"id\": 1}\n{\"id\": 2}".getBytes(StandardCharsets.UTF_8);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse =
                HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);

        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("", true),
                        httpClient);
        dorisStreamLoad.startLoad("1", false);
        dorisStreamLoad.writeRecord("{\"id\": 1}".getBytes(StandardCharsets.UTF_8));
        dorisStreamLoad.writeRecord("{\"id\": 2}".getBytes(StandardCharsets.UTF_8));
        dorisStreamLoad.stopLoad();
        byte[] buff = new byte[expectBuffer.length];
        int n = dorisStreamLoad.getRecordStream().read(buff);

        int ret = dorisStreamLoad.getRecordStream().read(new byte[4]);
        Assert.assertEquals(-1, ret);
        Assert.assertEquals(expectBuffer.length, n);
        Assert.assertArrayEquals(expectBuffer, buff);
    }
}
