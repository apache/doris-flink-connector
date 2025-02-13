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
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.hamcrest.core.StringStartsWith.startsWith;
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

    @Rule public ExpectedException thrown = ExpectedException.none();

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

        abortSuccessResponse = HttpTestUtil.getResponse(HttpTestUtil.ALREADY_ABORT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(abortSuccessResponse);
        dorisStreamLoad.abortTransaction(anyLong());
    }

    @Test(expected = DorisException.class)
    public void testAbortTransactionError() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortFailedResponse =
                HttpTestUtil.getResponse(HttpTestUtil.ALREADY_ABORT_RESPONSE_ERROR, true);
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

    @Test(expected = DorisException.class)
    public void testAbortTransactionLabelExistNoTxn() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortFailedResponse =
                HttpTestUtil.getResponse(
                        HttpTestUtil.LABEL_EXIST_PRECOMMITTED_NO_TXN_TABLE_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(abortFailedResponse);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("test001", true, "db.tbl", 1),
                        httpClient);
        dorisStreamLoad.abortPreCommit("123", anyLong());
    }

    @Test
    public void testAbortTransactionByLabel() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortSuccessResponse =
                HttpTestUtil.getResponse(HttpTestUtil.ABORT_SUCCESS_RESPONSE_BY_LABEL, true);
        when(httpClient.execute(any())).thenReturn(abortSuccessResponse);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("test001", true),
                        httpClient);
        dorisStreamLoad.abortTransactionByLabel("");
        dorisStreamLoad.abortTransactionByLabel("123");
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

    @Test(expected = DorisException.class)
    public void testAbortTransactionFailedCauseFinished() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortFailedResponse =
                HttpTestUtil.getResponse(HttpTestUtil.LABEL_EXIST_FINISHED_TABLE_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(abortFailedResponse);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("test001", true, "db.tbl", 1),
                        httpClient);
        dorisStreamLoad.abortPreCommit("123", anyLong());
    }

    @Test(expected = Exception.class)
    public void testAbortTransactionFailedError() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortFailedResponse =
                HttpTestUtil.getResponse(HttpTestUtil.ABORT_FAILED_RESPONSE, false);
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

    @Test(expected = Exception.class)
    public void testAbortTransactionFailedBylabel() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortFailedResponse =
                HttpTestUtil.getResponse(HttpTestUtil.ABORT_FAILED_RESPONSE_BY_LABEL, true);
        when(httpClient.execute(any())).thenReturn(abortFailedResponse);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("test001", true),
                        httpClient);
        dorisStreamLoad.abortTransactionByLabel("123");
    }

    @Test(expected = Exception.class)
    public void testAbortTransactionFailedBylabelError() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortFailedResponse =
                HttpTestUtil.getResponse(HttpTestUtil.ABORT_FAILED_RESPONSE_BY_LABEL, false);
        when(httpClient.execute(any())).thenReturn(abortFailedResponse);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("test001", true),
                        httpClient);
        dorisStreamLoad.abortTransactionByLabel("123");
    }

    @Test
    public void testAbortTransactionFailedByAlreadyCommit() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortFailedResponse =
                HttpTestUtil.getResponse(HttpTestUtil.COMMIT_VISIBLE_TXN_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(abortFailedResponse);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("test001", true),
                        httpClient);
        thrown.expect(DorisException.class);
        thrown.expectMessage(startsWith("try abort committed transaction"));
        dorisStreamLoad.abortTransactionByLabel("123");
    }

    @Test
    public void abortLabelExistTransaction() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse abortFailedResponse =
                HttpTestUtil.getResponse(HttpTestUtil.ABORT_FAILED_RESPONSE_BY_LABEL, true);
        when(httpClient.execute(any())).thenReturn(abortFailedResponse);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("test001", true),
                        httpClient);

        dorisStreamLoad.abortLabelExistTransaction(null);
        RespContent respContent = new RespContent();
        dorisStreamLoad.abortLabelExistTransaction(respContent);
        String msg = "Label [123] has already been used, relate to txn [332509]";
        respContent.setMessage(msg);
        dorisStreamLoad.abortLabelExistTransaction(respContent);

        abortFailedResponse = HttpTestUtil.getResponse(HttpTestUtil.ABORT_SUCCESS_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(abortFailedResponse);
        dorisStreamLoad.abortLabelExistTransaction(respContent);
    }

    @Test
    public void testSetHostPort() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        DorisStreamLoad dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        executionOptions,
                        new LabelGenerator("test001", true),
                        httpClient);
        dorisStreamLoad.setHostPort("127.0.0.1:8030");
        Assert.assertEquals(dorisStreamLoad.getHostPort(), "127.0.0.1:8030");
        Properties props = new Properties();
        props.put("format", "arrow");
        DorisExecutionOptions.Builder builder =
                DorisExecutionOptions.builder().setStreamLoadProp(props);
        dorisStreamLoad =
                new DorisStreamLoad(
                        "",
                        dorisOptions,
                        builder.build(),
                        new LabelGenerator("test001", true),
                        httpClient);
        Assert.assertNull(dorisStreamLoad.getLineDelimiter());
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
