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

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.CopyLoadException;
import org.apache.doris.flink.sink.HttpEntityMock;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.doris.flink.sink.committer.MockCommitRequest;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicStatusLine;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for Doris Copy Committer. */
public class TestDorisCopyCommitter {

    DorisCopyCommitter copyCommitter;
    DorisCopyCommittable copyCommittable;
    HttpEntityMock entityMock;

    private StatusLine normalLine = new BasicStatusLine(new ProtocolVersion("http", 1, 0), 200, "");
    private StatusLine abnormalLine =
            new BasicStatusLine(new ProtocolVersion("http", 1, 0), 404, "server 404");

    CloseableHttpResponse httpResponse;

    @Before
    public void setUp() throws Exception {
        DorisOptions dorisOptions = OptionUtils.buildDorisOptions();
        copyCommittable = new DorisCopyCommittable("127.0.0.1:8710", "copy into sql");
        HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        when(httpClientBuilder.build()).thenReturn(httpClient);
        entityMock = new HttpEntityMock();
        httpResponse = mock(CloseableHttpResponse.class);
        when(httpClient.execute(any())).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(normalLine);
        when(httpResponse.getEntity()).thenReturn(entityMock);
        copyCommitter = new DorisCopyCommitter(dorisOptions, 1, httpClientBuilder);
    }

    @Test
    public void testCommitted() throws Exception {
        String response =
                "{\"msg\":\"success\",\"code\":0,\"data\":{\"result\":{\"msg\":\"\",\"loadedRows\":\"2\",\"state\":\"FINISHED\",\"type\":\"\",\"filterRows\":\"0\",\"unselectRows\":\"0\",\"url\":null},\"time\":5230,\"type\":\"result_set\"},\"count\":0}";
        this.entityMock.setValue(response);
        final MockCommitRequest<DorisCopyCommittable> request =
                new MockCommitRequest<>(copyCommittable);
        copyCommitter.commit(Collections.singletonList(request));
    }

    @Test(expected = CopyLoadException.class)
    public void testCommitedError() throws Exception {
        String response =
                "{\"msg\":\"success\",\"code\":0,\"data\":{\"result\":{\"msg\":\"errCode = 2, detailMessage = No source file in this table(table).\",\"loadedRows\":\"\",\"state\":\"CANCELLED\",\"type\":\"ETL_RUN_FAIL\",\"filterRows\":\"\",\"unselectRows\":\"\",\"url\":null},\"time\":5255,\"type\":\"result_set\"},\"count\":0}";
        this.entityMock.setValue(response);
        final MockCommitRequest<DorisCopyCommittable> request =
                new MockCommitRequest<>(copyCommittable);
        copyCommitter.commit(Collections.singletonList(request));
    }

    @Test(expected = CopyLoadException.class)
    public void testCommitedError404() throws Exception {
        when(httpResponse.getStatusLine()).thenReturn(abnormalLine);
        when(httpResponse.getEntity()).thenReturn(null);
        final MockCommitRequest<DorisCopyCommittable> request =
                new MockCommitRequest<>(copyCommittable);
        copyCommitter.commit(Collections.singletonList(request));
    }

    @Test(expected = CopyLoadException.class)
    public void testCommitedErrorNullEntity() throws Exception {
        when(httpResponse.getStatusLine()).thenReturn(normalLine);
        when(httpResponse.getEntity()).thenReturn(null);
        final MockCommitRequest<DorisCopyCommittable> request =
                new MockCommitRequest<>(copyCommittable);
        copyCommitter.commit(Collections.singletonList(request));
    }

    @Test
    public void testHandleCommitResponse() throws Exception {
        String loadResult =
                "{\"msg\":\"Error\",\"code\":1,\"data\":\"Failed to execute sql: java.lang.ClassCastException:  java.util.LinkedHashMap$Entry cannot be cast to java.util.HashMap$TreeNode\",\"count\":0}";
        Assert.assertFalse(copyCommitter.handleCommitResponse(loadResult));

        loadResult =
                "{\"msg\":\"Error\",\"code\":0,\"data\":\"Failed to execute sql: java.lang.ClassCastException:  java.util.LinkedHashMap$Entry cannot be cast to java.util.HashMap$TreeNode\",\"count\":0}";
        Assert.assertFalse(copyCommitter.handleCommitResponse(loadResult));

        loadResult =
                "{\"msg\":\"success\",\"code\":0,\"data\":{\"result\":{\"msg\":\"errCode = 2, detailMessage = , host: 10.62.1.219\",\"loadedRows\":\"\",\"id\":\"88a895b14bf84184-9a061fd09f125b10\",\"state\":\"CANCELLED\",\"type\":\"LOAD_RUN_FAIL\",\"filterRows\":\"\",\"unselectRows\":\"\",\"url\":null},\"time\":6098,\"type\":\"result_set\"},\"count\":0} ";
        Assert.assertFalse(copyCommitter.handleCommitResponse(loadResult));

        loadResult = "{\"msg\":\"success\",\"code\":0,\"data\":{\"result\": null},\"count\":0}";
        Assert.assertFalse(copyCommitter.handleCommitResponse(loadResult));

        loadResult = "{\"msg\":\"success\"," + "\"code\":0,\"data\":{\"code\":1},\"count\":0} ";
        Assert.assertFalse(copyCommitter.handleCommitResponse(loadResult));

        loadResult =
                "{\"msg\":\"success\","
                        + "\"code\":0,\"data\":{\"result\":{\"msg\":\"errCode = 2, detailMessage = There is no scanNode Backend available.[2305966: not alive]\",\"loadedRows\":\"\",\"id\":\"c301fd3c88f946f1-98d05"
                        + "4ddae4106ae\",\"state\":\"CANCELLED\",\"type\":\"LOAD_RUN_FAIL\",\"filterRows\":\"\",\"unselectRows\":\"\",\"url\":null},\"time\":10092,\"type\":\"result_set\"},\"count\":0} ";
        Assert.assertFalse(copyCommitter.handleCommitResponse(loadResult));

        loadResult =
                "{\"msg\":\"Error\",\"code\":1,\"data\":\"Failed to execute sql: java.sql.SQLException: (conn=44217) Exception, msg: Node catalog is not ready, please wait for a while.\",\"count\":0}";
        Assert.assertFalse(copyCommitter.handleCommitResponse(loadResult));

        loadResult =
                "{\"msg\":\"success\",\"code\":0,\"data\":{\"result\":{\"msg\":\"\",\"loadedRows\":\"2399\",\"id\":\"31734d4274964740-ac2c022b6dfbf658\",\"state\":\"FINISHED\",\"type\":\"\",\"filterRows\":\"0\",\"unselectRows\":\"0\",\"url\":null},\"time\":54974,\"type\":\"result_set\"},\"count\":0}";
        Assert.assertTrue(copyCommitter.handleCommitResponse(loadResult));

        loadResult =
                "{\"msg\":\"success\",\"code\":0,\"data\":{\"result\":{\"msg\":\"errCode = 2, detailMessage = No files can be copied, matched 1 files, filtered 1 files because files may be loading or loaded\",\"loadedRows\":\"\",\"id\":\"b86cb31213014886-91bc97e8df01676f\",\"state\":\"CANCELLED\",\"type\":\"ETL_RUN_FAIL\",\"filterRows\":\"\",\"unselectRows\":\"\",\"url\":null},\"time\":5019,\"type\":\"result_set\"},\"count\":0}";
        Assert.assertTrue(copyCommitter.handleCommitResponse(loadResult));
    }
}
