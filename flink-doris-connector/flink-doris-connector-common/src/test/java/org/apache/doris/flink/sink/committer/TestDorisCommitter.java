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

package org.apache.doris.flink.sink.committer;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.BackendV2;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpEntityMock;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/** Test for Doris Committer. */
public class TestDorisCommitter {

    DorisCommitter dorisCommitter;
    DorisCommittable dorisCommittable;
    HttpEntityMock entityMock;
    private MockedStatic<RestService> restServiceMockedStatic;
    private MockedStatic<BackendUtil> backendUtilMockedStatic;
    @Rule public ExpectedException thrown = ExpectedException.none();

    private StatusLine normalLine = new BasicStatusLine(new ProtocolVersion("http", 1, 0), 200, "");
    private StatusLine abnormalLine =
            new BasicStatusLine(new ProtocolVersion("http", 1, 0), 404, "");
    CloseableHttpResponse httpResponse;

    @Before
    public void setUp() throws Exception {
        DorisOptions dorisOptions = OptionUtils.buildDorisOptions();
        DorisReadOptions readOptions = OptionUtils.buildDorisReadOptions();
        DorisExecutionOptions executionOptions = OptionUtils.buildExecutionOptional();
        dorisCommittable = new DorisCommittable("127.0.0.1:8710", "test", 0);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        entityMock = new HttpEntityMock();
        httpResponse = mock(CloseableHttpResponse.class);
        restServiceMockedStatic = mockStatic(RestService.class);
        backendUtilMockedStatic = mockStatic(BackendUtil.class);

        when(httpClient.execute(any())).thenReturn(httpResponse);

        restServiceMockedStatic
                .when(() -> RestService.getBackendsV2(any(), any(), any()))
                .thenReturn(
                        Collections.singletonList(
                                BackendV2.BackendRowV2.of("127.0.0.1", 8040, true)));
        backendUtilMockedStatic.when(() -> BackendUtil.tryHttpConnection(any())).thenReturn(true);

        dorisCommitter =
                new DorisCommitter(dorisOptions, readOptions, executionOptions, httpClient);
    }

    @Test
    public void testCommitted() throws Exception {
        when(httpResponse.getStatusLine()).thenReturn(normalLine);
        when(httpResponse.getEntity()).thenReturn(entityMock);
        String response =
                "{\n"
                        + "\"status\": \"Fail\",\n"
                        + "\"msg\": \"errCode = 2, detailMessage = transaction [2] is already visible, not pre-committed.\"\n"
                        + "}";
        this.entityMock.setValue(response);
        final MockCommitRequest<DorisCommittable> request =
                new MockCommitRequest<>(dorisCommittable);
        dorisCommitter.commit(Collections.singletonList(request));
    }

    @Test
    public void testCommitAbort() throws Exception {
        when(httpResponse.getStatusLine()).thenReturn(normalLine);
        when(httpResponse.getEntity()).thenReturn(entityMock);
        thrown.expect(DorisRuntimeException.class);
        thrown.expectMessage("commit transaction error");

        String response =
                "{\n"
                        + "\"status\": \"Fail\",\n"
                        + "\"msg\": \"errCode = 2, detailMessage = transaction [25] is already aborted. abort reason: User Abort\"\n"
                        + "}";
        this.entityMock.setValue(response);
        final MockCommitRequest<DorisCommittable> request =
                new MockCommitRequest<>(dorisCommittable);
        dorisCommitter.commit(Collections.singletonList(request));
    }

    @Test
    public void testCommitError() throws Exception {
        when(httpResponse.getStatusLine()).thenReturn(abnormalLine);
        when(httpResponse.getEntity()).thenReturn(entityMock);
        thrown.expect(DorisRuntimeException.class);
        thrown.expectMessage("commit transaction error");

        this.entityMock.setValue("404");
        final MockCommitRequest<DorisCommittable> request =
                new MockCommitRequest<>(dorisCommittable);
        dorisCommitter.commit(Collections.singletonList(request));
    }

    @Test
    public void testCommitNullEntity() throws Exception {
        when(httpResponse.getStatusLine()).thenReturn(normalLine);
        when(httpResponse.getEntity()).thenReturn(null);
        thrown.expect(DorisRuntimeException.class);
        thrown.expectMessage("commit transaction error");

        final MockCommitRequest<DorisCommittable> request =
                new MockCommitRequest<>(dorisCommittable);
        dorisCommitter.commit(Collections.singletonList(request));
    }

    @Test
    public void testCommittedRetry() throws Exception {
        DorisOptions dorisOptions = OptionUtils.buildDorisOptions();
        DorisReadOptions readOptions = OptionUtils.buildDorisReadOptions();
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder().setIgnoreCommitError(true).build();
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        HttpEntityMock entityMock = new HttpEntityMock();
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(abnormalLine);
        when(httpResponse.getEntity()).thenReturn(entityMock);
        entityMock.setValue("404");
        when(httpClient.execute(any())).thenReturn(httpResponse);
        dorisCommitter =
                new DorisCommitter(dorisOptions, readOptions, executionOptions, httpClient);
        final MockCommitRequest<DorisCommittable> request =
                new MockCommitRequest<>(dorisCommittable);
        dorisCommitter.commit(Collections.singletonList(request));
    }

    @Test
    public void testClose() {
        DorisOptions dorisOptions = OptionUtils.buildDorisOptions();
        DorisReadOptions readOptions = OptionUtils.buildDorisReadOptions();
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder().setMaxRetries(1).build();

        dorisCommitter = new DorisCommitter(dorisOptions, readOptions, executionOptions, null);
        dorisCommitter.close();
    }

    @After
    public void after() {
        restServiceMockedStatic.close();
        backendUtilMockedStatic.close();
    }
}
