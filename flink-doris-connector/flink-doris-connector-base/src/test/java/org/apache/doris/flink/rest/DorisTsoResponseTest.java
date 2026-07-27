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

package org.apache.doris.flink.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

class DorisTsoResponseTest {

    private static final Logger LOG = LoggerFactory.getLogger(DorisTsoResponseTest.class);

    @Test
    void extractsOnlyPhysicalTime() {
        String response =
                "{\"code\":0,\"msg\":\"success\",\"data\":{"
                        + "\"current_tso\":461373440032243713,"
                        + "\"current_tso_physical_time\":1760000000123,"
                        + "\"current_tso_logical_counter\":1}}";

        assertThat(RestService.parseCurrentTsoPhysicalTime(response)).isEqualTo(1760000000123L);
    }

    @Test
    void rejectsErrorAndMissingPhysicalTime() {
        assertThatThrownBy(
                        () ->
                                RestService.parseCurrentTsoPhysicalTime(
                                        "{\"code\":1,\"msg\":\"Temporary failure\"}"))
                .hasMessageContaining("Temporary failure");
        assertThatThrownBy(
                        () ->
                                RestService.parseCurrentTsoPhysicalTime(
                                        "{\"code\":0,\"msg\":\"success\",\"data\":{}}"))
                .hasMessageContaining("current_tso_physical_time");
    }

    @Test
    void buildsTimestampFormattingSql() {
        assertThat(RestService.buildCurrentTimestampSql(1760000000123L))
                .isEqualTo("SELECT FROM_UNIXTIME(1760000000123 / 1000, '%Y-%m-%d %H:%i:%s')");
    }

    @Test
    void validatesTimestampFormattingResult() {
        assertThat(RestService.validateCurrentTimestamp("2026-07-20 10:00:00"))
                .isEqualTo("2026-07-20 10:00:00");
        assertThatThrownBy(() -> RestService.validateCurrentTimestamp("2026-07-20 10:00:00.123000"))
                .hasMessageContaining("yyyy-MM-dd HH:mm:ss");
    }

    @Test
    void retriesConfiguredFrontendAndAppliesTimeoutsAfterTsoFailure() throws Exception {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("frontend:8030")
                        .setUsername("root")
                        .setPassword("")
                        .build();
        DorisReadOptions readOptions =
                DorisReadOptions.builder()
                        .setRequestConnectTimeoutMs(1234)
                        .setRequestReadTimeoutMs(2345)
                        .setRequestRetries(2)
                        .build();
        AtomicInteger tsoCalls = new AtomicInteger();
        AtomicInteger formatTimestampCalls = new AtomicInteger();
        ObjectMapper mapper = new ObjectMapper();

        try (MockedStatic<RestService> mocked = mockStatic(RestService.class, CALLS_REAL_METHODS)) {
            mocked.when(() -> RestService.handleResponse(any(), any()))
                    .thenAnswer(
                            invocation -> {
                                HttpRequestBase request = invocation.getArgument(0);
                                assertThat(request.getConfig()).isNotNull();
                                assertThat(request.getConfig().getConnectTimeout()).isEqualTo(1234);
                                assertThat(request.getConfig().getSocketTimeout()).isEqualTo(2345);

                                if (request instanceof HttpPost) {
                                    String statement =
                                            EntityUtils.toString(((HttpPost) request).getEntity());
                                    if (statement.contains("FROM_UNIXTIME")) {
                                        formatTimestampCalls.incrementAndGet();
                                        assertThat(request.getURI().getHost())
                                                .isEqualTo("frontend");
                                        return mapper.readTree(
                                                "{\"code\":0,\"data\":{\"data\":"
                                                        + "[[\"2026-07-20 10:00:00\"]]}}");
                                    }
                                } else if ("/api/tso".equals(request.getURI().getPath())) {
                                    assertThat(request.getURI().getHost()).isEqualTo("frontend");
                                    if (tsoCalls.getAndIncrement() == 0) {
                                        return mapper.readTree(
                                                "{\"code\":1,\"msg\":\"Temporary failure\"}");
                                    }
                                    return mapper.readTree(
                                            "{\"code\":0,\"data\":{"
                                                    + "\"current_tso_physical_time\":"
                                                    + "1760000000123}}");
                                }
                                throw new AssertionError("Unexpected request: " + request.getURI());
                            });

            assertThat(RestService.resolveCurrentTimestamp(options, readOptions, LOG))
                    .isEqualTo("2026-07-20 10:00:00");
        }

        assertThat(tsoCalls.get()).isEqualTo(2);
        assertThat(formatTimestampCalls.get()).isEqualTo(1);
    }
}
