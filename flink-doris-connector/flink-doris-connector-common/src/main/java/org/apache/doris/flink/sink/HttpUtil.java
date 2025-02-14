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

package org.apache.doris.flink.sink;

import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestContent;

import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;

/** util to build http client. */
public class HttpUtil {
    private final int connectTimeout;
    private final int socketTimeout;
    private HttpClientBuilder httpClientBuilder;

    public HttpUtil() {
        this.connectTimeout = DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
        this.socketTimeout = DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
        settingStreamHttpClientBuilder();
    }

    public HttpUtil(DorisReadOptions readOptions) {
        this.connectTimeout = readOptions.getRequestConnectTimeoutMs();
        this.socketTimeout = readOptions.getRequestReadTimeoutMs();
        settingStreamHttpClientBuilder();
    }

    private void settingStreamHttpClientBuilder() {
        this.httpClientBuilder =
                HttpClients.custom()
                        // default timeout 3s, maybe report 307 error when fe busy
                        .setRequestExecutor(new HttpRequestExecutor(socketTimeout))
                        .setRedirectStrategy(
                                new DefaultRedirectStrategy() {
                                    @Override
                                    protected boolean isRedirectable(String method) {
                                        return true;
                                    }
                                })
                        .setRetryHandler((exception, executionCount, context) -> false)
                        .setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(connectTimeout)
                                        .setConnectionRequestTimeout(connectTimeout)
                                        .build())
                        .addInterceptorLast(new RequestContent(true));
    }

    /**
     * for stream http
     *
     * @return
     */
    public CloseableHttpClient getHttpClient() {
        return httpClientBuilder.build();
    }

    /**
     * for batch http
     *
     * @return
     */
    public HttpClientBuilder getHttpClientBuilderForBatch() {
        return HttpClients.custom()
                .setRedirectStrategy(
                        new DefaultRedirectStrategy() {
                            @Override
                            protected boolean isRedirectable(String method) {
                                return true;
                            }
                        })
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setConnectTimeout(connectTimeout)
                                .setConnectionRequestTimeout(connectTimeout)
                                .setSocketTimeout(socketTimeout)
                                .build());
    }

    public HttpClientBuilder getHttpClientBuilderForCopyBatch() {
        return HttpClients.custom()
                .disableRedirectHandling()
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setConnectTimeout(connectTimeout)
                                .setConnectionRequestTimeout(connectTimeout)
                                .setSocketTimeout(socketTimeout)
                                .build());
    }
}
