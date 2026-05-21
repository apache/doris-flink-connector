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

import org.apache.doris.flink.cfg.DorisHttpOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestContent;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;

import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.SINK_HTTP_UTF8_CHARSET_DEFAULT;

/** util to build http client. */
public class HttpUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);
    private final int connectTimeout;
    private final int waitForContinueTimeout;
    private final boolean httpUtf8Charset;
    private final DorisHttpOptions httpOptions;
    private HttpClientBuilder httpClientBuilder;
    private static final RedirectStrategy REDIRECT_PUT_STRATEGY =
            new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            };

    public HttpUtil() {
        this(DorisHttpOptions.defaults());
    }

    public HttpUtil(DorisHttpOptions httpOptions) {
        this.connectTimeout = DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
        this.waitForContinueTimeout = DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
        this.httpUtf8Charset = SINK_HTTP_UTF8_CHARSET_DEFAULT;
        this.httpOptions = httpOptions == null ? DorisHttpOptions.defaults() : httpOptions;
        settingStreamHttpClientBuilder();
    }

    public HttpUtil(DorisReadOptions readOptions, boolean httpUtf8Charset) {
        this(readOptions, httpUtf8Charset, DorisHttpOptions.defaults());
    }

    public HttpUtil(
            DorisReadOptions readOptions, boolean httpUtf8Charset, DorisHttpOptions httpOptions) {
        this.connectTimeout = readOptions.getRequestConnectTimeoutMs();
        this.waitForContinueTimeout = readOptions.getRequestConnectTimeoutMs();
        this.httpUtf8Charset = httpUtf8Charset;
        this.httpOptions = httpOptions == null ? DorisHttpOptions.defaults() : httpOptions;
        settingStreamHttpClientBuilder();
    }

    private void settingStreamHttpClientBuilder() {
        ConnectionConfig connectionConfig = ConnectionConfig.DEFAULT;
        if (httpUtf8Charset) {
            connectionConfig =
                    ConnectionConfig.custom()
                            .setCharset(StandardCharsets.UTF_8)
                            .setMalformedInputAction(CodingErrorAction.REPLACE)
                            .setUnmappableInputAction(CodingErrorAction.REPLACE)
                            .build();
        }
        this.httpClientBuilder =
                configureSsl(HttpClients.custom())
                        .setDefaultConnectionConfig(connectionConfig)
                        // default timeout 3s, maybe report 307 error when fe busy
                        .setRequestExecutor(new HttpRequestExecutor(waitForContinueTimeout))
                        .setRedirectStrategy(REDIRECT_PUT_STRATEGY)
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
        ConnectionConfig connectionConfig = ConnectionConfig.DEFAULT;
        if (httpUtf8Charset) {
            connectionConfig =
                    ConnectionConfig.custom()
                            .setCharset(StandardCharsets.UTF_8)
                            .setMalformedInputAction(CodingErrorAction.REPLACE)
                            .setUnmappableInputAction(CodingErrorAction.REPLACE)
                            .build();
        }
        return configureSsl(HttpClients.custom())
                .setDefaultConnectionConfig(connectionConfig)
                .setRedirectStrategy(REDIRECT_PUT_STRATEGY)
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setConnectTimeout(connectTimeout)
                                .setConnectionRequestTimeout(connectTimeout)
                                // todo: Need to be extracted to DorisExecutionOption
                                // default checkpoint timeout is 10min
                                .setSocketTimeout(9 * 60 * 1000)
                                .build());
    }

    public HttpClientBuilder getHttpClientBuilderForCopyBatch() {
        return configureSsl(HttpClients.custom())
                .disableRedirectHandling()
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setConnectTimeout(connectTimeout)
                                .setConnectionRequestTimeout(connectTimeout)
                                // todo: Need to be extracted to DorisExecutionOption
                                // default checkpoint timeout is 10min
                                .setSocketTimeout(9 * 60 * 1000)
                                .build());
    }

    public static HttpClientBuilder buildHttpClientBuilder(DorisHttpOptions httpOptions) {
        return new HttpUtil(httpOptions).newHttpClientBuilder();
    }

    public static RedirectStrategy getRedirectPutStrategy() {
        return REDIRECT_PUT_STRATEGY;
    }

    public static boolean tryHttpConnection(String host, DorisHttpOptions httpOptions) {
        DorisHttpOptions options = httpOptions == null ? DorisHttpOptions.defaults() : httpOptions;
        String url = (options.isEnableHttps() ? "https://" : "http://") + host;
        try (CloseableHttpClient httpClient = buildHttpClientBuilder(options).build()) {
            LOG.debug("try to connect host {}", url);
            HttpGet httpGet = new HttpGet(url);
            httpGet.setConfig(
                    RequestConfig.custom()
                            .setConnectTimeout(60000)
                            .setSocketTimeout(60000)
                            .build());
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                int responseCode = response.getStatusLine().getStatusCode();
                String responseMessage = response.getStatusLine().getReasonPhrase();
                if (responseCode < 500) {
                    // code greater than 500 means a server-side exception.
                    return true;
                }
                LOG.warn(
                        "Failed to connect host {}, responseCode={}, msg={}",
                        url,
                        responseCode,
                        responseMessage);
                return false;
            }
        } catch (Exception ex) {
            LOG.warn("Failed to connect to host:{}", url, ex);
            return false;
        }
    }

    private HttpClientBuilder newHttpClientBuilder() {
        return configureSsl(HttpClients.custom());
    }

    private HttpClientBuilder configureSsl(HttpClientBuilder builder) {
        if (!httpOptions.isEnableHttps()) {
            return builder;
        }
        if (httpOptions.getHttpsKeyStorePath() == null
                || httpOptions.getHttpsKeyStorePath().isEmpty()) {
            return builder;
        }
        try {
            KeyStore keyStore = KeyStore.getInstance(httpOptions.getHttpsKeyStoreType());
            try (FileInputStream input = new FileInputStream(httpOptions.getHttpsKeyStorePath())) {
                String password = httpOptions.getHttpsKeyStorePassword();
                keyStore.load(input, password == null ? null : password.toCharArray());
            }
            SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
            sslContextBuilder.loadTrustMaterial(keyStore, null);
            return builder.setSSLSocketFactory(
                    new SSLConnectionSocketFactory(sslContextBuilder.build()));
        } catch (Exception e) {
            throw new DorisRuntimeException("Failed to build Doris HTTPS client", e);
        }
    }
}
