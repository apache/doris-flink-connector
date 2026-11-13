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

package org.apache.doris.flink.connection;

import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.sink.DorisRedirectStrategy;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/** Applies Doris TLS settings to HTTP clients without changing JVM global state. */
public final class DorisHttpClientFactory {

    private DorisHttpClientFactory() {}

    public static HttpClientBuilder configure(HttpClientBuilder builder, DorisTlsOptions options) {
        if (!options.isEnabledFor(DorisTlsOptions.Protocol.HTTP)) {
            return builder;
        }
        SSLContext sslContext = DorisTlsContextFactory.createSslContext(options);
        return builder.setSSLContext(sslContext)
                .setSSLHostnameVerifier(
                        options.isSkipHostnameVerification()
                                ? NoopHostnameVerifier.INSTANCE
                                : SSLConnectionSocketFactory.getDefaultHostnameVerifier())
                .setRedirectStrategy(new DorisRedirectStrategy(options));
    }

    public static CloseableHttpClient create(DorisTlsOptions options) {
        return configure(HttpClients.custom(), options).build();
    }

    public static HttpURLConnection openConnection(URL url, DorisTlsOptions options)
            throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        if (!(connection instanceof HttpsURLConnection)
                || !options.isEnabledFor(DorisTlsOptions.Protocol.HTTP)) {
            return connection;
        }

        HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
        httpsConnection.setSSLSocketFactory(
                DorisTlsContextFactory.createSslContext(options).getSocketFactory());
        HostnameVerifier hostnameVerifier =
                options.isSkipHostnameVerification()
                        ? (hostname, session) -> true
                        : HttpsURLConnection.getDefaultHostnameVerifier();
        httpsConnection.setHostnameVerifier(hostnameVerifier);
        return httpsConnection;
    }
}
