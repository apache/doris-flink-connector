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

import com.sun.net.httpserver.HttpServer;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.testutil.HttpsTestServer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

public class DorisHttpClientFactoryTest {

    @Test
    public void testApacheClientTrustsConfiguredCa() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            Assert.assertEquals(
                    200,
                    executeApache(
                            server.getUrl("localhost"),
                            tlsOptions(HttpsTestServer.resourcePath("/tls/ca.pem"), false)));
        }
    }

    @Test
    public void testUrlConnectionTrustsConfiguredCa() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            HttpURLConnection connection =
                    DorisHttpClientFactory.openConnection(
                            new URL(server.getUrl("localhost")),
                            tlsOptions(HttpsTestServer.resourcePath("/tls/ca.pem"), false));
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            try {
                Assert.assertEquals(200, connection.getResponseCode());
            } finally {
                connection.disconnect();
            }
        }
    }

    @Test
    public void testWrongCaFailsHandshake() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            assertTlsFailure(
                    server.getUrl("localhost"),
                    tlsOptions(HttpsTestServer.resourcePath("/tls/wrong-ca.pem"), false));
        }
    }

    @Test
    public void testHostnameMismatchFailsByDefault() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            assertTlsFailure(
                    server.getUrl("127.0.0.1"),
                    tlsOptions(HttpsTestServer.resourcePath("/tls/ca.pem"), false));
        }
    }

    @Test
    public void testHostnameMismatchCanBeSkippedWithoutSkippingCa() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            Assert.assertEquals(
                    200,
                    executeApache(
                            server.getUrl("127.0.0.1"),
                            tlsOptions(HttpsTestServer.resourcePath("/tls/ca.pem"), true)));
            assertTlsFailure(
                    server.getUrl("127.0.0.1"),
                    tlsOptions(HttpsTestServer.resourcePath("/tls/wrong-ca.pem"), true));
        }
    }

    @Test
    public void testMultiCertificatePemChainIsAccepted() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            Assert.assertEquals(
                    200,
                    executeApache(
                            server.getUrl("localhost"),
                            tlsOptions(HttpsTestServer.resourcePath("/tls/ca-chain.pem"), false)));
        }
    }

    @Test
    public void testApacheClientRejectsHttpsToHttpRedirect() throws Exception {
        AtomicInteger plaintextRequests = new AtomicInteger();
        HttpServer plaintextServer =
                HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
        plaintextServer.createContext(
                "/",
                exchange -> {
                    plaintextRequests.incrementAndGet();
                    exchange.sendResponseHeaders(200, -1);
                    exchange.close();
                });
        plaintextServer.start();

        try (HttpsTestServer tlsServer = new HttpsTestServer()) {
            tlsServer.redirectTo(
                    "http://127.0.0.1:" + plaintextServer.getAddress().getPort() + "/");
            HttpGet request = new HttpGet(tlsServer.getUrl("localhost"));
            request.setHeader("Authorization", "Basic sensitive");
            try (CloseableHttpClient client =
                    DorisHttpClientFactory.create(
                            tlsOptions(HttpsTestServer.resourcePath("/tls/ca.pem"), false))) {
                try {
                    client.execute(request).close();
                    Assert.fail("Expected HTTPS to HTTP redirect to be rejected");
                } catch (IOException e) {
                    Assert.assertTrue(hasMessage(e, "protocol downgrade"));
                }
            }
            Assert.assertEquals(0, plaintextRequests.get());
        } finally {
            plaintextServer.stop(0);
        }
    }

    private int executeApache(String url, DorisTlsOptions options) throws IOException {
        try (CloseableHttpClient client = DorisHttpClientFactory.create(options);
                CloseableHttpResponse response = client.execute(new HttpGet(url))) {
            return response.getStatusLine().getStatusCode();
        }
    }

    private void assertTlsFailure(String url, DorisTlsOptions options) throws Exception {
        try {
            executeApache(url, options);
            Assert.fail("Expected TLS validation to fail");
        } catch (IOException e) {
            Assert.assertTrue(
                    hasCause(e, SSLHandshakeException.class)
                            || hasCause(e, SSLPeerUnverifiedException.class));
        }
    }

    private boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
        Throwable current = throwable;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean hasMessage(Throwable throwable, String expected) {
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(expected)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private DorisTlsOptions tlsOptions(String caPath, boolean skipHostnameVerification) {
        return DorisTlsOptions.builder()
                .setEnabled(true)
                .setCaCertificatePath(caPath)
                .setSkipHostnameVerification(skipHostnameVerification)
                .build();
    }
}
