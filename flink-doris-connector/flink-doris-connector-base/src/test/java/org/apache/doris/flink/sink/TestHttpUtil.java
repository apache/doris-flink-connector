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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.doris.flink.sink;

import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.testutil.HttpsTestServer;
import org.apache.http.ProtocolException;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHttpResponse;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;

import java.net.URI;

public class TestHttpUtil {

    @Test
    public void testTlsRedirectAllowsHttpsTarget() throws Exception {
        DorisRedirectStrategy strategy = new DorisRedirectStrategy(tlsOptions(""));
        BasicHttpResponse response = redirectResponse("https://be.example:8040/load");

        URI location =
                strategy.getLocationURI(
                        new HttpPut("https://fe.example:8030/load"),
                        response,
                        HttpClientContext.create());

        Assert.assertEquals("https://be.example:8040/load", location.toString());
    }

    @Test
    public void testTlsRedirectRejectsProtocolDowngrade() throws Exception {
        DorisRedirectStrategy strategy = new DorisRedirectStrategy(tlsOptions(""));
        BasicHttpResponse response = redirectResponse("http://be.example:8040/load");

        try {
            strategy.getLocationURI(
                    new HttpPut("https://fe.example:8030/load"),
                    response,
                    HttpClientContext.create());
            Assert.fail("Expected TLS downgrade redirect to fail");
        } catch (ProtocolException e) {
            Assert.assertTrue(e.getMessage().contains("https://fe.example:8030/load"));
            Assert.assertTrue(e.getMessage().contains("http://be.example:8040/load"));
        }
    }

    @Test
    public void testHttpExcludedAllowsPlaintextRedirect() throws Exception {
        DorisRedirectStrategy strategy = new DorisRedirectStrategy(tlsOptions("http"));
        BasicHttpResponse response = redirectResponse("http://be.example:8040/load");

        URI location =
                strategy.getLocationURI(
                        new HttpPut("http://fe.example:8030/load"),
                        response,
                        HttpClientContext.create());

        Assert.assertEquals("http://be.example:8040/load", location.toString());
    }

    @Test
    public void testExternalStorageClientDoesNotInheritDorisCa() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            DorisTlsOptions options =
                    DorisTlsOptions.builder()
                            .setEnabled(true)
                            .setCaCertificatePath(HttpsTestServer.resourcePath("/tls/ca.pem"))
                            .build();
            HttpUtil httpUtil = new HttpUtil(options);
            try (CloseableHttpClient dorisClient =
                            httpUtil.getHttpClientBuilderForCopyBatch().build();
                    CloseableHttpResponse response =
                            dorisClient.execute(new HttpGet(server.getUrl("localhost")))) {
                Assert.assertEquals(200, response.getStatusLine().getStatusCode());
            }

            try (CloseableHttpClient storageClient =
                    httpUtil.getHttpClientBuilderForExternalStorage().build()) {
                try {
                    storageClient.execute(new HttpGet(server.getUrl("localhost"))).close();
                    Assert.fail("Expected the system-trust storage client to reject the Doris CA");
                } catch (Exception e) {
                    Assert.assertTrue(hasCause(e, SSLHandshakeException.class));
                }
            }
        }
    }

    private BasicHttpResponse redirectResponse(String location) {
        BasicHttpResponse response =
                new BasicHttpResponse(new ProtocolVersion("HTTP", 1, 1), 307, "Temporary Redirect");
        response.addHeader("Location", location);
        return response;
    }

    private DorisTlsOptions tlsOptions(String excludedProtocols) {
        return DorisTlsOptions.builder()
                .setEnabled(true)
                .setExcludedProtocols(excludedProtocols)
                .build();
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
}
