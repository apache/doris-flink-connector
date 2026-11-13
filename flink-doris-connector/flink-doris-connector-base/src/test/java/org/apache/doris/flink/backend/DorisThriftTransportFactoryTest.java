// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.doris.flink.backend;

import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.serialization.Routing;
import org.apache.doris.flink.testutil.HttpsTestServer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class DorisThriftTransportFactoryTest {

    @Test
    public void testTlsTransportUsesConfiguredCaAndMessageLimit() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            DorisReadOptions readOptions =
                    DorisReadOptions.builder().setThriftMaxMessageSize(123456).build();
            DorisTlsOptions tlsOptions = tlsOptions(false);
            TTransport transport =
                    DorisThriftTransportFactory.create(
                            new Routing(server.getEndpoint("localhost")), readOptions, tlsOptions);

            try {
                transport.open();
                Assert.assertTrue(transport.isOpen());
                Assert.assertEquals(123456, transport.getConfiguration().getMaxMessageSize());
            } finally {
                transport.close();
            }
        }
    }

    @Test
    public void testTlsTransportRejectsHostnameMismatch() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            TTransport transport =
                    DorisThriftTransportFactory.create(
                            new Routing(server.getEndpoint("127.0.0.1")),
                            DorisReadOptions.builder().build(),
                            tlsOptions(false));

            try {
                transport.open();
                Assert.fail("Expected hostname verification to reject the certificate");
            } catch (TTransportException expected) {
                Assert.assertFalse(transport.isOpen());
            } finally {
                transport.close();
            }
        }
    }

    @Test
    public void testTlsTransportCanSkipHostnameVerification() throws Exception {
        try (HttpsTestServer server = new HttpsTestServer()) {
            TTransport transport =
                    DorisThriftTransportFactory.create(
                            new Routing(server.getEndpoint("127.0.0.1")),
                            DorisReadOptions.builder().build(),
                            tlsOptions(true));

            try {
                transport.open();
                Assert.assertTrue(transport.isOpen());
            } finally {
                transport.close();
            }
        }
    }

    private DorisTlsOptions tlsOptions(boolean skipHostnameVerification) throws Exception {
        return DorisTlsOptions.builder()
                .setEnabled(true)
                .setCaCertificatePath(HttpsTestServer.resourcePath("/tls/ca.pem"))
                .setSkipHostnameVerification(skipHostnameVerification)
                .build();
    }
}
