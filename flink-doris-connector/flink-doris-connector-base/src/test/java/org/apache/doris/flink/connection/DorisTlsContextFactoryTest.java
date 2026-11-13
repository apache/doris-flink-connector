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
import org.apache.doris.flink.testutil.HttpsTestServer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.security.KeyStore;

public class DorisTlsContextFactoryTest {

    @Test
    public void testCreateTrustStoreLoadsEveryCertificate() throws Exception {
        String caChain = HttpsTestServer.resourcePath("/tls/ca-chain.pem");

        KeyStore trustStore = DorisTlsContextFactory.createTrustStore(caChain);

        Assert.assertEquals(2, trustStore.size());
    }

    @Test
    public void testSystemDefaultContextDoesNotReadMissingCaFile() {
        DorisTlsOptions options =
                DorisTlsOptions.builder()
                        .setEnabled(false)
                        .setCaCertificatePath("missing-ca.pem")
                        .build();

        Assert.assertNotNull(DorisTlsContextFactory.createSslContext(options));
    }

    @Test
    public void testMissingCaErrorContainsConfiguredAndAbsolutePaths() {
        String configuredPath = "missing-ca.pem";
        DorisTlsOptions options =
                DorisTlsOptions.builder()
                        .setEnabled(true)
                        .setCaCertificatePath(configuredPath)
                        .build();

        try {
            DorisTlsContextFactory.createSslContext(options);
            Assert.fail("Expected the missing CA file to be rejected");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains(configuredPath));
            Assert.assertTrue(
                    e.getMessage().contains(Paths.get(configuredPath).toAbsolutePath().toString()));
        }
    }
}
