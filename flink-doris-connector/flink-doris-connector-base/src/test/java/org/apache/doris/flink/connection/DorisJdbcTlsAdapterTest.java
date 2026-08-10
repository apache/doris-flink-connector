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

package org.apache.doris.flink.connection;

import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.testutil.HttpsTestServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.sql.SQLException;
import java.util.Properties;

public class DorisJdbcTlsAdapterTest {

    @Test
    public void testStrictTlsModes() throws Exception {
        try (DorisJdbcTlsAdapter adapter =
                DorisJdbcTlsAdapter.create(DorisTlsOptions.builder().setEnabled(true).build())) {
            Assert.assertEquals(
                    "VERIFY_IDENTITY",
                    adapter.createConnectionProperties("root", "pwd").getProperty("sslMode"));
        }

        try (DorisJdbcTlsAdapter adapter =
                DorisJdbcTlsAdapter.create(
                        DorisTlsOptions.builder()
                                .setEnabled(true)
                                .setSkipHostnameVerification(true)
                                .build())) {
            Assert.assertEquals(
                    "VERIFY_CA",
                    adapter.createConnectionProperties("root", "pwd").getProperty("sslMode"));
        }
    }

    @Test
    public void testDisabledAndExcludedMysqlDoNotSetTlsProperties() throws Exception {
        try (DorisJdbcTlsAdapter disabled = DorisJdbcTlsAdapter.create(DorisTlsOptions.disabled());
                DorisJdbcTlsAdapter excluded =
                        DorisJdbcTlsAdapter.create(
                                DorisTlsOptions.builder()
                                        .setEnabled(true)
                                        .setExcludedProtocols("mysql")
                                        .build())) {
            Assert.assertNull(
                    disabled.createConnectionProperties("root", "pwd").getProperty("sslMode"));
            Assert.assertNull(
                    excluded.createConnectionProperties("root", "pwd").getProperty("sslMode"));
        }
    }

    @Test
    public void testPemCaCreatesAndDeletesPkcs12TrustStore() throws Exception {
        DorisTlsOptions options =
                DorisTlsOptions.builder()
                        .setEnabled(true)
                        .setCaCertificatePath(HttpsTestServer.resourcePath("/tls/ca-chain.pem"))
                        .build();
        Path trustStorePath;
        try (DorisJdbcTlsAdapter adapter = DorisJdbcTlsAdapter.create(options)) {
            Properties properties = adapter.createConnectionProperties("root", "pwd");
            trustStorePath =
                    Paths.get(URI.create(properties.getProperty("trustCertificateKeyStoreUrl")));
            Assert.assertEquals("PKCS12", properties.getProperty("trustCertificateKeyStoreType"));
            Assert.assertTrue(Files.exists(trustStorePath));

            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            try (InputStream input = Files.newInputStream(trustStorePath)) {
                trustStore.load(
                        input,
                        properties.getProperty("trustCertificateKeyStorePassword").toCharArray());
            }
            Assert.assertEquals(2, trustStore.size());
        }
        Assert.assertFalse(Files.exists(trustStorePath));
    }

    @Test
    public void testManagedJdbcUrlTlsPropertiesAreRejected() throws Exception {
        try (DorisJdbcTlsAdapter adapter =
                DorisJdbcTlsAdapter.create(DorisTlsOptions.builder().setEnabled(true).build())) {
            for (String property :
                    new String[] {
                        "sslMode=REQUIRED",
                        "useSSL=true",
                        "requireSSL=true",
                        "verifyServerCertificate=false",
                        "trustCertificateKeyStoreUrl=file:/tmp/trust.p12"
                    }) {
                try {
                    adapter.validateJdbcUrl("jdbc:mysql://localhost:9030/db?" + property);
                    Assert.fail("Expected conflicting JDBC TLS property to fail: " + property);
                } catch (SQLException e) {
                    Assert.assertTrue(e.getMessage().contains(property.split("=")[0]));
                }
            }
        }
    }
}
