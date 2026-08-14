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

package org.apache.doris.flink.cfg;

import org.apache.flink.configuration.Configuration;

import org.apache.doris.flink.table.DorisConfigOptions;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class DorisTlsOptionsTest {

    @Test
    public void testDefaultOptionsDisableEveryProtocol() {
        DorisTlsOptions options = DorisTlsOptions.disabled();

        Assert.assertFalse(options.isEnabled());
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.HTTP));
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.THRIFT));
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.ARROW_FLIGHT));
        Assert.assertEquals("", options.getCaCertificatePath());
        Assert.assertFalse(options.isSkipHostnameVerification());
        Assert.assertTrue(options.getExcludedProtocols().isEmpty());
    }

    @Test
    public void testExcludedProtocolsAreTrimmedAndCaseInsensitive() {
        DorisTlsOptions options =
                DorisTlsOptions.builder()
                        .setEnabled(true)
                        .setCaCertificatePath("doris-tls/ca.pem")
                        .setSkipHostnameVerification(true)
                        .setExcludedProtocols(" MYSQL, thrift, arrowFlight ")
                        .build();

        Assert.assertTrue(options.isEnabledFor(DorisTlsOptions.Protocol.HTTP));
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.THRIFT));
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.ARROW_FLIGHT));
        Assert.assertEquals("doris-tls/ca.pem", options.getCaCertificatePath());
        Assert.assertTrue(options.isSkipHostnameVerification());
    }

    @Test
    public void testUnknownExcludedProtocolIsRejected() {
        try {
            DorisTlsOptions.builder().setEnabled(true).setExcludedProtocols("http,smtp").build();
            Assert.fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("smtp"));
        }
    }

    @Test
    public void testReadableConfigCreatesEquivalentOptions() {
        Configuration configuration = new Configuration();
        configuration.set(DorisConfigOptions.DORIS_ENABLE_TLS, true);
        configuration.set(DorisConfigOptions.DORIS_TLS_CA_CERTIFICATE_PATH, "certs/ca.pem");
        configuration.set(DorisConfigOptions.DORIS_TLS_SKIP_HOSTNAME_VERIFICATION, true);
        configuration.set(DorisConfigOptions.DORIS_TLS_EXCLUDED_PROTOCOLS, "mysql");

        DorisTlsOptions expected =
                DorisTlsOptions.builder()
                        .setEnabled(true)
                        .setCaCertificatePath("certs/ca.pem")
                        .setSkipHostnameVerification(true)
                        .setExcludedProtocols("mysql")
                        .build();

        Assert.assertEquals(expected, DorisConfigOptions.getTlsOptions(configuration));
    }

    @Test
    public void testSerializationRoundTrip() throws Exception {
        DorisTlsOptions options =
                DorisTlsOptions.builder()
                        .setEnabled(true)
                        .setCaCertificatePath("certs/ca-chain.pem")
                        .setExcludedProtocols("arrowflight")
                        .build();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(options);
        }

        DorisTlsOptions restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (DorisTlsOptions) input.readObject();
        }

        Assert.assertEquals(options, restored);
        Assert.assertEquals(options.hashCode(), restored.hashCode());
    }
}
