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

package org.apache.doris.flink.source.reader;

import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlConnectionProperties;
import org.apache.arrow.flight.Location;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

public class DorisFlightValueReaderTest {

    @Test
    public void testTlsLocationAndRootCertificates() {
        DorisOptions options = options(DorisTlsOptions.builder().setEnabled(true).build());
        InputStream rootCertificates = new ByteArrayInputStream(new byte[] {1, 2, 3});

        Map<String, Object> parameters =
                DorisFlightValueReader.createConnectionParameters(
                        "fe.example", 9040, options, rootCertificates);

        Assert.assertEquals(
                Location.forGrpcTls("fe.example", 9040).getUri().toString(),
                AdbcDriver.PARAM_URI.get(parameters));
        Assert.assertSame(
                rootCertificates, FlightSqlConnectionProperties.TLS_ROOT_CERTS.get(parameters));
        Assert.assertFalse(
                parameters.containsKey(FlightSqlConnectionProperties.TLS_SKIP_VERIFY.getKey()));
    }

    @Test
    public void testSystemTrustDoesNotSetRootCertificates() {
        DorisOptions options = options(DorisTlsOptions.builder().setEnabled(true).build());

        Map<String, Object> parameters =
                DorisFlightValueReader.createConnectionParameters(
                        "fe.example", 9040, options, null);

        Assert.assertFalse(
                parameters.containsKey(FlightSqlConnectionProperties.TLS_ROOT_CERTS.getKey()));
    }

    @Test
    public void testExcludedArrowFlightUsesInsecureLocation() {
        DorisOptions options =
                options(
                        DorisTlsOptions.builder()
                                .setEnabled(true)
                                .setExcludedProtocols("arrowflight")
                                .build());

        Map<String, Object> parameters =
                DorisFlightValueReader.createConnectionParameters(
                        "fe.example", 9040, options, null);

        Assert.assertEquals(
                Location.forGrpcInsecure("fe.example", 9040).getUri().toString(),
                AdbcDriver.PARAM_URI.get(parameters));
    }

    @Test
    public void testHostnameSkipFailsClosedForArrowFlightTls() {
        DorisOptions options =
                options(
                        DorisTlsOptions.builder()
                                .setEnabled(true)
                                .setSkipHostnameVerification(true)
                                .build());

        try {
            DorisFlightValueReader.createConnectionParameters("fe.example", 9040, options, null);
            Assert.fail("Expected unsupported hostname verification policy to fail");
        } catch (DorisRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Arrow Flight"));
            Assert.assertTrue(e.getMessage().contains("hostname verification"));
        }
    }

    private DorisOptions options(DorisTlsOptions tlsOptions) {
        return DorisOptions.builder()
                .setFenodes("fe.example:8030")
                .setUsername("root")
                .setPassword("")
                .setTlsOptions(tlsOptions)
                .build();
    }
}
