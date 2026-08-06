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

package org.apache.doris.flink.container.instance;

import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.cfg.DorisTlsOptions.Protocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DorisCustomerContainerTest {

    private static final String[] TLS_PROPERTIES = {
        "doris_enable_tls",
        "doris_tls_ca_certificate_path",
        "doris_tls_skip_hostname_verification",
        "doris_tls_excluded_protocols"
    };

    private final Map<String, String> originalProperties = new HashMap<>();

    @Before
    public void saveAndClearTlsProperties() {
        for (String property : TLS_PROPERTIES) {
            originalProperties.put(property, System.getProperty(property));
            System.clearProperty(property);
        }
    }

    @After
    public void restoreTlsProperties() {
        for (String property : TLS_PROPERTIES) {
            String originalValue = originalProperties.get(property);
            if (originalValue == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, originalValue);
            }
        }
    }

    @Test
    public void testTlsIsDisabledByDefault() {
        ContainerService container = new DorisCustomerContainer();

        assertEquals(DorisTlsOptions.disabled(), container.getTlsOptions());
    }

    @Test
    public void testTlsOptionsAreLoadedFromSystemProperties() {
        System.setProperty("doris_enable_tls", "true");
        System.setProperty("doris_tls_ca_certificate_path", "/tmp/doris-ca.pem");
        System.setProperty("doris_tls_skip_hostname_verification", "true");
        System.setProperty("doris_tls_excluded_protocols", " arrowflight ");

        DorisTlsOptions tlsOptions = new DorisCustomerContainer().getTlsOptions();

        assertTrue(tlsOptions.isEnabled());
        assertEquals("/tmp/doris-ca.pem", tlsOptions.getCaCertificatePath());
        assertTrue(tlsOptions.isSkipHostnameVerification());
        assertTrue(tlsOptions.isEnabledFor(Protocol.HTTP));
        assertTrue(tlsOptions.isEnabledFor(Protocol.MYSQL));
        assertTrue(tlsOptions.isEnabledFor(Protocol.THRIFT));
        assertFalse(tlsOptions.isEnabledFor(Protocol.ARROW_FLIGHT));
    }
}
