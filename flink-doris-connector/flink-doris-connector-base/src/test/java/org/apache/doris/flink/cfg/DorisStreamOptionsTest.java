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

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class DorisStreamOptionsTest {

    @Test
    public void testLegacyPropertiesUseTlsDefaults() {
        DorisTlsOptions tlsOptions = createOptions(new Properties()).getOptions().getTlsOptions();

        Assert.assertEquals(DorisTlsOptions.disabled(), tlsOptions);
    }

    @Test
    public void testLegacyPropertiesParseTlsOptions() {
        Properties properties = new Properties();
        properties.setProperty(ConfigurationOptions.DORIS_ENABLE_TLS, "true");
        properties.setProperty(ConfigurationOptions.DORIS_TLS_CA_CERTIFICATE_PATH, "certs/ca.pem");
        properties.setProperty(ConfigurationOptions.DORIS_TLS_SKIP_HOSTNAME_VERIFICATION, "true");
        properties.setProperty(
                ConfigurationOptions.DORIS_TLS_EXCLUDED_PROTOCOLS, "mysql,arrowflight");

        DorisTlsOptions tlsOptions = createOptions(properties).getOptions().getTlsOptions();

        Assert.assertTrue(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.HTTP));
        Assert.assertFalse(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
        Assert.assertFalse(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.ARROW_FLIGHT));
        Assert.assertEquals("certs/ca.pem", tlsOptions.getCaCertificatePath());
        Assert.assertTrue(tlsOptions.isSkipHostnameVerification());
    }

    private DorisStreamOptions createOptions(Properties tlsProperties) {
        Properties properties = new Properties();
        properties.setProperty(ConfigurationOptions.DORIS_FENODES, "127.0.0.1:8030");
        properties.putAll(tlsProperties);
        return new DorisStreamOptions(properties);
    }
}
