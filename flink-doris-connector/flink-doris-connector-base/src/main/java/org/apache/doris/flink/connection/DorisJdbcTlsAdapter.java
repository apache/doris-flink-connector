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

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/** Adapts the shared PEM trust policy to strict MySQL Connector/J TLS properties. */
public final class DorisJdbcTlsAdapter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisJdbcTlsAdapter.class);
    private static final Set<String> MANAGED_PROPERTIES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    "sslmode",
                                    "usessl",
                                    "requiressl",
                                    "verifyservercertificate",
                                    "trustcertificatekeystoreurl",
                                    "trustcertificatekeystoretype",
                                    "trustcertificatekeystorepassword")));

    private final DorisTlsOptions options;
    private final Path trustStorePath;
    private final String trustStorePassword;

    private DorisJdbcTlsAdapter(
            DorisTlsOptions options, Path trustStorePath, String trustStorePassword) {
        this.options = options;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
    }

    public static DorisJdbcTlsAdapter create(DorisTlsOptions options) throws SQLException {
        if (!options.isEnabledFor(DorisTlsOptions.Protocol.MYSQL)
                || StringUtils.isEmpty(options.getCaCertificatePath())) {
            return new DorisJdbcTlsAdapter(options, null, null);
        }

        Path path = null;
        try {
            KeyStore source =
                    DorisTlsContextFactory.createTrustStore(options.getCaCertificatePath());
            KeyStore target = KeyStore.getInstance("PKCS12");
            target.load(null, null);
            Enumeration<String> aliases = source.aliases();
            int index = 0;
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                target.setCertificateEntry("doris-ca-" + index++, source.getCertificate(alias));
            }

            path = Files.createTempFile("doris-jdbc-trust-", ".p12");
            String password = randomPassword();
            try (OutputStream output = Files.newOutputStream(path)) {
                target.store(output, password.toCharArray());
            }
            return new DorisJdbcTlsAdapter(options, path, password);
        } catch (Exception e) {
            if (path != null) {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception cleanupError) {
                    e.addSuppressed(cleanupError);
                }
            }
            throw new SQLException("Unable to prepare the Doris JDBC TLS truststore", e);
        }
    }

    public Properties createConnectionProperties(String username, String password) {
        Properties properties = new Properties();
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }
        if (!options.isEnabledFor(DorisTlsOptions.Protocol.MYSQL)) {
            return properties;
        }

        properties.setProperty(
                "sslMode", options.isSkipHostnameVerification() ? "VERIFY_CA" : "VERIFY_IDENTITY");
        if (trustStorePath != null) {
            properties.setProperty(
                    "trustCertificateKeyStoreUrl", trustStorePath.toUri().toString());
            properties.setProperty("trustCertificateKeyStoreType", "PKCS12");
            properties.setProperty("trustCertificateKeyStorePassword", trustStorePassword);
        }
        return properties;
    }

    public void validateJdbcUrl(String jdbcUrl) throws SQLException {
        if (!options.isEnabledFor(DorisTlsOptions.Protocol.MYSQL) || jdbcUrl == null) {
            return;
        }
        int queryStart = jdbcUrl.indexOf('?');
        if (queryStart < 0 || queryStart == jdbcUrl.length() - 1) {
            return;
        }
        String[] parameters = jdbcUrl.substring(queryStart + 1).split("[&;]");
        for (String parameter : parameters) {
            String name = parameter.split("=", 2)[0].trim().toLowerCase(Locale.ROOT);
            if (MANAGED_PROPERTIES.contains(name)) {
                throw new SQLException(
                        "JDBC URL contains TLS property '"
                                + parameter.split("=", 2)[0]
                                + "' that conflicts with Doris TLS options");
            }
        }
    }

    @Override
    public void close() {
        if (trustStorePath == null) {
            return;
        }
        try {
            Files.deleteIfExists(trustStorePath);
        } catch (Exception e) {
            LOG.warn("Failed to delete temporary Doris JDBC TLS truststore {}", trustStorePath, e);
        }
    }

    private static String randomPassword() {
        byte[] bytes = new byte[24];
        new SecureRandom().nextBytes(bytes);
        StringBuilder password = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            password.append(String.format("%02x", value & 0xff));
        }
        return password.toString();
    }
}
