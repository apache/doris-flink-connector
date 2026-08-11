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

package org.apache.doris;

import org.apache.doris.flink.cfg.DorisTlsOptions;

import java.util.HashSet;
import java.util.Set;

final class DorisArguments {
    private static final String FE_ADDRESS = "--doris-fe-address";
    private static final String TABLE_IDENTIFIER = "--doris-table-identifier";
    private static final String DATABASE = "--doris-database";
    private static final String USER = "--doris-user";
    private static final String PASSWORD = "--doris-password";
    private static final String ENABLE_TLS = "--doris-enable-tls";
    private static final String CA_CERTIFICATE_PATH = "--doris-tls-ca-certificate-path";
    private static final String SKIP_HOSTNAME_VERIFICATION =
            "--doris-tls-skip-hostname-verification";
    private static final String EXCLUDED_PROTOCOLS = "--doris-tls-excluded-protocols";

    private String feAddress;
    private String tableIdentifier;
    private String database;
    private String user;
    private String password;
    private boolean tlsEnabled;
    private String caCertificatePath = "";
    private boolean skipHostnameVerification;
    private String excludedProtocols = "";

    static DorisArguments parse(String[] args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be name-value pairs");
        }

        DorisArguments arguments = new DorisArguments();
        Set<String> parsedOptions = new HashSet<>();
        for (int i = 0; i < args.length; i += 2) {
            String name = args[i];
            if (!parsedOptions.add(name)) {
                throw new IllegalArgumentException("Duplicate argument: " + name);
            }
            String value = args[i + 1];
            switch (name) {
                case FE_ADDRESS:
                    arguments.feAddress = value;
                    break;
                case TABLE_IDENTIFIER:
                    arguments.tableIdentifier = value;
                    break;
                case DATABASE:
                    arguments.database = value;
                    break;
                case USER:
                    arguments.user = value;
                    break;
                case PASSWORD:
                    arguments.password = value;
                    break;
                case ENABLE_TLS:
                    arguments.tlsEnabled = parseBoolean(name, value);
                    break;
                case CA_CERTIFICATE_PATH:
                    arguments.caCertificatePath = value;
                    break;
                case SKIP_HOSTNAME_VERIFICATION:
                    arguments.skipHostnameVerification = parseBoolean(name, value);
                    break;
                case EXCLUDED_PROTOCOLS:
                    arguments.excludedProtocols = value;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + name);
            }
        }
        return arguments;
    }

    String getFeAddress() {
        return requireOption(FE_ADDRESS, feAddress);
    }

    String getTableIdentifier() {
        return requireOption(TABLE_IDENTIFIER, tableIdentifier);
    }

    String getDatabase() {
        return requireOption(DATABASE, database);
    }

    String getUser() {
        return requireOption(USER, user);
    }

    String getPassword() {
        return requireOption(PASSWORD, password);
    }

    DorisTlsOptions toDorisTlsOptions() {
        return DorisTlsOptions.builder()
                .setEnabled(tlsEnabled)
                .setCaCertificatePath(caCertificatePath)
                .setSkipHostnameVerification(skipHostnameVerification)
                .setExcludedProtocols(excludedProtocols)
                .build();
    }

    String toFlinkSqlOptions() {
        if (!tlsEnabled) {
            return "";
        }
        return ",\n  'doris.enable.tls' = 'true'"
                + ",\n  'doris.tls.ca-certificate-path' = '"
                + escapeSqlLiteral(caCertificatePath)
                + "'"
                + ",\n  'doris.tls.skip-hostname-verification' = '"
                + skipHostnameVerification
                + "'"
                + ",\n  'doris.tls.excluded-protocols' = '"
                + escapeSqlLiteral(excludedProtocols)
                + "'";
    }

    private static String requireOption(String name, String value) {
        if (value == null) {
            throw new IllegalArgumentException("Missing required argument: " + name);
        }
        return value;
    }

    private static boolean parseBoolean(String name, String value) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException(name + " must be true or false");
    }

    private static String escapeSqlLiteral(String value) {
        return value.replace("'", "''");
    }
}
