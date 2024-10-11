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

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Options for the Doris connector. */
public class DorisOptions extends DorisConnectionOptions {

    private static final long serialVersionUID = 1L;

    private String tableIdentifier;

    public DorisOptions(String fenodes, String username, String password, String tableIdentifier) {
        super(fenodes, username, password);
        this.tableIdentifier = tableIdentifier;
    }

    public DorisOptions(
            String fenodes,
            String username,
            String password,
            String tableIdentifier,
            String jdbcUrl) {
        super(fenodes, username, password, jdbcUrl);
        this.tableIdentifier = tableIdentifier;
    }

    public DorisOptions(
            String fenodes,
            String beNodes,
            String username,
            String password,
            String tableIdentifier,
            String jdbcUrl,
            boolean redirect) {
        super(fenodes, beNodes, username, password, jdbcUrl, redirect);
        this.tableIdentifier = tableIdentifier;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public void setTableIdentifier(String tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisOptions that = (DorisOptions) o;
        return Objects.equals(tableIdentifier, that.tableIdentifier)
                && autoRedirect == that.autoRedirect
                && Objects.equals(fenodes, that.fenodes)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(jdbcUrl, that.jdbcUrl)
                && Objects.equals(benodes, that.benodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fenodes, username, password, jdbcUrl, benodes, autoRedirect, tableIdentifier);
    }

    /** Builder of {@link DorisOptions}. */
    public static class Builder {
        private String fenodes;
        private String benodes;
        private String jdbcUrl;
        private String username;
        private String password;
        private boolean autoRedirect = true;
        private String tableIdentifier;

        /**
         * Sets the tableIdentifier for the DorisOptions.
         *
         * @param tableIdentifier Doris's database name and table name, such as db.tbl
         * @return this DorisOptions.builder.
         */
        public Builder setTableIdentifier(String tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
            return this;
        }

        /**
         * Sets the username of doris cluster.
         *
         * @param username Doris cluster username
         * @return this DorisOptions.builder.
         */
        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /**
         * Sets the password of doris cluster.
         *
         * @param password Doris cluster password
         * @return this DorisOptions.builder.
         */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the doris frontend http rest url, such as 127.0.0.1:8030,127.0.0.2:8030
         *
         * @param fenodes
         * @return this DorisOptions.builder.
         */
        public Builder setFenodes(String fenodes) {
            this.fenodes = fenodes;
            return this;
        }

        /**
         * Sets the doris backend http rest url, such as 127.0.0.1:8040,127.0.0.2:8040
         *
         * @param benodes
         * @return this DorisOptions.builder.
         */
        public Builder setBenodes(String benodes) {
            this.benodes = benodes;
            return this;
        }

        /**
         * Sets the doris fe jdbc url for lookup query, such as jdbc:mysql://127.0.0.1:9030
         *
         * @param jdbcUrl
         * @return this DorisOptions.builder.
         */
        public Builder setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        /**
         * Sets the autoRedirect for DorisOptions. If true, stream load will be written directly to
         * fe. If false, it will first get the be list and write directly to be.
         *
         * @param autoRedirect
         * @return this DorisOptions.builder.
         */
        public Builder setAutoRedirect(boolean autoRedirect) {
            this.autoRedirect = autoRedirect;
            return this;
        }

        /**
         * Build the {@link DorisOptions}.
         *
         * @return a DorisOptions with the settings made for this builder.
         */
        public DorisOptions build() {
            checkNotNull(fenodes, "No fenodes supplied.");
            // multi table load, don't need check
            // checkNotNull(tableIdentifier, "No tableIdentifier supplied.");
            return new DorisOptions(
                    fenodes, benodes, username, password, tableIdentifier, jdbcUrl, autoRedirect);
        }
    }
}
