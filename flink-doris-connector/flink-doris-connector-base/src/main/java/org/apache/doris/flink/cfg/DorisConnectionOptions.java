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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** Doris connection options. */
public class DorisConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String fenodes;
    protected final String username;
    protected final String password;
    protected String jdbcUrl;
    protected String benodes;
    protected DorisHttpOptions httpOptions = DorisHttpOptions.defaults();

    /**
     * Used to enable automatic redirection of fe, When it is not enabled, it will actively request
     * the be list, and the polling will initiate a streamload request to be.
     */
    protected boolean autoRedirect;

    public DorisConnectionOptions(String fenodes, String username, String password) {
        this.fenodes = Preconditions.checkNotNull(fenodes, "fenodes  is empty");
        this.username = username;
        this.password = password;
    }

    public DorisConnectionOptions(
            String fenodes, String username, String password, String jdbcUrl) {
        this(fenodes, username, password);
        this.jdbcUrl = jdbcUrl;
    }

    public DorisConnectionOptions(
            String fenodes,
            String benodes,
            String username,
            String password,
            String jdbcUrl,
            boolean autoRedirect) {
        this(
                fenodes,
                benodes,
                username,
                password,
                jdbcUrl,
                autoRedirect,
                DorisHttpOptions.defaults());
    }

    public DorisConnectionOptions(
            String fenodes,
            String benodes,
            String username,
            String password,
            String jdbcUrl,
            boolean autoRedirect,
            DorisHttpOptions httpOptions) {
        this(fenodes, username, password);
        this.benodes = benodes;
        this.jdbcUrl = jdbcUrl;
        this.autoRedirect = autoRedirect;
        this.httpOptions = httpOptions == null ? DorisHttpOptions.defaults() : httpOptions;
    }

    public String getFenodes() {
        return fenodes;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getBenodes() {
        return benodes;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public boolean isAutoRedirect() {
        return autoRedirect;
    }

    public DorisHttpOptions getHttpOptions() {
        return httpOptions;
    }

    public boolean isEnableHttps() {
        return httpOptions.isEnableHttps();
    }

    /** Builder for {@link DorisConnectionOptions}. */
    public static class DorisConnectionOptionsBuilder {
        private String fenodes;
        private String benodes;
        private String username;
        private String password;
        private String jdbcUrl;
        private boolean autoRedirect;
        private DorisHttpOptions httpOptions = DorisHttpOptions.defaults();

        public DorisConnectionOptionsBuilder withFenodes(String fenodes) {
            this.fenodes = fenodes;
            return this;
        }

        public DorisConnectionOptionsBuilder withBenodes(String benodes) {
            this.benodes = benodes;
            return this;
        }

        public DorisConnectionOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public DorisConnectionOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public DorisConnectionOptionsBuilder withJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public DorisConnectionOptionsBuilder withAutoRedirect(boolean autoRedirect) {
            this.autoRedirect = autoRedirect;
            return this;
        }

        public DorisConnectionOptionsBuilder withHttpOptions(DorisHttpOptions httpOptions) {
            this.httpOptions = httpOptions;
            return this;
        }

        public DorisConnectionOptions build() {
            return new DorisConnectionOptions(
                    fenodes, benodes, username, password, jdbcUrl, autoRedirect, httpOptions);
        }
    }
}
