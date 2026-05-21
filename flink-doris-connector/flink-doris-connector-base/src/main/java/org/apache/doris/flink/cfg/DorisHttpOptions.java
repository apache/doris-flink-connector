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

import java.io.Serializable;
import java.util.Objects;

/** HTTP protocol options shared by Doris REST requests. */
public class DorisHttpOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_KEY_STORE_TYPE = "JKS";

    private final boolean enableHttps;
    private final String httpsKeyStorePath;
    private final String httpsKeyStoreType;
    private final String httpsKeyStorePassword;

    public DorisHttpOptions(
            boolean enableHttps,
            String httpsKeyStorePath,
            String httpsKeyStoreType,
            String httpsKeyStorePassword) {
        this.enableHttps = enableHttps;
        this.httpsKeyStorePath = httpsKeyStorePath;
        this.httpsKeyStoreType =
                httpsKeyStoreType == null ? DEFAULT_KEY_STORE_TYPE : httpsKeyStoreType;
        this.httpsKeyStorePassword = httpsKeyStorePassword;
    }

    public static DorisHttpOptions defaults() {
        return of(false);
    }

    public static DorisHttpOptions of(boolean enableHttps) {
        return new DorisHttpOptions(enableHttps, null, DEFAULT_KEY_STORE_TYPE, null);
    }

    public boolean isEnableHttps() {
        return enableHttps;
    }

    public String getHttpsKeyStorePath() {
        return httpsKeyStorePath;
    }

    public String getHttpsKeyStoreType() {
        return httpsKeyStoreType;
    }

    public String getHttpsKeyStorePassword() {
        return httpsKeyStorePassword;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisHttpOptions that = (DorisHttpOptions) o;
        return enableHttps == that.enableHttps
                && Objects.equals(httpsKeyStorePath, that.httpsKeyStorePath)
                && Objects.equals(httpsKeyStoreType, that.httpsKeyStoreType)
                && Objects.equals(httpsKeyStorePassword, that.httpsKeyStorePassword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                enableHttps, httpsKeyStorePath, httpsKeyStoreType, httpsKeyStorePassword);
    }
}
