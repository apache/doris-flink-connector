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

public class DorisLookupOptions implements Serializable {

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final int jdbcReadBatchSize;
    private final int jdbcReadBatchQueueSize;
    private final int jdbcReadThreadSize;

    private final boolean async;

    public DorisLookupOptions(
            long cacheMaxSize,
            long cacheExpireMs,
            int maxRetryTimes,
            int jdbcReadBatchSize,
            int jdbcReadBatchQueueSize,
            int jdbcReadThreadSize,
            boolean async) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
        this.jdbcReadBatchSize = jdbcReadBatchSize;
        this.jdbcReadBatchQueueSize = jdbcReadBatchQueueSize;
        this.jdbcReadThreadSize = jdbcReadThreadSize;
        this.async = async;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheExpireMs() {
        return cacheExpireMs;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public int getJdbcReadBatchSize() {
        return jdbcReadBatchSize;
    }

    public int getJdbcReadBatchQueueSize() {
        return jdbcReadBatchQueueSize;
    }

    public int getJdbcReadThreadSize() {
        return jdbcReadThreadSize;
    }

    public boolean isAsync() {
        return async;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisLookupOptions that = (DorisLookupOptions) o;
        return cacheMaxSize == that.cacheMaxSize
                && cacheExpireMs == that.cacheExpireMs
                && maxRetryTimes == that.maxRetryTimes
                && jdbcReadBatchSize == that.jdbcReadBatchSize
                && jdbcReadBatchQueueSize == that.jdbcReadBatchQueueSize
                && jdbcReadThreadSize == that.jdbcReadThreadSize
                && async == that.async;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                cacheMaxSize,
                cacheExpireMs,
                maxRetryTimes,
                jdbcReadBatchSize,
                jdbcReadBatchQueueSize,
                jdbcReadThreadSize,
                async);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder of {@link DorisLookupOptions}. */
    public static class Builder {
        private long cacheMaxSize = -1L;
        private long cacheExpireMs = -1L;
        private int maxRetryTimes = 1;
        private int jdbcReadBatchSize;
        private int jdbcReadBatchQueueSize;
        private int jdbcReadThreadSize;
        private boolean async;

        /** optional, lookup cache max size, over this value, the old data will be eliminated. */
        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        /** optional, lookup cache expire mills, over this time, the old data will expire. */
        public Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        /** optional, max retry times. */
        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public Builder setJdbcReadBatchSize(int jdbcReadBatchSize) {
            this.jdbcReadBatchSize = jdbcReadBatchSize;
            return this;
        }

        public Builder setJdbcReadBatchQueueSize(int jdbcReadBatchQueueSize) {
            this.jdbcReadBatchQueueSize = jdbcReadBatchQueueSize;
            return this;
        }

        public Builder setJdbcReadThreadSize(int jdbcReadThreadSize) {
            this.jdbcReadThreadSize = jdbcReadThreadSize;
            return this;
        }

        public Builder setAsync(boolean async) {
            this.async = async;
            return this;
        }

        public DorisLookupOptions build() {
            return new DorisLookupOptions(
                    cacheMaxSize,
                    cacheExpireMs,
                    maxRetryTimes,
                    jdbcReadBatchSize,
                    jdbcReadBatchQueueSize,
                    jdbcReadThreadSize,
                    async);
        }
    }
}
