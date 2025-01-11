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

/** Doris read Options. */
public class DorisReadOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private String readFields;
    private String filterQuery;
    // 增加分区过滤功能，减少查询数据量
    private String filterPartition;
    private Integer requestTabletSize;
    private Integer requestConnectTimeoutMs;
    private Integer requestReadTimeoutMs;
    private Integer requestQueryTimeoutS;
    private Integer requestRetries;
    private Integer requestBatchSize;
    private Long execMemLimit;
    private Integer deserializeQueueSize;
    private Boolean deserializeArrowAsync;
    private boolean useOldApi;
    private boolean useFlightSql;
    private Integer flightSqlPort;

    public DorisReadOptions(
            String readFields,
            String filterQuery,
            String filterPartition,
            Integer requestTabletSize,
            Integer requestConnectTimeoutMs,
            Integer requestReadTimeoutMs,
            Integer requestQueryTimeoutS,
            Integer requestRetries,
            Integer requestBatchSize,
            Long execMemLimit,
            Integer deserializeQueueSize,
            Boolean deserializeArrowAsync,
            boolean useOldApi,
            boolean useFlightSql,
            Integer flightSqlPort) {
        this.readFields = readFields;
        this.filterQuery = filterQuery;
        this.filterPartition = filterPartition;
        this.requestTabletSize = requestTabletSize;
        this.requestConnectTimeoutMs = requestConnectTimeoutMs;
        this.requestReadTimeoutMs = requestReadTimeoutMs;
        this.requestQueryTimeoutS = requestQueryTimeoutS;
        this.requestRetries = requestRetries;
        this.requestBatchSize = requestBatchSize;
        this.execMemLimit = execMemLimit;
        this.deserializeQueueSize = deserializeQueueSize;
        this.deserializeArrowAsync = deserializeArrowAsync;
        this.useOldApi = useOldApi;
        this.useFlightSql = useFlightSql;
        this.flightSqlPort = flightSqlPort;
    }

    public String getReadFields() {
        return readFields;
    }

    public String getFilterQuery() {
        return filterQuery;
    }

    public String getFilterPartition() {
        return filterPartition;
    }

    public Integer getRequestTabletSize() {
        return requestTabletSize;
    }

    public Integer getRequestConnectTimeoutMs() {
        return requestConnectTimeoutMs;
    }

    public Integer getRequestReadTimeoutMs() {
        return requestReadTimeoutMs;
    }

    public Integer getRequestRetries() {
        return requestRetries;
    }

    public Integer getRequestBatchSize() {
        return requestBatchSize;
    }

    public Integer getRequestQueryTimeoutS() {
        return requestQueryTimeoutS;
    }

    public Long getExecMemLimit() {
        return execMemLimit;
    }

    public Integer getDeserializeQueueSize() {
        return deserializeQueueSize;
    }

    public Boolean getDeserializeArrowAsync() {
        return deserializeArrowAsync;
    }

    public boolean getUseOldApi() {
        return useOldApi;
    }

    public void setReadFields(String readFields) {
        this.readFields = readFields;
    }

    public void setFilterQuery(String filterQuery) {
        this.filterQuery = filterQuery;
    }

    public void setFilterPartition(String filterPartition) {
        this.filterPartition = filterPartition;
    }

    public boolean getUseFlightSql() {
        return useFlightSql;
    }

    public Integer getFlightSqlPort() {
        return flightSqlPort;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DorisReadOptions defaults() {
        return DorisReadOptions.builder().build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisReadOptions that = (DorisReadOptions) o;
        return useOldApi == that.useOldApi
                && Objects.equals(readFields, that.readFields)
                && Objects.equals(filterQuery, that.filterQuery)
                && Objects.equals(filterPartition, that.filterPartition)
                && Objects.equals(requestTabletSize, that.requestTabletSize)
                && Objects.equals(requestConnectTimeoutMs, that.requestConnectTimeoutMs)
                && Objects.equals(requestReadTimeoutMs, that.requestReadTimeoutMs)
                && Objects.equals(requestQueryTimeoutS, that.requestQueryTimeoutS)
                && Objects.equals(requestRetries, that.requestRetries)
                && Objects.equals(requestBatchSize, that.requestBatchSize)
                && Objects.equals(execMemLimit, that.execMemLimit)
                && Objects.equals(deserializeQueueSize, that.deserializeQueueSize)
                && Objects.equals(deserializeArrowAsync, that.deserializeArrowAsync)
                && Objects.equals(useFlightSql, that.useFlightSql)
                && Objects.equals(flightSqlPort, that.flightSqlPort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                readFields,
                filterQuery,
                filterPartition,
                requestTabletSize,
                requestConnectTimeoutMs,
                requestReadTimeoutMs,
                requestQueryTimeoutS,
                requestRetries,
                requestBatchSize,
                execMemLimit,
                deserializeQueueSize,
                deserializeArrowAsync,
                useOldApi,
                useFlightSql,
                flightSqlPort);
    }

    public DorisReadOptions copy() {
        return new DorisReadOptions(
                readFields,
                filterQuery,
                filterPartition,
                requestTabletSize,
                requestConnectTimeoutMs,
                requestReadTimeoutMs,
                requestQueryTimeoutS,
                requestRetries,
                requestBatchSize,
                execMemLimit,
                deserializeQueueSize,
                deserializeArrowAsync,
                useOldApi,
                useFlightSql,
                flightSqlPort);
    }

    /** Builder of {@link DorisReadOptions}. */
    public static class Builder {

        private String readFields;
        private String filterQuery;
        private String filterPartition;
        private Integer requestTabletSize;
        private Integer requestConnectTimeoutMs;
        private Integer requestReadTimeoutMs;
        private Integer requestQueryTimeoutS;
        private Integer requestRetries;
        private Integer requestBatchSize;
        private Long execMemLimit;
        private Integer deserializeQueueSize;
        private Boolean deserializeArrowAsync;
        private Boolean useOldApi = false;
        private Boolean useFlightSql = false;
        private Integer flightSqlPort;

        /**
         * Sets the readFields for doris table to push down projection, such as name,age.
         *
         * @param readFields
         * @return this DorisReadOptions.builder.
         */
        public Builder setReadFields(String readFields) {
            this.readFields = readFields;
            return this;
        }

        /**
         * Sets the filterQuery for doris table to push down filter, such as name,age.
         *
         * @param filterQuery
         * @return this DorisReadOptions.builder.
         */
        public Builder setFilterQuery(String filterQuery) {
            this.filterQuery = filterQuery;
            return this;
        }

        /**
         * Sets the filterPartition for doris table to filter partition.
         *
         * @param filterPartition
         * @return this DorisReadOptions.builder.
         */
        public Builder setFilterPartition(String filterPartition) {
            this.filterPartition = filterPartition;
            return this;
        }

        /**
         * Sets the requestTabletSize for DorisReadOptions. The number of Doris Tablets
         * corresponding to a Partition, the smaller this value is set, the more Partitions will be
         * generated. This improves the parallelism on the Flink side, but at the same time puts
         * more pressure on Doris.
         *
         * @param requestTabletSize
         * @return this DorisReadOptions.builder.
         */
        public Builder setRequestTabletSize(Integer requestTabletSize) {
            this.requestTabletSize = requestTabletSize;
            return this;
        }

        /**
         * Sets the request connect timeout for DorisReadOptions.
         *
         * @param requestConnectTimeoutMs
         * @return this DorisReadOptions.builder.
         */
        public Builder setRequestConnectTimeoutMs(Integer requestConnectTimeoutMs) {
            this.requestConnectTimeoutMs = requestConnectTimeoutMs;
            return this;
        }

        /**
         * Sets the request read timeout for DorisReadOptions.
         *
         * @param requestReadTimeoutMs
         * @return this DorisReadOptions.builder.
         */
        public Builder setRequestReadTimeoutMs(Integer requestReadTimeoutMs) {
            this.requestReadTimeoutMs = requestReadTimeoutMs;
            return this;
        }

        /**
         * Sets the timeout time for querying Doris for DorisReadOptions.
         *
         * @param requesQueryTimeoutS
         * @return this DorisReadOptions.builder.
         */
        public Builder setRequestQueryTimeoutS(Integer requesQueryTimeoutS) {
            this.requestQueryTimeoutS = requesQueryTimeoutS;
            return this;
        }

        /**
         * Sets the number of retries to send requests to Doris for DorisReadOptions.
         *
         * @param requestRetries
         * @return this DorisReadOptions.builder.
         */
        public Builder setRequestRetries(Integer requestRetries) {
            this.requestRetries = requestRetries;
            return this;
        }

        /**
         * Sets the read batch size for DorisReadOptions.
         *
         * @param requestBatchSize
         * @return this DorisReadOptions.builder.
         */
        public Builder setRequestBatchSize(Integer requestBatchSize) {
            this.requestBatchSize = requestBatchSize;
            return this;
        }

        /**
         * Sets the Memory limit for a single query for DorisReadOptions.
         *
         * @param execMemLimit
         * @return this DorisReadOptions.builder.
         */
        public Builder setExecMemLimit(Long execMemLimit) {
            this.execMemLimit = execMemLimit;
            return this;
        }

        /**
         * Sets the Asynchronous conversion of internal processing queue in Arrow format
         *
         * @param deserializeQueueSize
         * @return this DorisReadOptions.builder.
         */
        public Builder setDeserializeQueueSize(Integer deserializeQueueSize) {
            this.deserializeQueueSize = deserializeQueueSize;
            return this;
        }

        /**
         * Sets Whether to support asynchronous conversion of Arrow format to RowBatch needed for
         * connector iterations.
         *
         * @param deserializeArrowAsync
         * @return this DorisReadOptions.builder.
         */
        public Builder setDeserializeArrowAsync(Boolean deserializeArrowAsync) {
            this.deserializeArrowAsync = deserializeArrowAsync;
            return this;
        }

        /**
         * Whether to use the legacy source api
         *
         * @param useOldApi
         * @return this DorisReadOptions.builder.
         */
        public Builder setUseOldApi(Boolean useOldApi) {
            this.useOldApi = useOldApi;
            return this;
        }

        /**
         * Whether to use arrow flight sql for query, only supports Doris2.1 and above
         *
         * @param useFlightSql
         * @return this DorisReadOptions.builder.
         */
        public Builder setUseFlightSql(Boolean useFlightSql) {
            this.useFlightSql = useFlightSql;
            return this;
        }

        /**
         * Sets the flight sql port for DorisReadOptions.
         *
         * @param flightSqlPort
         * @return this DorisReadOptions.builder.
         */
        public Builder setFlightSqlPort(Integer flightSqlPort) {
            this.flightSqlPort = flightSqlPort;
            return this;
        }

        /**
         * Build the {@link DorisReadOptions}.
         *
         * @return a DorisReadOptions with the settings made for this builder.
         */
        public DorisReadOptions build() {
            return new DorisReadOptions(
                    readFields,
                    filterQuery,
                    filterPartition,
                    requestTabletSize,
                    requestConnectTimeoutMs,
                    requestReadTimeoutMs,
                    requestQueryTimeoutS,
                    requestRetries,
                    requestBatchSize,
                    execMemLimit,
                    deserializeQueueSize,
                    deserializeArrowAsync,
                    useOldApi,
                    useFlightSql,
                    flightSqlPort);
        }
    }
}
