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
import java.util.Properties;

/**
 * Doris sink batch options.
 */
public class DorisExecutionOptions implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final int DEFAULT_CHECK_INTERVAL = 10000;
    public static final int DEFAULT_MAX_RETRY_TIMES = 1;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private static final int DEFAULT_BUFFER_COUNT = 3;
    private final int checkInterval;
    private final int maxRetries;
    private final int bufferSize;
    private final int bufferCount;
    private final String labelPrefix;

    /**
     * Properties for the StreamLoad.
     */
    private final Properties streamLoadProp;

    private final Boolean enableDelete;

    private final Boolean enable2PC;

    public DorisExecutionOptions(int checkInterval,
                                 int maxRetries,
                                 int bufferSize,
                                 int bufferCount,
                                 String labelPrefix,
                                 Properties streamLoadProp,
                                 Boolean enableDelete,
                                 Boolean enable2PC) {
        Preconditions.checkArgument(maxRetries >= 0);
        this.checkInterval = checkInterval;
        this.maxRetries = maxRetries;
        this.bufferSize = bufferSize;
        this.bufferCount = bufferCount;
        this.labelPrefix = labelPrefix;
        this.streamLoadProp = streamLoadProp;
        this.enableDelete = enableDelete;
        this.enable2PC = enable2PC;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DorisExecutionOptions defaults() {
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        return new Builder().setStreamLoadProp(properties).build();
    }

    public Integer checkInterval() {
        return checkInterval;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getBufferCount() {
        return bufferCount;
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    public Properties getStreamLoadProp() {
        return streamLoadProp;
    }

    public Boolean getDeletable() {
        return enableDelete;
    }

    public Boolean enabled2PC() {
        return enable2PC;
    }
    /**
     * Builder of {@link DorisExecutionOptions}.
     */
    public static class Builder {
        private int checkInterval = DEFAULT_CHECK_INTERVAL;
        private int maxRetries = DEFAULT_MAX_RETRY_TIMES;
        private int bufferSize = DEFAULT_BUFFER_SIZE;
        private int bufferCount = DEFAULT_BUFFER_COUNT;
        private String labelPrefix = "";
        private Properties streamLoadProp = new Properties();
        private boolean enableDelete = false;

        private boolean enable2PC = true;

        public Builder setCheckInterval(Integer checkInterval) {
            this.checkInterval = checkInterval;
            return this;
        }

        public Builder setMaxRetries(Integer maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder setBufferCount(int bufferCount) {
            this.bufferCount = bufferCount;
            return this;
        }

        public Builder setLabelPrefix(String labelPrefix) {
            this.labelPrefix = labelPrefix;
            return this;
        }

        public Builder setStreamLoadProp(Properties streamLoadProp) {
            this.streamLoadProp = streamLoadProp;
            return this;
        }

        public Builder setDeletable(Boolean enableDelete) {
            this.enableDelete = enableDelete;
            return this;
        }

        public Builder disable2PC() {
            this.enable2PC = false;
            return this;
        }

        public DorisExecutionOptions build() {
            return new DorisExecutionOptions(checkInterval, maxRetries, bufferSize, bufferCount, labelPrefix, streamLoadProp, enableDelete, enable2PC);
        }
    }


}
