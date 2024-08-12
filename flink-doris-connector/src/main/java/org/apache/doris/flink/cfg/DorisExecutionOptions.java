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

import org.apache.doris.flink.sink.writer.WriteMode;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

import static org.apache.doris.flink.sink.writer.LoadConstants.FORMAT_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.JSON;
import static org.apache.doris.flink.sink.writer.LoadConstants.READ_JSON_BY_LINE;

/** Doris sink batch options. */
public class DorisExecutionOptions implements Serializable {

    private static final long serialVersionUID = 1L;
    // 0 means disable checker thread
    public static final int DEFAULT_CHECK_INTERVAL = 0;
    public static final int DEFAULT_MAX_RETRY_TIMES = 3;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private static final int DEFAULT_BUFFER_COUNT = 3;
    // batch flush
    private static final int DEFAULT_FLUSH_QUEUE_SIZE = 2;
    private static final int DEFAULT_BUFFER_FLUSH_MAX_ROWS = 500000;
    private static final int DEFAULT_BUFFER_FLUSH_MAX_BYTES = 100 * 1024 * 1024;
    private static final long DEFAULT_BUFFER_FLUSH_INTERVAL_MS = 10 * 1000;
    private final int checkInterval;
    private final int maxRetries;
    private final int bufferSize;
    private final int bufferCount;
    private final String labelPrefix;
    private final boolean useCache;

    /** Properties for the StreamLoad. */
    private final Properties streamLoadProp;

    private final Boolean enableDelete;
    private Boolean enable2PC;
    private boolean force2PC;

    // batch mode param
    private final int flushQueueSize;
    private final int bufferFlushMaxRows;
    private final int bufferFlushMaxBytes;
    private final long bufferFlushIntervalMs;
    private final boolean enableBatchMode;
    private final boolean ignoreUpdateBefore;
    private final WriteMode writeMode;
    private final boolean ignoreCommitError;

    public DorisExecutionOptions(
            int checkInterval,
            int maxRetries,
            int bufferSize,
            int bufferCount,
            String labelPrefix,
            boolean useCache,
            Properties streamLoadProp,
            Boolean enableDelete,
            Boolean enable2PC,
            boolean enableBatchMode,
            int flushQueueSize,
            int bufferFlushMaxRows,
            int bufferFlushMaxBytes,
            long bufferFlushIntervalMs,
            boolean ignoreUpdateBefore,
            boolean force2PC,
            WriteMode writeMode,
            boolean ignoreCommitError) {
        Preconditions.checkArgument(maxRetries >= 0);
        this.checkInterval = checkInterval;
        this.maxRetries = maxRetries;
        this.bufferSize = bufferSize;
        this.bufferCount = bufferCount;
        this.labelPrefix = labelPrefix;
        this.useCache = useCache;
        this.streamLoadProp = streamLoadProp;
        this.enableDelete = enableDelete;
        this.enable2PC = enable2PC;
        this.force2PC = force2PC;

        this.enableBatchMode = enableBatchMode;
        this.flushQueueSize = flushQueueSize;
        this.bufferFlushMaxRows = bufferFlushMaxRows;
        this.bufferFlushMaxBytes = bufferFlushMaxBytes;
        this.bufferFlushIntervalMs = bufferFlushIntervalMs;

        this.ignoreUpdateBefore = ignoreUpdateBefore;
        this.writeMode = writeMode;
        this.ignoreCommitError = ignoreCommitError;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builderDefaults() {
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        return new Builder().setStreamLoadProp(properties);
    }

    public static DorisExecutionOptions defaults() {
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        return new Builder().setStreamLoadProp(properties).build();
    }

    public static Properties defaultsProperties() {
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        return properties;
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

    public boolean isUseCache() {
        return useCache;
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

    public int getFlushQueueSize() {
        return flushQueueSize;
    }

    public int getBufferFlushMaxRows() {
        return bufferFlushMaxRows;
    }

    public int getBufferFlushMaxBytes() {
        return bufferFlushMaxBytes;
    }

    public long getBufferFlushIntervalMs() {
        return bufferFlushIntervalMs;
    }

    public boolean enableBatchMode() {
        return enableBatchMode;
    }

    public boolean getIgnoreUpdateBefore() {
        return ignoreUpdateBefore;
    }

    public void setEnable2PC(Boolean enable2PC) {
        this.enable2PC = enable2PC;
    }

    public boolean force2PC() {
        return force2PC;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public boolean ignoreCommitError() {
        return ignoreCommitError;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisExecutionOptions that = (DorisExecutionOptions) o;
        return checkInterval == that.checkInterval
                && maxRetries == that.maxRetries
                && bufferSize == that.bufferSize
                && bufferCount == that.bufferCount
                && useCache == that.useCache
                && force2PC == that.force2PC
                && flushQueueSize == that.flushQueueSize
                && bufferFlushMaxRows == that.bufferFlushMaxRows
                && bufferFlushMaxBytes == that.bufferFlushMaxBytes
                && bufferFlushIntervalMs == that.bufferFlushIntervalMs
                && enableBatchMode == that.enableBatchMode
                && ignoreUpdateBefore == that.ignoreUpdateBefore
                && ignoreCommitError == that.ignoreCommitError
                && Objects.equals(labelPrefix, that.labelPrefix)
                && Objects.equals(streamLoadProp, that.streamLoadProp)
                && Objects.equals(enableDelete, that.enableDelete)
                && Objects.equals(enable2PC, that.enable2PC)
                && writeMode == that.writeMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                checkInterval,
                maxRetries,
                bufferSize,
                bufferCount,
                labelPrefix,
                useCache,
                streamLoadProp,
                enableDelete,
                enable2PC,
                force2PC,
                flushQueueSize,
                bufferFlushMaxRows,
                bufferFlushMaxBytes,
                bufferFlushIntervalMs,
                enableBatchMode,
                ignoreUpdateBefore,
                writeMode,
                ignoreCommitError);
    }

    /** Builder of {@link DorisExecutionOptions}. */
    public static class Builder {
        private int checkInterval = DEFAULT_CHECK_INTERVAL;
        private int maxRetries = DEFAULT_MAX_RETRY_TIMES;
        private int bufferSize = DEFAULT_BUFFER_SIZE;
        private int bufferCount = DEFAULT_BUFFER_COUNT;
        private String labelPrefix = "";
        private boolean useCache = false;
        private Properties streamLoadProp = new Properties();
        private boolean enableDelete = true;
        private boolean enable2PC = true;

        // A flag used to determine whether to forcibly open 2pc. By default, the uniq model close
        // 2pc.
        private boolean force2PC = false;

        private int flushQueueSize = DEFAULT_FLUSH_QUEUE_SIZE;
        private int bufferFlushMaxRows = DEFAULT_BUFFER_FLUSH_MAX_ROWS;
        private int bufferFlushMaxBytes = DEFAULT_BUFFER_FLUSH_MAX_BYTES;
        private long bufferFlushIntervalMs = DEFAULT_BUFFER_FLUSH_INTERVAL_MS;
        private boolean enableBatchMode = false;

        private boolean ignoreUpdateBefore = true;
        private WriteMode writeMode = WriteMode.STREAM_LOAD;
        private boolean ignoreCommitError = false;

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

        public Builder setUseCache(boolean useCache) {
            this.useCache = useCache;
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

        public Builder enable2PC() {
            this.enable2PC = true;
            // Force open 2pc
            this.force2PC = true;
            return this;
        }

        public Builder setBatchMode(Boolean enableBatchMode) {
            this.enableBatchMode = enableBatchMode;
            if (enableBatchMode.equals(Boolean.TRUE)) {
                this.writeMode = WriteMode.STREAM_LOAD_BATCH;
            }
            return this;
        }

        public Builder setFlushQueueSize(int flushQueueSize) {
            this.flushQueueSize = flushQueueSize;
            return this;
        }

        public Builder setBufferFlushIntervalMs(long bufferFlushIntervalMs) {
            this.bufferFlushIntervalMs = bufferFlushIntervalMs;
            return this;
        }

        public Builder setBufferFlushMaxRows(int bufferFlushMaxRows) {
            this.bufferFlushMaxRows = bufferFlushMaxRows;
            return this;
        }

        public Builder setBufferFlushMaxBytes(int bufferFlushMaxBytes) {
            this.bufferFlushMaxBytes = bufferFlushMaxBytes;
            return this;
        }

        public Builder setIgnoreUpdateBefore(boolean ignoreUpdateBefore) {
            this.ignoreUpdateBefore = ignoreUpdateBefore;
            return this;
        }

        public Builder setWriteMode(WriteMode writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        public Builder setIgnoreCommitError(boolean ignoreCommitError) {
            this.ignoreCommitError = ignoreCommitError;
            return this;
        }

        public DorisExecutionOptions build() {
            // If format=json is set but read_json_by_line is not set, record may not be written.
            if (streamLoadProp != null
                    && streamLoadProp.containsKey(FORMAT_KEY)
                    && JSON.equals(streamLoadProp.getProperty(FORMAT_KEY))) {
                streamLoadProp.put(READ_JSON_BY_LINE, true);
            }

            Preconditions.checkArgument(
                    bufferFlushIntervalMs >= 1000,
                    "bufferFlushIntervalMs must be greater than or equal to 1 second");

            Preconditions.checkArgument(
                    bufferFlushMaxRows >= 10000,
                    "bufferFlushMaxRows must be greater than or equal to 10000");

            Preconditions.checkArgument(
                    bufferFlushMaxBytes >= 10485760,
                    "bufferFlushMaxBytes must be greater than or equal to 10485760(10MB)");

            return new DorisExecutionOptions(
                    checkInterval,
                    maxRetries,
                    bufferSize,
                    bufferCount,
                    labelPrefix,
                    useCache,
                    streamLoadProp,
                    enableDelete,
                    enable2PC,
                    enableBatchMode,
                    flushQueueSize,
                    bufferFlushMaxRows,
                    bufferFlushMaxBytes,
                    bufferFlushIntervalMs,
                    ignoreUpdateBefore,
                    force2PC,
                    writeMode,
                    ignoreCommitError);
        }
    }
}
