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

package org.apache.doris.flink.sink.copy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.CopyLoadException;
import org.apache.doris.flink.exception.DorisBatchLoadException;
import org.apache.doris.flink.sink.EscapeHandler;
import org.apache.doris.flink.sink.HttpPutBuilder;
import org.apache.doris.flink.sink.HttpUtil;
import org.apache.doris.flink.sink.writer.LabelGenerator;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;

/** load data from stage load. */
public class BatchStageLoad implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BatchStageLoad.class);
    private final LabelGenerator labelGenerator;
    private final byte[] lineDelimiter;
    private static final String UPLOAD_URL_PATTERN = "http://%s/copy/upload";
    private static final String LINE_DELIMITER_KEY_WITH_PRETIX = "file.line_delimiter";
    private String uploadUrl;
    private String hostPort;
    private final String username;
    private final String password;
    private final Properties loadProps;
    private Map<String, BatchRecordBuffer> bufferMap = new ConcurrentHashMap<>();
    private Map<String, List<String>> fileListMap = new ConcurrentHashMap<>();
    private long currentCheckpointID;
    private AtomicInteger fileNum;
    private DorisExecutionOptions executionOptions;
    private ExecutorService loadExecutorService;
    private StageLoadAsyncExecutor loadAsyncExecutor;
    private BlockingQueue<BatchRecordBuffer> flushQueue;
    private final AtomicBoolean started;
    private volatile boolean loadThreadAlive = false;
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);
    private HttpClientBuilder httpClientBuilder = new HttpUtil().getHttpClientBuilderForCopyBatch();

    public BatchStageLoad(
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions executionOptions,
            LabelGenerator labelGenerator) {
        this.username = dorisOptions.getUsername();
        this.password = dorisOptions.getPassword();
        this.loadProps = executionOptions.getStreamLoadProp();
        this.labelGenerator = labelGenerator;
        this.hostPort = dorisOptions.getFenodes();
        this.uploadUrl = String.format(UPLOAD_URL_PATTERN, hostPort);
        this.fileNum = new AtomicInteger();
        this.lineDelimiter =
                EscapeHandler.escapeString(
                                loadProps.getProperty(
                                        LINE_DELIMITER_KEY_WITH_PRETIX, LINE_DELIMITER_DEFAULT))
                        .getBytes();
        this.executionOptions = executionOptions;
        this.flushQueue = new LinkedBlockingDeque<>(executionOptions.getFlushQueueSize());
        this.loadAsyncExecutor = new StageLoadAsyncExecutor();
        this.loadExecutorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1),
                        new DefaultThreadFactory("copy-executor"),
                        new ThreadPoolExecutor.AbortPolicy());
        this.started = new AtomicBoolean(true);
        this.loadExecutorService.execute(loadAsyncExecutor);
    }

    /**
     * write record into cache.
     *
     * @param record
     * @throws IOException
     */
    public synchronized void writeRecord(String database, String table, byte[] record)
            throws InterruptedException {
        checkFlushException();
        String bufferKey = getTableIdentifier(database, table);
        BatchRecordBuffer buffer =
                bufferMap.computeIfAbsent(
                        bufferKey,
                        k ->
                                new BatchRecordBuffer(
                                        database,
                                        table,
                                        this.lineDelimiter,
                                        executionOptions.getBufferFlushMaxBytes()));
        buffer.insert(record);
        // When it exceeds 80% of the byteSize,to flush, to avoid triggering bytebuffer expansion
        if (buffer.getBufferSizeBytes() >= executionOptions.getBufferFlushMaxBytes() * 0.8
                || (executionOptions.getBufferFlushMaxRows() != 0
                        && buffer.getNumOfRecords() >= executionOptions.getBufferFlushMaxRows())) {
            flush(bufferKey, false);
        }
    }

    public synchronized void flush(String bufferKey, boolean waitUtilDone)
            throws InterruptedException {
        checkFlushException();
        if (null == bufferKey) {
            for (String key : bufferMap.keySet()) {
                flushBuffer(key);
            }
        } else if (bufferMap.containsKey(bufferKey)) {
            flushBuffer(bufferKey);
        }

        if (waitUtilDone) {
            waitAsyncLoadFinish();
        }
    }

    private synchronized void flushBuffer(String bufferKey) {
        BatchRecordBuffer buffer = bufferMap.get(bufferKey);
        buffer.setLabelName(
                labelGenerator.generateCopyBatchLabel(
                        buffer.getTable(), currentCheckpointID, fileNum.getAndIncrement()));
        putRecordToFlushQueue(buffer);
        bufferMap.remove(bufferKey);
    }

    private void putRecordToFlushQueue(BatchRecordBuffer buffer) {
        checkFlushException();
        if (!loadThreadAlive) {
            throw new RuntimeException("load thread already exit, write was interrupted");
        }
        try {
            flushQueue.put(buffer);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to put record buffer to flush queue");
        }
    }

    public void setCurrentCheckpointID(long currentCheckpointID) {
        this.currentCheckpointID = currentCheckpointID;
    }

    private void checkFlushException() {
        if (exception.get() != null) {
            throw new DorisBatchLoadException(exception.get());
        }
    }

    private void waitAsyncLoadFinish() {
        for (int i = 0; i < executionOptions.getFlushQueueSize() + 1; i++) {
            BatchRecordBuffer empty = new BatchRecordBuffer();
            putRecordToFlushQueue(empty);
        }
    }

    private String getTableIdentifier(String database, String table) {
        return database + "." + table;
    }

    public void close() {
        // close async executor
        this.loadExecutorService.shutdown();
        this.started.set(false);
        // clear buffer
        this.flushQueue.clear();
        this.fileListMap.clear();
        this.bufferMap.clear();
    }

    public Map<String, List<String>> getFileListMap() {
        return fileListMap;
    }

    public void clearFileListMap() {
        this.fileNum.set(0);
        this.fileListMap.clear();
    }

    class StageLoadAsyncExecutor implements Runnable {
        @Override
        public void run() {
            LOG.info("StageLoadAsyncExecutor start");
            loadThreadAlive = true;
            while (started.get()) {
                BatchRecordBuffer buffer = null;
                try {
                    buffer = flushQueue.poll(2000L, TimeUnit.MILLISECONDS);
                    if (buffer == null) {
                        continue;
                    }
                    if (buffer.getLabelName() != null) {
                        String bufferKey =
                                getTableIdentifier(buffer.getDatabase(), buffer.getTable());
                        uploadToStorage(buffer.getLabelName(), buffer);
                        fileListMap
                                .computeIfAbsent(bufferKey, k -> new ArrayList<>())
                                .add(buffer.getLabelName());
                    }
                } catch (Exception e) {
                    LOG.error("worker running error", e);
                    exception.set(e);
                    // clear queue to avoid writer thread blocking
                    flushQueue.clear();
                    fileListMap.clear();
                    bufferMap.clear();
                    break;
                }
            }
            LOG.info("StageLoadAsyncExecutor stop");
            loadThreadAlive = false;
        }

        /*
         * upload to storage
         */
        public void uploadToStorage(String fileName, BatchRecordBuffer buffer) throws IOException {
            long start = System.currentTimeMillis();
            LOG.info("file write started for {}", fileName);
            String address = getUploadAddress(fileName);
            long addressTs = System.currentTimeMillis();
            LOG.info(
                    "redirect to internalStage address:{}, cost {} ms", address, addressTs - start);
            String requestId = uploadToInternalStage(address, buffer.getData());
            LOG.info(
                    "upload file {} finished, record {} size {}, cost {}ms, with requestId {}",
                    fileName,
                    buffer.getNumOfRecords(),
                    buffer.getBufferSizeBytes(),
                    System.currentTimeMillis() - addressTs,
                    requestId);
        }

        public String uploadToInternalStage(String address, ByteBuffer data)
                throws CopyLoadException {
            ByteArrayEntity entity =
                    new ByteArrayEntity(data.array(), data.arrayOffset(), data.limit());
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder.setUrl(address).addCommonHeader().setEntity(entity);
            HttpPut httpPut = putBuilder.build();
            try {
                Object result =
                        BackoffAndRetryUtils.backoffAndRetry(
                                BackoffAndRetryUtils.LoadOperation.UPLOAD_FILE,
                                () -> {
                                    try (CloseableHttpClient httpClient =
                                            httpClientBuilder.build()) {
                                        try (CloseableHttpResponse response =
                                                httpClient.execute(httpPut)) {
                                            final int statusCode =
                                                    response.getStatusLine().getStatusCode();
                                            String requestId =
                                                    getRequestId(response.getAllHeaders());
                                            if (statusCode == 200 && response.getEntity() != null) {
                                                String loadResult =
                                                        EntityUtils.toString(response.getEntity());
                                                if (loadResult == null || loadResult.isEmpty()) {
                                                    // upload finished
                                                    return requestId;
                                                }
                                                LOG.error(
                                                        "upload file failed, requestId is {}, response result: {}",
                                                        requestId,
                                                        loadResult);
                                                throw new CopyLoadException(
                                                        "upload file failed: "
                                                                + response.getStatusLine()
                                                                        .toString()
                                                                + ", with requestId "
                                                                + requestId);
                                            }
                                            throw new CopyLoadException(
                                                    "upload file error: "
                                                            + response.getStatusLine().toString()
                                                            + ", with requestId "
                                                            + requestId);
                                        }
                                    }
                                });
                return String.valueOf(result);
            } catch (Exception ex) {
                LOG.error("Failed to upload data to internal stage ", ex);
                throw new CopyLoadException(
                        "Failed to upload data to internal stage, " + ex.getMessage());
            }
        }

        /*
         * get requestId from response Header for upload stage
         * header key are: x-oss-request-id/x-cos-request-id/x-obs-request-id/x-amz-request-id
         * @return key:value
         */
        public String getRequestId(Header[] headers) {
            if (headers == null || headers.length == 0) {
                return null;
            }
            for (int i = 0; i < headers.length; i++) {
                final Header header = headers[i];
                String name = header.getName();
                if (name != null && name.toLowerCase().matches("x-\\S+-request-id")) {
                    return name + ":" + header.getValue();
                }
            }
            return null;
        }

        /** Get the redirected s3 address. */
        public String getUploadAddress(String fileName) throws CopyLoadException {
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder
                    .setUrl(uploadUrl)
                    .addFileName(fileName)
                    .addCommonHeader()
                    .setEmptyEntity()
                    .baseAuth(username, password);

            try {
                Object address =
                        BackoffAndRetryUtils.backoffAndRetry(
                                BackoffAndRetryUtils.LoadOperation.GET_INTERNAL_STAGE_ADDRESS,
                                () -> {
                                    try (CloseableHttpClient httpClient =
                                            httpClientBuilder.build()) {
                                        try (CloseableHttpResponse execute =
                                                httpClient.execute(putBuilder.build())) {
                                            int statusCode =
                                                    execute.getStatusLine().getStatusCode();
                                            String reason =
                                                    execute.getStatusLine().getReasonPhrase();
                                            if (statusCode == 307) {
                                                Header location =
                                                        execute.getFirstHeader("location");
                                                String uploadAddress = location.getValue();
                                                return uploadAddress;
                                            } else {
                                                HttpEntity entity = execute.getEntity();
                                                String result =
                                                        entity == null
                                                                ? null
                                                                : EntityUtils.toString(entity);
                                                LOG.error(
                                                        "Failed to get internalStage address, status {}, reason {}, response {}",
                                                        statusCode,
                                                        reason,
                                                        result);
                                                throw new CopyLoadException(
                                                        "Failed get internalStage address");
                                            }
                                        }
                                    }
                                });
                Preconditions.checkNotNull(address, "internalStage address is null");
                return address.toString();
            } catch (Exception e) {
                LOG.error("Get internalStage address error,", e);
                throw new CopyLoadException("Get internalStage address error, " + e.getMessage());
            }
        }
    }

    static class DefaultThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DefaultThreadFactory(String name) {
            namePrefix = "pool-" + poolNumber.getAndIncrement() + "-" + name + "-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }

    @VisibleForTesting
    public void setHttpClientBuilder(HttpClientBuilder httpClientBuilder) {
        this.httpClientBuilder = httpClientBuilder;
    }

    @VisibleForTesting
    public boolean isLoadThreadAlive() {
        return loadThreadAlive;
    }
}
