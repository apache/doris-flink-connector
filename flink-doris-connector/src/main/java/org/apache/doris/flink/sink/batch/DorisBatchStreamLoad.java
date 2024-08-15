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

package org.apache.doris.flink.sink.batch;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisBatchLoadException;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.EscapeHandler;
import org.apache.doris.flink.sink.HttpPutBuilder;
import org.apache.doris.flink.sink.HttpUtil;
import org.apache.doris.flink.sink.LoadStatus;
import org.apache.doris.flink.sink.writer.LabelGenerator;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.doris.flink.sink.LoadStatus.PUBLISH_TIMEOUT;
import static org.apache.doris.flink.sink.LoadStatus.SUCCESS;
import static org.apache.doris.flink.sink.writer.LoadConstants.ARROW;
import static org.apache.doris.flink.sink.writer.LoadConstants.COMPRESS_TYPE;
import static org.apache.doris.flink.sink.writer.LoadConstants.COMPRESS_TYPE_GZ;
import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.FORMAT_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.GROUP_COMMIT;
import static org.apache.doris.flink.sink.writer.LoadConstants.GROUP_COMMIT_OFF_MODE;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

/** async stream load. */
public class DorisBatchStreamLoad implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisBatchStreamLoad.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
    private static final long STREAM_LOAD_MAX_BYTES = 10 * 1024 * 1024 * 1024L; // 10 GB
    private static final long STREAM_LOAD_MAX_ROWS = Integer.MAX_VALUE;
    private final LabelGenerator labelGenerator;
    private final byte[] lineDelimiter;
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private String loadUrl;
    private String hostPort;
    private final String username;
    private final String password;
    private final Properties loadProps;
    private Map<String, BatchRecordBuffer> bufferMap = new ConcurrentHashMap<>();
    private DorisExecutionOptions executionOptions;
    private ExecutorService loadExecutorService;
    private LoadAsyncExecutor loadAsyncExecutor;
    private BlockingQueue<BatchRecordBuffer> flushQueue;
    private final AtomicBoolean started;
    private volatile boolean loadThreadAlive = false;
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);
    private HttpClientBuilder httpClientBuilder = new HttpUtil().getHttpClientBuilderForBatch();
    private BackendUtil backendUtil;
    private boolean enableGroupCommit;
    private boolean enableGzCompress;
    private int subTaskId;
    private long maxBlockedBytes;
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final Lock lock = new ReentrantLock();
    private final Condition block = lock.newCondition();

    public DorisBatchStreamLoad(
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions executionOptions,
            LabelGenerator labelGenerator,
            int subTaskId) {
        this.backendUtil =
                StringUtils.isNotEmpty(dorisOptions.getBenodes())
                        ? new BackendUtil(dorisOptions.getBenodes())
                        : new BackendUtil(
                                RestService.getBackendsV2(dorisOptions, dorisReadOptions, LOG));
        this.hostPort = backendUtil.getAvailableBackend();
        this.username = dorisOptions.getUsername();
        this.password = dorisOptions.getPassword();
        this.loadProps = executionOptions.getStreamLoadProp();
        this.labelGenerator = labelGenerator;
        if (loadProps.getProperty(FORMAT_KEY, CSV).equals(ARROW)) {
            this.lineDelimiter = null;
        } else {
            this.lineDelimiter =
                    EscapeHandler.escapeString(
                                    loadProps.getProperty(
                                            LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT))
                            .getBytes();
        }
        this.enableGroupCommit =
                loadProps.containsKey(GROUP_COMMIT)
                        && !loadProps
                                .getProperty(GROUP_COMMIT)
                                .equalsIgnoreCase(GROUP_COMMIT_OFF_MODE);
        this.enableGzCompress = loadProps.getProperty(COMPRESS_TYPE, "").equals(COMPRESS_TYPE_GZ);
        this.executionOptions = executionOptions;
        this.flushQueue = new LinkedBlockingDeque<>(executionOptions.getFlushQueueSize());
        // maxBlockedBytes ensures that a buffer can be written even if the queue is full
        this.maxBlockedBytes =
                (long) executionOptions.getBufferFlushMaxBytes()
                        * (executionOptions.getFlushQueueSize() + 1);
        if (StringUtils.isNotBlank(dorisOptions.getTableIdentifier())) {
            String[] tableInfo = dorisOptions.getTableIdentifier().split("\\.");
            Preconditions.checkState(
                    tableInfo.length == 2,
                    "tableIdentifier input error, the format is database.table");
            this.loadUrl = String.format(LOAD_URL_PATTERN, hostPort, tableInfo[0], tableInfo[1]);
        }
        this.loadAsyncExecutor = new LoadAsyncExecutor(executionOptions.getFlushQueueSize());
        this.loadExecutorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1),
                        new DefaultThreadFactory("streamload-executor"),
                        new ThreadPoolExecutor.AbortPolicy());
        this.started = new AtomicBoolean(true);
        this.loadExecutorService.execute(loadAsyncExecutor);
        this.subTaskId = subTaskId;
    }

    /**
     * write record into cache.
     *
     * @param record
     * @throws IOException
     */
    public synchronized void writeRecord(String database, String table, byte[] record) {
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
                                        executionOptions.getBufferFlushIntervalMs()));

        int bytes = buffer.insert(record);
        currentCacheBytes.addAndGet(bytes);
        if (currentCacheBytes.get() > maxBlockedBytes) {
            lock.lock();
            try {
                while (currentCacheBytes.get() >= maxBlockedBytes) {
                    LOG.info(
                            "Cache full, waiting for flush, currentBytes: {}, maxBlockedBytes: {}",
                            currentCacheBytes.get(),
                            maxBlockedBytes);
                    block.await(1, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                this.exception.set(e);
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }

        // queue has space, flush according to the bufferMaxRows/bufferMaxBytes
        if (flushQueue.size() < executionOptions.getFlushQueueSize()
                && (buffer.getBufferSizeBytes() >= executionOptions.getBufferFlushMaxBytes()
                        || buffer.getNumOfRecords() >= executionOptions.getBufferFlushMaxRows())) {
            boolean flush = bufferFullFlush(bufferKey);
            LOG.info("trigger flush by buffer full, flush: {}", flush);

        } else if (buffer.getBufferSizeBytes() >= STREAM_LOAD_MAX_BYTES
                || buffer.getNumOfRecords() >= STREAM_LOAD_MAX_ROWS) {
            // The buffer capacity exceeds the stream load limit, flush
            boolean flush = bufferFullFlush(bufferKey);
            LOG.info("trigger flush by buffer exceeding the limit, flush: {}", flush);
        }
    }

    public synchronized boolean bufferFullFlush(String bufferKey) {
        return doFlush(bufferKey, false, true);
    }

    public synchronized boolean intervalFlush() {
        return doFlush(null, false, false);
    }

    public synchronized boolean checkpointFlush() {
        return doFlush(null, true, false);
    }

    private synchronized boolean doFlush(
            String bufferKey, boolean waitUtilDone, boolean bufferFull) {
        checkFlushException();
        if (waitUtilDone || bufferFull) {
            boolean flush = flush(bufferKey, waitUtilDone);
            return flush;
        } else if (flushQueue.size() < executionOptions.getFlushQueueSize()) {
            boolean flush = flush(bufferKey, false);
            return flush;
        }
        return false;
    }

    private synchronized boolean flush(String bufferKey, boolean waitUtilDone) {
        if (null == bufferKey) {
            boolean flush = false;
            for (String key : bufferMap.keySet()) {
                BatchRecordBuffer buffer = bufferMap.get(key);
                if (waitUtilDone || buffer.shouldFlush()) {
                    // Ensure that the interval satisfies intervalMS
                    flushBuffer(key);
                    flush = true;
                }
            }
            if (!waitUtilDone && !flush) {
                return false;
            }
        } else if (bufferMap.containsKey(bufferKey)) {
            flushBuffer(bufferKey);
        } else {
            throw new DorisBatchLoadException("buffer not found for key: " + bufferKey);
        }
        if (waitUtilDone) {
            waitAsyncLoadFinish();
        }
        return true;
    }

    private synchronized void flushBuffer(String bufferKey) {
        BatchRecordBuffer buffer = bufferMap.get(bufferKey);
        buffer.setLabelName(labelGenerator.generateBatchLabel(buffer.getTable()));
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
    }

    @VisibleForTesting
    public boolean mergeBuffer(List<BatchRecordBuffer> recordList, BatchRecordBuffer buffer) {
        boolean merge = false;
        if (recordList.size() > 1) {
            boolean sameTable =
                    recordList.stream()
                                    .map(BatchRecordBuffer::getTableIdentifier)
                                    .distinct()
                                    .count()
                            == 1;
            // Buffers can be merged only if they belong to the same table.
            if (sameTable) {
                for (BatchRecordBuffer recordBuffer : recordList) {
                    if (recordBuffer != null
                            && recordBuffer.getLabelName() != null
                            && !buffer.getLabelName().equals(recordBuffer.getLabelName())
                            && !recordBuffer.getBuffer().isEmpty()) {
                        merge(buffer, recordBuffer);
                        merge = true;
                    }
                }
                LOG.info(
                        "merge {} buffer to one stream load, result bufferBytes {}",
                        recordList.size(),
                        buffer.getBufferSizeBytes());
            }
        }
        return merge;
    }

    private boolean merge(BatchRecordBuffer mergeBuffer, BatchRecordBuffer buffer) {
        if (buffer.getBuffer().isEmpty()) {
            return false;
        }
        if (!mergeBuffer.getBuffer().isEmpty()) {
            mergeBuffer.getBuffer().add(mergeBuffer.getLineDelimiter());
            mergeBuffer.setBufferSizeBytes(
                    mergeBuffer.getBufferSizeBytes() + mergeBuffer.getLineDelimiter().length);
            currentCacheBytes.addAndGet(buffer.getLineDelimiter().length);
        }
        mergeBuffer.getBuffer().addAll(buffer.getBuffer());
        mergeBuffer.setNumOfRecords(mergeBuffer.getNumOfRecords() + buffer.getNumOfRecords());
        mergeBuffer.setBufferSizeBytes(
                mergeBuffer.getBufferSizeBytes() + buffer.getBufferSizeBytes());
        return true;
    }

    class LoadAsyncExecutor implements Runnable {

        private int flushQueueSize;

        public LoadAsyncExecutor(int flushQueueSize) {
            this.flushQueueSize = flushQueueSize;
        }

        @Override
        public void run() {
            LOG.info("LoadAsyncExecutor start");
            loadThreadAlive = true;
            List<BatchRecordBuffer> recordList = new ArrayList<>(flushQueueSize);
            while (started.get()) {
                recordList.clear();
                try {
                    BatchRecordBuffer buffer = flushQueue.poll(2000L, TimeUnit.MILLISECONDS);
                    if (buffer == null || buffer.getLabelName() == null) {
                        // label is empty and does not need to load. It is the flag of waitUtilDone
                        continue;
                    }
                    recordList.add(buffer);
                    boolean merge = false;
                    if (!flushQueue.isEmpty()) {
                        flushQueue.drainTo(recordList, flushQueueSize - 1);
                        if (mergeBuffer(recordList, buffer)) {
                            load(buffer.getLabelName(), buffer);
                            merge = true;
                        }
                    }

                    if (!merge) {
                        for (BatchRecordBuffer bf : recordList) {
                            if (bf == null || bf.getLabelName() == null) {
                                continue;
                            }
                            load(bf.getLabelName(), bf);
                        }
                    }

                    if (flushQueue.size() < flushQueueSize) {
                        // Avoid waiting for 2 rounds of intervalMs
                        doFlush(null, false, false);
                    }
                } catch (Exception e) {
                    LOG.error("worker running error", e);
                    exception.set(e);
                    // clear queue to avoid writer thread blocking
                    flushQueue.clear();
                    break;
                }
            }
            LOG.info("LoadAsyncExecutor stop");
            loadThreadAlive = false;
        }

        /** execute stream load. */
        public void load(String label, BatchRecordBuffer buffer) throws IOException {
            if (enableGroupCommit) {
                label = null;
            }
            refreshLoadUrl(buffer.getDatabase(), buffer.getTable());

            BatchBufferHttpEntity entity = new BatchBufferHttpEntity(buffer);
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder
                    .setUrl(loadUrl)
                    .baseAuth(username, password)
                    .setLabel(label)
                    .addCommonHeader()
                    .setEntity(entity)
                    .addHiddenColumns(executionOptions.getDeletable())
                    .addProperties(executionOptions.getStreamLoadProp());

            if (enableGzCompress) {
                putBuilder.setEntity(new GzipCompressingEntity(entity));
            }
            Throwable resEx = new Throwable();
            int retry = 0;
            while (retry <= executionOptions.getMaxRetries()) {
                if (enableGroupCommit) {
                    LOG.info("stream load started with group commit on host {}", hostPort);
                } else {
                    LOG.info(
                            "stream load started for {} on host {}",
                            putBuilder.getLabel(),
                            hostPort);
                }

                try (CloseableHttpClient httpClient = httpClientBuilder.build()) {
                    try (CloseableHttpResponse response = httpClient.execute(putBuilder.build())) {
                        int statusCode = response.getStatusLine().getStatusCode();
                        String reason = response.getStatusLine().toString();
                        if (statusCode == 200 && response.getEntity() != null) {
                            String loadResult = EntityUtils.toString(response.getEntity());
                            LOG.info("load Result {}", loadResult);
                            RespContent respContent =
                                    OBJECT_MAPPER.readValue(loadResult, RespContent.class);
                            if (DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                                long cacheByteBeforeFlush =
                                        currentCacheBytes.getAndAdd(-respContent.getLoadBytes());
                                LOG.info(
                                        "load success, cacheBeforeFlushBytes: {}, currentCacheBytes : {}",
                                        cacheByteBeforeFlush,
                                        currentCacheBytes.get());
                                lock.lock();
                                try {
                                    block.signal();
                                } finally {
                                    lock.unlock();
                                }
                                return;
                            } else if (LoadStatus.LABEL_ALREADY_EXIST.equals(
                                    respContent.getStatus())) {
                                // todo: need to abort transaction when JobStatus not finished
                                putBuilder.setLabel(label + "_" + retry);
                                reason = respContent.getMessage();
                            } else {
                                String errMsg =
                                        String.format(
                                                "stream load error: %s, see more in %s",
                                                respContent.getMessage(),
                                                respContent.getErrorURL());
                                throw new DorisBatchLoadException(errMsg);
                            }
                        }
                        LOG.error(
                                "stream load failed with {}, reason {}, to retry",
                                hostPort,
                                reason);
                        if (retry == executionOptions.getMaxRetries()) {
                            resEx = new DorisRuntimeException("stream load failed with: " + reason);
                        }
                    } catch (Exception ex) {
                        resEx = ex;
                        LOG.error("stream load error with {}, to retry, cause by", hostPort, ex);
                    }
                }
                retry++;
                // get available backend retry
                refreshLoadUrl(buffer.getDatabase(), buffer.getTable());
                putBuilder.setUrl(loadUrl);
            }
            buffer.clear();
            buffer = null;

            if (retry >= executionOptions.getMaxRetries()) {
                throw new DorisBatchLoadException(
                        "stream load error: " + resEx.getMessage(), resEx);
            }
        }

        private void refreshLoadUrl(String database, String table) {
            hostPort = backendUtil.getAvailableBackend(subTaskId);
            loadUrl = String.format(LOAD_URL_PATTERN, hostPort, database, table);
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
    public void setBackendUtil(BackendUtil backendUtil) {
        this.backendUtil = backendUtil;
    }

    @VisibleForTesting
    public void setHttpClientBuilder(HttpClientBuilder httpClientBuilder) {
        this.httpClientBuilder = httpClientBuilder;
    }

    @VisibleForTesting
    public AtomicReference<Throwable> getException() {
        return exception;
    }

    @VisibleForTesting
    public boolean isLoadThreadAlive() {
        return loadThreadAlive;
    }
}
