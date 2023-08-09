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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisBatchLoadException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.EscapeHandler;
import org.apache.doris.flink.sink.HttpPutBuilder;
import org.apache.doris.flink.sink.HttpUtil;
import org.apache.doris.flink.sink.writer.LabelGenerator;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.doris.flink.sink.LoadStatus.PUBLISH_TIMEOUT;
import static org.apache.doris.flink.sink.LoadStatus.SUCCESS;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

/**
 * async stream load
 **/
public class DorisBatchStreamLoad implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisBatchStreamLoad.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
    private final LabelGenerator labelGenerator;
    private final byte[] lineDelimiter;
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private String loadUrl;
    private String hostPort;
    private final String username;
    private final String password;
    private final String db;
    private final String table;
    private final Properties loadProps;
    private BatchRecordBuffer buffer;
    private DorisExecutionOptions executionOptions;
    private ExecutorService loadExecutorService;
    private LoadAsyncExecutor loadAsyncExecutor;
    private BlockingQueue<BatchRecordBuffer> writeQueue;
    private BlockingQueue<BatchRecordBuffer> readQueue;
    private final AtomicBoolean started;
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);
    private CloseableHttpClient httpClient = new HttpUtil().getHttpClient();
    private BackendUtil backendUtil;

    public DorisBatchStreamLoad(DorisOptions dorisOptions,
                                DorisReadOptions dorisReadOptions,
                                DorisExecutionOptions executionOptions,
                                LabelGenerator labelGenerator) {
        this.backendUtil = new BackendUtil(RestService.getBackendsV2(dorisOptions, dorisReadOptions, LOG));
        this.hostPort = backendUtil.getAvailableBackend();
        String[] tableInfo = dorisOptions.getTableIdentifier().split("\\.");
        this.db = tableInfo[0];
        this.table = tableInfo[1];
        this.username = dorisOptions.getUsername();
        this.password = dorisOptions.getPassword();
        this.loadUrl = String.format(LOAD_URL_PATTERN, hostPort, db, table);
        this.loadProps = executionOptions.getStreamLoadProp();
        this.labelGenerator = labelGenerator;
        this.lineDelimiter = EscapeHandler.escapeString(loadProps.getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT)).getBytes();
        this.executionOptions = executionOptions;
        //init queue
        this.writeQueue = new ArrayBlockingQueue<>(executionOptions.getFlushQueueSize());
        LOG.info("init RecordBuffer capacity {}, count {}", executionOptions.getBufferFlushMaxBytes(), executionOptions.getFlushQueueSize());
        for (int index = 0; index < executionOptions.getFlushQueueSize(); index++) {
            this.writeQueue.add(new BatchRecordBuffer(this.lineDelimiter, executionOptions.getBufferFlushMaxBytes()));
        }
        readQueue = new LinkedBlockingDeque<>();

        this.loadAsyncExecutor= new LoadAsyncExecutor();
        this.loadExecutorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1), new DefaultThreadFactory("streamload-executor"), new ThreadPoolExecutor.AbortPolicy());
        this.started = new AtomicBoolean(true);
        this.loadExecutorService.execute(loadAsyncExecutor);
    }

    /**
     * write record into cache.
     * @param record
     * @throws IOException
     */
    public synchronized void writeRecord(byte[] record) throws InterruptedException {
        checkFlushException();
        if(buffer == null){
            buffer = takeRecordFromWriteQueue();
        }
        buffer.insert(record);
        //When it exceeds 80% of the byteSize,to flush, to avoid triggering bytebuffer expansion
        if (buffer.getBufferSizeBytes() >= executionOptions.getBufferFlushMaxBytes() * 0.8
                || (executionOptions.getBufferFlushMaxRows() != 0 && buffer.getNumOfRecords() >= executionOptions.getBufferFlushMaxRows())) {
            flush(false);
        }
    }

    public synchronized void flush(boolean waitUtilDone) throws InterruptedException {
        checkFlushException();
        if (buffer == null) {
            LOG.debug("buffer is empty, skip flush.");
            return;
        }
        buffer.setLabelName(labelGenerator.generateBatchLabel());
        BatchRecordBuffer tmpBuff = buffer;
        readQueue.put(tmpBuff);
        if(waitUtilDone){
            waitAsyncLoadFinish();
        }
        this.buffer = null;
    }

    private void putRecordToWriteQueue(BatchRecordBuffer buffer){
        try {
            writeQueue.put(buffer);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to recycle a buffer to queue");
        }
    }

    private BatchRecordBuffer takeRecordFromWriteQueue(){
        checkFlushException();
        try {
            return writeQueue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to take a buffer from queue");
        }
    }

    private void checkFlushException() {
        if (exception.get() != null) {
            throw new DorisBatchLoadException(exception.get());
        }
    }

    private void waitAsyncLoadFinish() throws InterruptedException {
        for(int i = 0; i < executionOptions.getFlushQueueSize() + 1 ; i++){
            BatchRecordBuffer empty = takeRecordFromWriteQueue();
            readQueue.put(empty);
        }
    }

    public void close(){
        //close async executor
        this.loadExecutorService.shutdown();
        this.started.set(false);

        //clear buffer
        this.writeQueue.clear();
        this.readQueue.clear();
    }

    class LoadAsyncExecutor implements Runnable {
        @Override
        public void run() {
            LOG.info("LoadAsyncExecutor start");
            while (started.get()) {
                BatchRecordBuffer buffer = null;
                try {
                    buffer = readQueue.poll(2000L, TimeUnit.MILLISECONDS);
                    if(buffer == null){
                        continue;
                    }
                    if (buffer.getLabelName() != null) {
                        load(buffer.getLabelName(), buffer);
                    }
                } catch (Exception e) {
                    LOG.error("worker running error", e);
                    exception.set(e);
                    break;
                } finally {
                    //Recycle buffer to avoid writer thread blocking
                    if(buffer != null){
                        buffer.clear();
                        putRecordToWriteQueue(buffer);
                    }
                }
            }
            LOG.info("LoadAsyncExecutor stop");
        }

        /**
         * execute stream load
         */
        public void load(String label, BatchRecordBuffer buffer) throws IOException{
            refreshLoadUrl();
            ByteBuffer data = buffer.getData();
            ByteArrayEntity entity = new ByteArrayEntity(data.array(), data.arrayOffset(), data.limit());
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder.setUrl(loadUrl)
                    .baseAuth(username, password)
                    .setLabel(label)
                    .addCommonHeader()
                    .setEntity(entity)
                    .addHiddenColumns(executionOptions.getDeletable())
                    .addProperties(executionOptions.getStreamLoadProp());

            int retry = 0;
            while (retry <= executionOptions.getMaxRetries()) {
                LOG.info("stream load started for {} on host {}", label, hostPort);
                try (CloseableHttpResponse response = httpClient.execute(putBuilder.build())) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode == 200 && response.getEntity() != null) {
                        String loadResult = EntityUtils.toString(response.getEntity());
                        LOG.info("load Result {}", loadResult);
                        RespContent respContent = OBJECT_MAPPER.readValue(loadResult, RespContent.class);
                        if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                            String errMsg = String.format("stream load error: %s, see more in %s", respContent.getMessage(), respContent.getErrorURL());
                            throw new DorisBatchLoadException(errMsg);
                        }else{
                            return;
                        }
                    }
                    LOG.error("stream load failed with {}, reason {}, to retry", hostPort, response.getStatusLine().toString());
                }catch (Exception ex){
                    if (retry == executionOptions.getMaxRetries()) {
                        throw new DorisBatchLoadException("stream load error: ", ex);
                    }
                    LOG.error("stream load error with {}, to retry, cause by", hostPort, ex);

                }
                retry++;
                // get available backend retry
                refreshLoadUrl();
                putBuilder.setUrl(loadUrl);
            }
        }

        private void refreshLoadUrl(){
            hostPort = backendUtil.getAvailableBackend();
            loadUrl = String.format(LOAD_URL_PATTERN, hostPort, db, table);
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
}
