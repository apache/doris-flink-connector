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
package org.apache.doris.flink.sink.writer;

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.HttpUtil;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisStreamLoadManager {
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoadManager.class);

    private ConcurrentHashMap<Long, RecordBufferCache> cpkDataCache;
    private DorisStreamLoadImpl dorisStreamLoadImpl;
    private volatile boolean loading;
    private StreamLoadPara para;
    private String labelPrefix;
    private LabelGenerator labelGenerator;
    private transient ScheduledExecutorService scheduledExecutorService;
    private long curLoadingCheckpoint = -1;
    private boolean init = false;

    private static class SingletonHolder {
        private static final DorisStreamLoadManager INSTANCE = new DorisStreamLoadManager();
    }

    public static DorisStreamLoadManager getDorisStreamLoadManager() {
        return SingletonHolder.INSTANCE;
    }

    private DorisStreamLoadManager(){

    }

    public void init(StreamLoadPara para){
        cpkDataCache = new ConcurrentHashMap<>();
        this.para = para;
        this.labelPrefix = para.labelPrefix;
        this.labelGenerator = new LabelGenerator(labelPrefix, para.executionOptions.enabled2PC());
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("stream-load-check"));
        this.curLoadingCheckpoint = para.lastCheckpointId + 1;
        this.init = true;
    }

    public boolean isInit() {
        return this.init;
    }

    /**
     * 1, init the stream load,
     * 2, prepare the load,
     * 3,start the new stream load
     * @param state
     * @throws IOException
     */
    public void initializeLoad(List<DorisWriterState> state) throws IOException {
        try {
            this.dorisStreamLoadImpl = new DorisStreamLoadImpl(
                    RestService.getBackend(para.dorisOptions, para.dorisReadOptions, LOG),
                    para.dorisOptions,
                    para.executionOptions,
                    labelGenerator, new HttpUtil().getHttpClient());
            // TODO: we need check and abort all pending transaction.
            //  Discard transactions that may cause the job to fail.
            if(para.executionOptions.enabled2PC()) {
                dorisStreamLoadImpl.abortPreCommit(labelPrefix,  curLoadingCheckpoint);
            }
        } catch (Exception e) {
            throw new DorisRuntimeException(e);
        }
        dorisStreamLoadImpl.startLoad(labelGenerator.generateLabel(curLoadingCheckpoint),
                                          getStreamDataCache(curLoadingCheckpoint), false);
        // when uploading data in streaming mode, we need to regularly detect whether there are exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(this::checkDone, 200,
                                                            para.executionOptions.checkInterval(),
                                                            TimeUnit.MILLISECONDS);
    }

    /**
     * stop the stream load
     * @return
     * @throws IOException
     */
    public RespContent stopLoad() throws IOException{
        return dorisStreamLoadImpl.stopLoad();
    }

    /**
     * start a new stream load
     * @param label
     * @param checkpointId
     * @throws IOException
     */
    public void startLoad(String label, long checkpointId) throws IOException{
        dorisStreamLoadImpl.startLoad(label, getStreamDataCache(checkpointId),false);
    }

    public void close() throws IOException {
        dorisStreamLoadImpl.close();
    }

    private void checkDone() {
        // the load future is done and checked in prepareCommit().
        // this will check error while loading.
        LOG.debug("start timer checker, interval {} ms", para.executionOptions.checkInterval());
        if (dorisStreamLoadImpl.getPendingLoadFuture() != null
                && dorisStreamLoadImpl.getPendingLoadFuture().isDone()) {
            if (!loading) {
                LOG.debug("not loading, skip timer checker");
                return;
            }

            // double-check the future, to avoid getting the old future
            if (dorisStreamLoadImpl.getPendingLoadFuture() != null
                    && dorisStreamLoadImpl.getPendingLoadFuture().isDone()) {
                // error happened when loading, now we should stop receive data
                // and abort previous txn(stream load) and start a new txn(stream load)
                // use send cached data to new txn, then notify to restart the stream
                try {
                    this.dorisStreamLoadImpl.setHostPort(RestService.getBackend(para.dorisOptions, para.dorisReadOptions, LOG));
                    // TODO: we need check and abort all pending transaction.
                    //  Discard transactions that may cause the job to fail.
                    if(para.executionOptions.enabled2PC()) {
                        dorisStreamLoadImpl.abortPreCommit(labelPrefix, curLoadingCheckpoint);
                    }
                } catch (Exception e) {
                    throw new DorisRuntimeException(e);
                }

                try {
                    // start a new txn(stream load)
                    LOG.info("getting exception, breakpoint resume for checkpoint ID: {}", curLoadingCheckpoint);
                    dorisStreamLoadImpl.startLoad(labelGenerator.generateLabel(curLoadingCheckpoint),
                                                      getStreamDataCache(curLoadingCheckpoint), true);
                } catch (Exception e) {
                    throw new DorisRuntimeException(e);
                }
            }
        }
    }


    public void writeRecord(byte[] buf) throws IOException {
        dorisStreamLoadImpl.writeRecord(buf);
    }

    public RecordBufferCache getStreamDataCache(long cpk) {
        if (!cpkDataCache.containsKey(cpk)) {
            LOG.info("create data cache for checkpoint ID: {}", cpk);
            cpkDataCache.put(cpk, new RecordBufferCache(para.executionOptions.getBufferSize()));
        }
        return cpkDataCache.get(cpk);
    }

    public void remove(long cpk) {
        // TODO recycle the RecordBufferCache
        if (cpkDataCache.containsKey(cpk)) {
            LOG.info("cleaning cache for checkpoint ID: {}", cpk);
            //recycle all buffer to ByteBufferManager
            cpkDataCache.get(cpk).recycle();
            cpkDataCache.remove(cpk);
        }
        curLoadingCheckpoint = cpk + 1;
    }

    public void setLoading(boolean loading) {
        this.loading = loading;
    }

    public boolean enabled2PC() {
        return this.para.executionOptions.enabled2PC();
    }

    public String getDb() {
        return this.dorisStreamLoadImpl.getDb();
    }

    public String getHostPort() {
        return this.dorisStreamLoadImpl.getHostPort();
    }

    public void setDorisStreamLoad(DorisStreamLoadImpl streamLoad) {
        this.dorisStreamLoadImpl = streamLoad;
    }

    public boolean isLoading() {
        return this.loading;
    }
}
