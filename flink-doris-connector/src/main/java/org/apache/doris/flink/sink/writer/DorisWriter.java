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

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.StreamLoadException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpUtil;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.doris.flink.sink.LoadStatus.PUBLISH_TIMEOUT;
import static org.apache.doris.flink.sink.LoadStatus.SUCCESS;

/**
 * Doris Writer will load data to doris.
 * @param <IN>
 */
public class DorisWriter<IN> implements StatefulSink.StatefulSinkWriter<IN, DorisWriterState>,
        TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, DorisCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriter.class);
    private static final List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
    private final long lastCheckpointId;
    private long curCheckpointId;
    private DorisStreamLoad dorisStreamLoad;
    volatile boolean loading;
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions executionOptions;
    private final String labelPrefix;
    private final LabelGenerator labelGenerator;
    private final int intervalTime;
    private final DorisWriterState dorisWriterState;
    private final DorisRecordSerializer<IN> serializer;
    private final transient ScheduledExecutorService scheduledExecutorService;
    private transient Thread executorThread;
    private transient volatile Exception loadException = null;
    private BackendUtil backendUtil;
    private String currentLabel;

    public DorisWriter(Sink.InitContext initContext,
                       Collection<DorisWriterState> state,
                       DorisRecordSerializer<IN> serializer,
                       DorisOptions dorisOptions,
                       DorisReadOptions dorisReadOptions,
                       DorisExecutionOptions executionOptions) {
        this.lastCheckpointId =
                initContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        this.curCheckpointId = lastCheckpointId + 1;
        LOG.info("restore checkpointId {}", lastCheckpointId);
        LOG.info("labelPrefix " + executionOptions.getLabelPrefix());
        this.dorisWriterState = new DorisWriterState(executionOptions.getLabelPrefix());
        this.labelPrefix = executionOptions.getLabelPrefix() + "_" + initContext.getSubtaskId();
        this.labelGenerator = new LabelGenerator(labelPrefix, executionOptions.enabled2PC());
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("stream-load-check"));
        this.serializer = serializer;
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.executionOptions = executionOptions;
        this.intervalTime = executionOptions.checkInterval();
        this.loading = false;

        initializeLoad(state);
    }

    public void initializeLoad(Collection<DorisWriterState> state) {
        this.backendUtil = StringUtils.isNotEmpty(dorisOptions.getBenodes()) ? new BackendUtil(
                dorisOptions.getBenodes())
                : new BackendUtil(RestService.getBackendsV2(dorisOptions, dorisReadOptions, LOG));
        try {
            this.dorisStreamLoad = new DorisStreamLoad(
                    backendUtil.getAvailableBackend(),
                    dorisOptions,
                    executionOptions,
                    labelGenerator, new HttpUtil().getHttpClient());
            // TODO: we need check and abort all pending transaction.
            //  Discard transactions that may cause the job to fail.
            if(executionOptions.enabled2PC()) {
                dorisStreamLoad.abortPreCommit(labelPrefix, curCheckpointId);
            }
        } catch (Exception e) {
            throw new DorisRuntimeException(e);
        }
        // get main work thread.
        executorThread = Thread.currentThread();
        this.currentLabel = labelGenerator.generateLabel(curCheckpointId);
        // when uploading data in streaming mode, we need to regularly detect whether there are exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(this::checkDone, 200, intervalTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(IN in, Context context) throws IOException {
        checkLoadException();
        byte[] serialize = serializer.serialize(in);
        if(Objects.isNull(serialize)){
            //ddl record
            return;
        }
        if(!loading) {
            // start stream load only when there has data
            dorisStreamLoad.startLoad(currentLabel, false);
            loading = true;
        }
        dorisStreamLoad.writeRecord(serialize);
    }

    @Override
    public void flush(boolean flush) throws IOException, InterruptedException {

    }


    @Override
    public Collection<DorisCommittable> prepareCommit() throws IOException, InterruptedException {
        if(!loading){
            //There is no data during the entire checkpoint period
            return Collections.emptyList();
        }
        // disable exception checker before stop load.
        loading = false;
        Preconditions.checkState(dorisStreamLoad != null);
        RespContent respContent = dorisStreamLoad.stopLoad(currentLabel);
        if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
            String errMsg = String.format("stream load error: %s, see more in %s", respContent.getMessage(), respContent.getErrorURL());
            throw new DorisRuntimeException(errMsg);
        }
        if (!executionOptions.enabled2PC()) {
            return Collections.emptyList();
        }
        long txnId = respContent.getTxnId();
        return ImmutableList.of(new DorisCommittable(dorisStreamLoad.getHostPort(), dorisStreamLoad.getDb(), txnId));
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        Preconditions.checkState(dorisStreamLoad != null);
        // dynamic refresh BE node
        this.dorisStreamLoad.setHostPort(backendUtil.getAvailableBackend());
        this.curCheckpointId = checkpointId + 1;
        this.currentLabel = labelGenerator.generateLabel(curCheckpointId);
        return Collections.singletonList(dorisWriterState);
    }

    private void checkDone() {
        // the load future is done and checked in prepareCommit().
        // this will check error while loading.
        LOG.debug("start timer checker, interval {} ms", intervalTime);
        if (dorisStreamLoad.getPendingLoadFuture() != null
                && dorisStreamLoad.getPendingLoadFuture().isDone()) {
            if (!loading) {
                LOG.debug("not loading, skip timer checker");
                return;
            }

            // double-check the future, to avoid getting the old future
            if (dorisStreamLoad.getPendingLoadFuture() != null
                    && dorisStreamLoad.getPendingLoadFuture().isDone()) {
                // error happened when loading, now we should stop receive data
                // and abort previous txn(stream load) and start a new txn(stream load)
                // use send cached data to new txn, then notify to restart the stream
                if (executionOptions.isUseCache()) {
                    try {
                        this.dorisStreamLoad.setHostPort(backendUtil.getAvailableBackend());
                        if (executionOptions.enabled2PC()) {
                            dorisStreamLoad.abortPreCommit(labelPrefix, curCheckpointId);
                        }
                        // start a new txn(stream load)
                        LOG.info("getting exception, breakpoint resume for checkpoint ID: {}", curCheckpointId);
                        dorisStreamLoad.startLoad(labelGenerator.generateLabel(curCheckpointId), true);
                    } catch (Exception e) {
                        throw new DorisRuntimeException(e);
                    }
                } else {
                    String errorMsg;
                    try {
                        RespContent content = dorisStreamLoad.handlePreCommitResponse(dorisStreamLoad.getPendingLoadFuture().get());
                        errorMsg = content.getMessage();
                    } catch (Exception e) {
                        errorMsg = e.getMessage();
                    }

                    loadException = new StreamLoadException(errorMsg);
                    LOG.error("stream load finished unexpectedly, interrupt worker thread! {}", errorMsg);
                    // set the executor thread interrupted in case blocking in write data.
                    executorThread.interrupt();
                }
            }
        }
    }

    private void checkLoadException() {
        if (loadException != null) {
            throw new RuntimeException("error while loading data.", loadException);
        }
    }

    @VisibleForTesting
    public boolean isLoading() {
        return this.loading;
    }

    @VisibleForTesting
    public void setDorisStreamLoad(DorisStreamLoad streamLoad) {
        this.dorisStreamLoad = streamLoad;
    }

    @Override
    public void close() throws Exception {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        if (dorisStreamLoad != null) {
            dorisStreamLoad.close();
        }
    }

}
