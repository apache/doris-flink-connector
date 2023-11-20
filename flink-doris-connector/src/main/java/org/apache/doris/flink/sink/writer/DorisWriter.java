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
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpUtil;
import org.apache.doris.flink.sink.batch.RecordWithMeta;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
    private Map<String, DorisStreamLoad> dorisStreamLoadMap = new ConcurrentHashMap<>();
    private Map<String, LabelGenerator> labelGeneratorMap = new ConcurrentHashMap<>();;
    volatile boolean globalLoading;
    private Map<String, Boolean> loadingMap = new ConcurrentHashMap<>();
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions executionOptions;
    private final String labelPrefix;
    private final int subtaskId;
    private final int intervalTime;
    private final DorisRecordSerializer<IN> serializer;
    private final transient ScheduledExecutorService scheduledExecutorService;
    private transient Thread executorThread;
    private transient volatile Exception loadException = null;
    private BackendUtil backendUtil;

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
        this.labelPrefix = executionOptions.getLabelPrefix();
        this.subtaskId = initContext.getSubtaskId();
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("stream-load-check"));
        this.serializer = serializer;
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.executionOptions = executionOptions;
        this.intervalTime = executionOptions.checkInterval();
        this.globalLoading = false;

        initializeLoad(state);
    }

    public void initializeLoad(Collection<DorisWriterState> state) {
        this.backendUtil = BackendUtil.getInstance(dorisOptions, dorisReadOptions, LOG);
        try {
            if(executionOptions.enabled2PC()) {
                abortLingeringTransactions(state);
            }
        } catch (Exception e) {
            LOG.error("Failed to abort transaction.", e);
            throw new DorisRuntimeException(e);
        }
        // get main work thread.
        executorThread = Thread.currentThread();
        // when uploading data in streaming mode, we need to regularly detect whether there are exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(this::checkDone, 200, intervalTime, TimeUnit.MILLISECONDS);
    }

    private void abortLingeringTransactions(Collection<DorisWriterState> recoveredStates) throws Exception {
        List<String> alreadyAborts = new ArrayList<>();
        //abort label in state
         for(DorisWriterState state : recoveredStates){
             // Todo: When the sink parallelism is reduced,
             //  the txn of the redundant task before aborting is also needed.
             if(!state.getLabelPrefix().equals(labelPrefix)){
                 LOG.warn("Label prefix from previous execution {} has changed to {}.", state.getLabelPrefix(), executionOptions.getLabelPrefix());
             }
             String key = state.getDatabase() + "." + state.getTable();
             DorisStreamLoad streamLoader = getStreamLoader(key);
             streamLoader.abortPreCommit(state.getLabelPrefix(), curCheckpointId);
             alreadyAborts.add(state.getLabelPrefix());
        }

        // TODO: In a multi-table scenario, if do not restore from checkpoint,
        //  when modify labelPrefix at startup, we cannot abort the previous label.
        if(!alreadyAborts.contains(labelPrefix)
                && StringUtils.isNotEmpty(dorisOptions.getTableIdentifier())
                && StringUtils.isNotEmpty(labelPrefix)){
            //abort current labelPrefix
            DorisStreamLoad streamLoader = getStreamLoader(dorisOptions.getTableIdentifier());
            streamLoader.abortPreCommit(labelPrefix, curCheckpointId);
        }
    }

    @Override
    public void write(IN in, Context context) throws IOException {
        checkLoadException();
        Tuple2<String, byte[]> rowTuple = serializeRecord(in);
        String tableKey = rowTuple.f0;
        byte[] serializeRow = rowTuple.f1;
        if(serializeRow == null){
            return;
        }

        DorisStreamLoad streamLoader = getStreamLoader(tableKey);
        if(!loadingMap.containsKey(tableKey)) {
            // start stream load only when there has data
            LabelGenerator labelGenerator = getLabelGenerator(tableKey);
            String currentLabel = labelGenerator.generateTableLabel(curCheckpointId);
            streamLoader.startLoad(currentLabel, false);
            loadingMap.put(tableKey, true);
            globalLoading = true;
        }
        streamLoader.writeRecord(serializeRow);
    }

    private Tuple2<String, byte[]> serializeRecord(IN in) throws IOException {
        String tableKey = dorisOptions.getTableIdentifier();
        byte[] serializeRow = null;
        if(serializer != null) {
            serializeRow = serializer.serialize(in);
            if(Objects.isNull(serializeRow)){
                //ddl record by JsonDebeziumSchemaSerializer
                return Tuple2.of(tableKey, null);
            }
        }
        //multi table load
        if(in instanceof RecordWithMeta){
            RecordWithMeta row = (RecordWithMeta) in;
            if(StringUtils.isBlank(row.getTable())
                    || StringUtils.isBlank(row.getDatabase())
                    || row.getRecord() == null){
                LOG.warn("Record or meta format is incorrect, ignore record db:{}, table:{}, row:{}", row.getDatabase(), row.getTable(), row.getRecord());
                return Tuple2.of(tableKey, null);
            }
            tableKey = row.getDatabase() + "." + row.getTable();
            serializeRow = row.getRecord().getBytes(StandardCharsets.UTF_8);
        }
        return Tuple2.of(tableKey, serializeRow);
    }

    @Override
    public void flush(boolean flush) throws IOException, InterruptedException {
        //No action is triggered, everything is in the precommit method
    }

    @Override
    public Collection<DorisCommittable> prepareCommit() throws IOException, InterruptedException {
        // Verify whether data is written during a checkpoint
        if(!globalLoading && loadingMap.values().stream().noneMatch(Boolean::booleanValue)){
            return Collections.emptyList();
        }
        // disable exception checker before stop load.
        globalLoading = false;
        // clean loadingMap
        loadingMap.clear();

        // submit stream load http request
        List<DorisCommittable> committableList = new ArrayList<>();
        for(Map.Entry<String, DorisStreamLoad> streamLoader : dorisStreamLoadMap.entrySet()){
            String tableIdentifier = streamLoader.getKey();
            DorisStreamLoad dorisStreamLoad = streamLoader.getValue();
            LabelGenerator labelGenerator = getLabelGenerator(tableIdentifier);
            String currentLabel = labelGenerator.generateTableLabel(curCheckpointId);
            RespContent respContent = dorisStreamLoad.stopLoad(currentLabel);
            if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                String errMsg = String.format("tabel {} stream load error: %s, see more in %s", tableIdentifier, respContent.getMessage(), respContent.getErrorURL());
                throw new DorisRuntimeException(errMsg);
            }
            if(executionOptions.enabled2PC()){
                long txnId = respContent.getTxnId();
                committableList.add(new DorisCommittable(dorisStreamLoad.getHostPort(), dorisStreamLoad.getDb(), txnId));
            }
        }
        return committableList;
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        List<DorisWriterState> writerStates = new ArrayList<>();
        for(DorisStreamLoad dorisStreamLoad : dorisStreamLoadMap.values()){
            //Dynamic refresh backend
            dorisStreamLoad.setHostPort(backendUtil.getAvailableBackend());
            DorisWriterState writerState = new DorisWriterState(labelPrefix, dorisStreamLoad.getDb(), dorisStreamLoad.getTable(), subtaskId);
            writerStates.add(writerState);
        }
        this.curCheckpointId = checkpointId + 1;
        return writerStates;
    }

    private LabelGenerator getLabelGenerator(String tableKey){
        return labelGeneratorMap.computeIfAbsent(tableKey, v-> new LabelGenerator(labelPrefix, executionOptions.enabled2PC(), tableKey, subtaskId));
    }

    private DorisStreamLoad getStreamLoader(String tableKey){
        LabelGenerator labelGenerator = getLabelGenerator(tableKey);
        dorisOptions.setTableIdentifier(tableKey);
        return dorisStreamLoadMap.computeIfAbsent(tableKey, v -> new DorisStreamLoad(backendUtil.getAvailableBackend(),
                dorisOptions,
                executionOptions,
                labelGenerator,
                new HttpUtil().getHttpClient()));
    }

    /**
     * Check the streamload http request regularly
     */
    private void checkDone() {
        for(Map.Entry<String, DorisStreamLoad> streamLoadMap : dorisStreamLoadMap.entrySet()){
            checkAllDone(streamLoadMap.getKey(), streamLoadMap.getValue());
        }
    }

    private void checkAllDone(String tableIdentifier, DorisStreamLoad dorisStreamLoad){
        // the load future is done and checked in prepareCommit().
        // this will check error while loading.
        LOG.debug("start timer checker, interval {} ms", intervalTime);
        if (dorisStreamLoad.getPendingLoadFuture() != null
                && dorisStreamLoad.getPendingLoadFuture().isDone()) {
            if (!globalLoading || !loadingMap.get(tableIdentifier)) {
                LOG.debug("not loading, skip timer checker for table {}", tableIdentifier);
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
                        dorisStreamLoad.setHostPort(backendUtil.getAvailableBackend());
                        if (executionOptions.enabled2PC()) {
                            dorisStreamLoad.abortPreCommit(labelPrefix, curCheckpointId);
                        }
                        // start a new txn(stream load)
                        LOG.info("getting exception, breakpoint resume for checkpoint ID: {}, table {}", curCheckpointId, tableIdentifier);
                        LabelGenerator labelGenerator = getLabelGenerator(tableIdentifier);
                        dorisStreamLoad.startLoad(labelGenerator.generateTableLabel(curCheckpointId), true);
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
                    LOG.error("table {} stream load finished unexpectedly, interrupt worker thread! {}", tableIdentifier, errorMsg);
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
        return this.globalLoading;
    }

    @VisibleForTesting
    public void setDorisStreamLoadMap(Map<String, DorisStreamLoad> streamLoadMap) {
        this.dorisStreamLoadMap = streamLoadMap;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close DorisWriter.");
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        if (dorisStreamLoadMap != null && !dorisStreamLoadMap.isEmpty()) {
            for(DorisStreamLoad dorisStreamLoad : dorisStreamLoadMap.values()){
                dorisStreamLoad.close();
            }
        }
    }

}
