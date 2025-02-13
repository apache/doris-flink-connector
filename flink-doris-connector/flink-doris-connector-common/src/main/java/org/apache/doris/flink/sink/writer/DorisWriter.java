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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.LabelAlreadyExistsException;
import org.apache.doris.flink.exception.StreamLoadException;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpUtil;
import org.apache.doris.flink.sink.LoadStatus;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.doris.flink.sink.LoadStatus.PUBLISH_TIMEOUT;
import static org.apache.doris.flink.sink.LoadStatus.SUCCESS;
import static org.apache.doris.flink.sink.writer.DorisStreamLoad.JOB_EXIST_FINISHED;

/**
 * Doris Writer will load data to doris.
 *
 * @param <IN>
 */
public class DorisWriter<IN>
        implements DorisAbstractWriter<IN, DorisWriterState, DorisCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriter.class);
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
    private final long lastCheckpointId;
    private long curCheckpointId;
    private Map<String, DorisStreamLoad> dorisStreamLoadMap = new ConcurrentHashMap<>();
    private Map<String, LabelGenerator> labelGeneratorMap = new ConcurrentHashMap<>();
    volatile boolean globalLoading;
    private Map<String, Boolean> loadingMap = new ConcurrentHashMap<>();
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions executionOptions;
    private String labelPrefix;
    private final int subtaskId;
    private final int intervalTime;
    private final DorisRecordSerializer<IN> serializer;
    private final transient ScheduledExecutorService scheduledExecutorService;
    private transient Thread executorThread;
    private transient volatile Exception loadException = null;
    private BackendUtil backendUtil;
    private SinkWriterMetricGroup sinkMetricGroup;
    private Map<String, DorisWriteMetrics> sinkMetricsMap = new ConcurrentHashMap<>();
    private volatile boolean multiTableLoad = false;

    public DorisWriter(
            Sink.InitContext initContext,
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
        LOG.info("restore from checkpointId {}", lastCheckpointId);
        LOG.info("labelPrefix {}", executionOptions.getLabelPrefix());
        this.labelPrefix = executionOptions.getLabelPrefix();
        this.subtaskId = initContext.getSubtaskId();
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("stream-load-check"));
        this.serializer = serializer;
        if (StringUtils.isBlank(dorisOptions.getTableIdentifier())) {
            this.multiTableLoad = true;
            LOG.info("table.identifier is empty, multiple table writes.");
        }
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.executionOptions = executionOptions;
        this.intervalTime = executionOptions.checkInterval();
        this.globalLoading = false;
        sinkMetricGroup = initContext.metricGroup();
        initializeLoad(state);
        serializer.initial();
    }

    public void initializeLoad(Collection<DorisWriterState> state) {
        this.backendUtil = BackendUtil.getInstance(dorisOptions, dorisReadOptions, LOG);
        try {
            if (executionOptions.enabled2PC()) {
                abortLingeringTransactions(state);
            }
        } catch (Exception e) {
            LOG.error("Failed to abort transaction.", e);
            throw new DorisRuntimeException(e);
        }
        // get main work thread.
        executorThread = Thread.currentThread();
        // todo: When writing to multiple tables,
        //  the checkdone thread may cause problems.
        if (!multiTableLoad && intervalTime > 1000) {
            // when uploading data in streaming mode, we need to regularly detect whether there are
            // exceptions.
            LOG.info("start stream load checkdone thread.");
            scheduledExecutorService.scheduleWithFixedDelay(
                    this::checkDone, 200, intervalTime, TimeUnit.MILLISECONDS);
        }
    }

    @VisibleForTesting
    public void abortLingeringTransactions(Collection<DorisWriterState> recoveredStates)
            throws Exception {
        List<String> alreadyAborts = new ArrayList<>();
        // abort label in state
        for (DorisWriterState state : recoveredStates) {
            LOG.info("try to abort txn from DorisWriterState {}", state.toString());
            // Todo: When the sink parallelism is reduced,
            //  the txn of the redundant task before aborting is also needed.
            if (!state.getLabelPrefix().equals(labelPrefix)) {
                LOG.warn(
                        "Label prefix from previous execution {} has changed to {}.",
                        state.getLabelPrefix(),
                        executionOptions.getLabelPrefix());
            }
            if (state.getDatabase() == null || state.getTable() == null) {
                LOG.warn(
                        "Transactions cannot be aborted when restore because the last used flink-doris-connector version less than 1.5.0.");
                continue;
            }
            String key = state.getDatabase() + "." + state.getTable();
            DorisStreamLoad streamLoader = getStreamLoader(key);
            streamLoader.abortPreCommit(state.getLabelPrefix(), curCheckpointId);
            alreadyAborts.add(state.getLabelPrefix());
        }

        // TODO: In a multi-table scenario, if do not restore from checkpoint,
        //  when modify labelPrefix at startup, we cannot abort the previous label.
        if (!alreadyAborts.contains(labelPrefix)
                && StringUtils.isNotEmpty(dorisOptions.getTableIdentifier())
                && StringUtils.isNotEmpty(labelPrefix)) {
            // abort current labelPrefix
            DorisStreamLoad streamLoader = getStreamLoader(dorisOptions.getTableIdentifier());
            streamLoader.abortPreCommit(labelPrefix, curCheckpointId);
        }
    }

    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        checkLoadException();
        writeOneDorisRecord(serializer.serialize(in));
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        writeOneDorisRecord(serializer.flush());
    }

    public void writeOneDorisRecord(DorisRecord record) throws IOException, InterruptedException {
        if (record == null || record.getRow() == null) {
            // ddl or value is null
            return;
        }

        // multi table load
        String tableKey = dorisOptions.getTableIdentifier();
        if (record.getTableIdentifier() != null) {
            tableKey = record.getTableIdentifier();
        }

        DorisStreamLoad streamLoader = getStreamLoader(tableKey);
        if (!loadingMap.containsKey(tableKey)) {
            // start stream load only when there has data
            LabelGenerator labelGenerator = getLabelGenerator(tableKey);
            String currentLabel = labelGenerator.generateTableLabel(curCheckpointId);
            streamLoader.startLoad(currentLabel, false);
            loadingMap.put(tableKey, true);
            globalLoading = true;
            registerMetrics(tableKey);
        }
        streamLoader.writeRecord(record.getRow());
    }

    @VisibleForTesting
    public void setSinkMetricGroup(SinkWriterMetricGroup sinkMetricGroup) {
        this.sinkMetricGroup = sinkMetricGroup;
    }

    public void registerMetrics(String tableKey) {
        if (sinkMetricsMap.containsKey(tableKey)) {
            return;
        }
        DorisWriteMetrics metrics = DorisWriteMetrics.of(sinkMetricGroup, tableKey);
        sinkMetricsMap.put(tableKey, metrics);
    }

    @Override
    public Collection<DorisCommittable> prepareCommit() throws IOException, InterruptedException {
        // Verify whether data is written during a checkpoint
        if (!globalLoading && loadingMap.values().stream().noneMatch(Boolean::booleanValue)) {
            return Collections.emptyList();
        }

        // disable exception checker before stop load.
        globalLoading = false;
        // submit stream load http request
        List<DorisCommittable> committableList = new ArrayList<>();
        for (Map.Entry<String, DorisStreamLoad> streamLoader : dorisStreamLoadMap.entrySet()) {
            String tableIdentifier = streamLoader.getKey();
            if (!loadingMap.getOrDefault(tableIdentifier, false)) {
                LOG.debug("skip table {}, no data need to load.", tableIdentifier);
                continue;
            }
            DorisStreamLoad dorisStreamLoad = streamLoader.getValue();
            RespContent respContent = dorisStreamLoad.stopLoad();
            // refresh metrics
            if (sinkMetricsMap.containsKey(tableIdentifier)) {
                DorisWriteMetrics dorisWriteMetrics = sinkMetricsMap.get(tableIdentifier);
                dorisWriteMetrics.flush(respContent);
            }
            if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                if (executionOptions.enabled2PC()
                        && LoadStatus.LABEL_ALREADY_EXIST.equals(respContent.getStatus())
                        && !JOB_EXIST_FINISHED.equals(respContent.getExistingJobStatus())) {
                    LOG.info(
                            "try to abort {} cause status {}, exist job status {} ",
                            respContent.getLabel(),
                            respContent.getStatus(),
                            respContent.getExistingJobStatus());
                    dorisStreamLoad.abortLabelExistTransaction(respContent);
                    throw new LabelAlreadyExistsException("Exist label abort finished, retry");
                } else {
                    String errMsg =
                            String.format(
                                    "table %s stream load error: %s, see more in %s",
                                    tableIdentifier,
                                    respContent.getMessage(),
                                    respContent.getErrorURL());
                    LOG.error("Failed to load, {}", errMsg);
                    throw new DorisRuntimeException(errMsg);
                }
            }
            if (executionOptions.enabled2PC()) {
                long txnId = respContent.getTxnId();
                committableList.add(
                        new DorisCommittable(
                                dorisStreamLoad.getHostPort(), dorisStreamLoad.getDb(), txnId));
            }
        }

        // clean loadingMap
        loadingMap.clear();
        return committableList;
    }

    private void abortPossibleSuccessfulTransaction() {
        // In the case of multi-table writing, if a new table is added during the period
        // (there is no streamloader for this table in the previous Checkpoint state),
        // in the precommit phase, if some tables succeed and others fail,
        // the txn of successful precommit cannot be aborted.
        if (executionOptions.enabled2PC() && multiTableLoad) {
            LOG.info("Try to abort may have successfully preCommitted label.");
            for (Map.Entry<String, DorisStreamLoad> entry : dorisStreamLoadMap.entrySet()) {
                DorisStreamLoad abortLoader = entry.getValue();
                try {
                    abortLoader.abortTransactionByLabel(abortLoader.getCurrentLabel());
                } catch (Exception ex) {
                    LOG.warn(
                            "Skip abort transaction failed by label, reason is {}.",
                            ex.getMessage());
                }
            }
        }
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        List<DorisWriterState> writerStates = new ArrayList<>();
        for (DorisStreamLoad dorisStreamLoad : dorisStreamLoadMap.values()) {
            // Dynamic refresh backend
            dorisStreamLoad.setHostPort(backendUtil.getAvailableBackend(subtaskId));
            DorisWriterState writerState =
                    new DorisWriterState(
                            labelPrefix,
                            dorisStreamLoad.getDb(),
                            dorisStreamLoad.getTable(),
                            subtaskId);
            writerStates.add(writerState);
        }
        this.curCheckpointId = checkpointId + 1;
        return writerStates;
    }

    private LabelGenerator getLabelGenerator(String tableKey) {
        return labelGeneratorMap.computeIfAbsent(
                tableKey,
                v ->
                        new LabelGenerator(
                                labelPrefix, executionOptions.enabled2PC(), tableKey, subtaskId));
    }

    @VisibleForTesting
    public DorisStreamLoad getStreamLoader(String tableKey) {
        LabelGenerator labelGenerator = getLabelGenerator(tableKey);
        dorisOptions.setTableIdentifier(tableKey);
        return dorisStreamLoadMap.computeIfAbsent(
                tableKey,
                v ->
                        new DorisStreamLoad(
                                backendUtil.getAvailableBackend(subtaskId),
                                dorisOptions,
                                executionOptions,
                                labelGenerator,
                                new HttpUtil(dorisReadOptions).getHttpClient()));
    }

    /** Check the streamload http request regularly. */
    private void checkDone() {
        for (Map.Entry<String, DorisStreamLoad> streamLoadMap : dorisStreamLoadMap.entrySet()) {
            checkAllDone(streamLoadMap.getKey(), streamLoadMap.getValue());
        }
    }

    private void checkAllDone(String tableIdentifier, DorisStreamLoad dorisStreamLoad) {
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
                        dorisStreamLoad.setHostPort(backendUtil.getAvailableBackend(subtaskId));
                        if (executionOptions.enabled2PC()) {
                            dorisStreamLoad.abortPreCommit(labelPrefix, curCheckpointId);
                        }
                        // start a new txn(stream load)
                        LOG.info(
                                "getting exception, breakpoint resume for checkpoint ID: {}, table {}",
                                curCheckpointId,
                                tableIdentifier);
                        LabelGenerator labelGenerator = getLabelGenerator(tableIdentifier);
                        dorisStreamLoad.startLoad(
                                labelGenerator.generateTableLabel(curCheckpointId), true);
                    } catch (Exception e) {
                        throw new DorisRuntimeException(e);
                    }
                } else {
                    String errorMsg;
                    try {
                        RespContent content =
                                dorisStreamLoad.handlePreCommitResponse(
                                        dorisStreamLoad.getPendingLoadFuture().get());
                        if (executionOptions.enabled2PC()
                                && LoadStatus.LABEL_ALREADY_EXIST.equals(content.getStatus())) {
                            LOG.info(
                                    "try to abort {} cause Label Already Exists",
                                    content.getLabel());
                            dorisStreamLoad.abortLabelExistTransaction(content);
                            errorMsg = "Exist label abort finished, retry";
                            LOG.info(errorMsg);
                            return;
                        } else {
                            errorMsg = content.getMessage();
                            loadException = new StreamLoadException(errorMsg);
                        }
                    } catch (Exception e) {
                        errorMsg = e.getMessage();
                        loadException = new DorisRuntimeException(e);
                    }

                    LOG.error(
                            "table {} stream load finished unexpectedly, interrupt worker thread! {}",
                            tableIdentifier,
                            errorMsg);

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

    @VisibleForTesting
    public void setDorisMetricsMap(Map<String, DorisWriteMetrics> metricsMap) {
        this.sinkMetricsMap = metricsMap;
    }

    @VisibleForTesting
    public void setBackendUtil(BackendUtil backendUtil) {
        this.backendUtil = backendUtil;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close DorisWriter.");
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        abortPossibleSuccessfulTransaction();

        if (dorisStreamLoadMap != null && !dorisStreamLoadMap.isEmpty()) {
            for (DorisStreamLoad dorisStreamLoad : dorisStreamLoadMap.values()) {
                dorisStreamLoad.close();
            }
        }
        serializer.close();
    }
}
