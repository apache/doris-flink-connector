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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.writer.DorisAbstractWriter;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.LabelGenerator;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DorisCopyWriter<IN>
        implements DorisAbstractWriter<IN, DorisWriterState, DorisCopyCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisCopyWriter.class);
    private final long lastCheckpointId;
    private BatchStageLoad batchStageLoad;
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions executionOptions;
    private final String labelPrefix;
    private final LabelGenerator labelGenerator;
    private final long flushIntervalMs;
    private final DorisRecordSerializer<IN> serializer;
    private final transient ScheduledExecutorService scheduledExecutorService;
    private transient volatile Exception flushException = null;
    private String database;
    private String table;

    public DorisCopyWriter(
            Sink.InitContext initContext,
            DorisRecordSerializer<IN> serializer,
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions executionOptions) {
        if (!StringUtils.isNullOrWhitespaceOnly(dorisOptions.getTableIdentifier())) {
            String[] tableInfo = dorisOptions.getTableIdentifier().split("\\.");
            Preconditions.checkState(
                    tableInfo.length == 2,
                    "tableIdentifier input error, the format is database.table");
            this.database = tableInfo[0];
            this.table = tableInfo[1];
        }
        this.lastCheckpointId =
                initContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        LOG.info("restore checkpointId {}", lastCheckpointId);
        LOG.info("labelPrefix {}", executionOptions.getLabelPrefix());
        this.labelPrefix =
                executionOptions.getLabelPrefix()
                        + "_"
                        + UUID.randomUUID().toString().replaceAll("-", "");
        this.labelGenerator = new LabelGenerator(labelPrefix, false, initContext.getSubtaskId());
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(
                        1, new ExecutorThreadFactory("copy-upload-interval"));
        this.serializer = serializer;
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.executionOptions = executionOptions;
        this.flushIntervalMs = executionOptions.getBufferFlushIntervalMs();
        serializer.initial();
        initializeLoad();
    }

    public void initializeLoad() {
        this.batchStageLoad =
                new BatchStageLoad(
                        dorisOptions, dorisReadOptions, executionOptions, labelGenerator);
        this.batchStageLoad.setCurrentCheckpointID(lastCheckpointId + 1);
        // when uploading data in streaming mode, we need to regularly detect whether there are
        // exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(
                this::intervalFlush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void intervalFlush() {
        try {
            LOG.info("interval flush triggered.");
            batchStageLoad.flush(null, false);
        } catch (InterruptedException e) {
            flushException = e;
        }
    }

    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        checkFlushException();
        writeOneDorisRecord(serializer.serialize(in));
    }

    public void writeOneDorisRecord(DorisRecord record) throws InterruptedException {
        if (record == null || record.getRow() == null) {
            // ddl or value is null
            return;
        }
        String db = this.database;
        String tbl = this.table;
        // multi table load
        if (record.getTableIdentifier() != null) {
            db = record.getDatabase();
            tbl = record.getTable();
        }
        batchStageLoad.writeRecord(db, tbl, record.getRow());
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        checkFlushException();
        writeOneDorisRecord(serializer.flush());
        LOG.info("checkpoint flush triggered.");
        batchStageLoad.flush(null, true);
    }

    @Override
    public Collection<DorisCopyCommittable> prepareCommit()
            throws IOException, InterruptedException {
        Preconditions.checkState(batchStageLoad != null);
        LOG.info("checkpoint arrived, upload buffer to storage");
        List<DorisCopyCommittable> committables = new ArrayList<>();
        Map<String, List<String>> fileListMap = this.batchStageLoad.getFileListMap();
        for (Map.Entry<String, List<String>> entry : fileListMap.entrySet()) {
            String tableIdentifier = entry.getKey();
            List<String> fileList = entry.getValue();
            CopySQLBuilder copySQLBuilder =
                    new CopySQLBuilder(tableIdentifier, executionOptions, fileList);
            String copySql = copySQLBuilder.buildCopySQL();
            committables.add(new DorisCopyCommittable(dorisOptions.getFenodes(), copySql));
        }
        return committables;
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        Preconditions.checkState(batchStageLoad != null);
        if (!batchStageLoad.getFileListMap().isEmpty()) {
            LOG.info("clear the file list {}", batchStageLoad.getFileListMap());
            this.batchStageLoad.clearFileListMap();
        }

        this.batchStageLoad.setCurrentCheckpointID(checkpointId + 1);
        // Files will be automatically cleaned
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        LOG.info("DorisBatchWriter Close");
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        batchStageLoad.close();
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to streamload failed.", flushException);
        }
    }

    @VisibleForTesting
    public BatchStageLoad getBatchStageLoad() {
        return batchStageLoad;
    }
}
