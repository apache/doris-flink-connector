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
import org.apache.flink.util.Preconditions;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.DorisCommittable;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.doris.flink.sink.LoadStatus.*;


/**
 * Doris Writer will load data to doris.
 * @param <IN>
 */
public class DorisCacheWriter<IN> implements SinkWriter<IN, DorisCommittable, DorisWriterState> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisCacheWriter.class);
    private static final List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
    private final long lastCheckpointId;
    private DorisStreamLoadImpl dorisStreamLoadImpl;
    private volatile boolean loading;
    private final String labelPrefix;
    private final LabelGenerator labelGenerator;
    private final DorisWriterState dorisWriterState;
    private final DorisRecordSerializer<IN> serializer;
    private transient volatile Exception loadException = null;
    private DorisStreamLoadManager dorisStreamLoadManager;
    private long curCheckpointId = -1;

    public DorisCacheWriter(Sink.InitContext initContext,
                            List<DorisWriterState> state,
                            DorisRecordSerializer<IN> serializer,
                            DorisOptions dorisOptions,
                            DorisReadOptions dorisReadOptions,
                            DorisExecutionOptions executionOptions) {
        this.lastCheckpointId =
                initContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        LOG.info("restore checkpointId {}", lastCheckpointId);
        LOG.info("labelPrefix " + executionOptions.getLabelPrefix());
        this.dorisWriterState = new DorisWriterState(executionOptions.getLabelPrefix());
        this.labelPrefix = executionOptions.getLabelPrefix() + "_" + initContext.getSubtaskId();
        this.labelGenerator = new LabelGenerator(labelPrefix, executionOptions.enabled2PC());
        this.serializer = serializer;

        StreamLoadPara para = new StreamLoadPara();
        para.lastCheckpointId = lastCheckpointId;
        para.labelPrefix = labelPrefix;
        para.dorisOptions = dorisOptions;
        para.dorisReadOptions = dorisReadOptions;
        para.executionOptions = executionOptions;
        this.dorisStreamLoadManager = DorisStreamLoadManager.getDorisStreamLoadManager();
        this.dorisStreamLoadManager.init(initContext.getSubtaskId(), para);
    }

    public void initializeLoad(List<DorisWriterState> state) throws IOException {
        // move DorisStreamLoadManager
        this.dorisStreamLoadManager.initializeLoad(state);
    }

    @Override
    public void write(IN in, Context context) throws IOException {
        checkLoadException();
        byte[] serialize = serializer.serialize(in);
        if(Objects.isNull(serialize)){
            return;
        }

        // TODO encap the next to DorisStreamLoadManager::write
        dorisStreamLoadManager.writeRecord(serialize, curCheckpointId);
    }

    @Override
    public List<DorisCommittable> prepareCommit(boolean flush) throws IOException {
        // disable exception checker before stop load.
        dorisStreamLoadManager.setLoading(false);
        Preconditions.checkState(dorisStreamLoadManager != null);
        RespContent respContent = dorisStreamLoadManager.stopLoad();
        if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
            String errMsg = String.format("stream load error: %s, see more in %s", respContent.getMessage(),
                                                respContent.getErrorURL());
            throw new DorisRuntimeException(errMsg);
        }
        if (!dorisStreamLoadManager.enabled2PC()) {
            return Collections.emptyList();
        }
        long txnId = respContent.getTxnId();

        return ImmutableList.of(new DorisCommittable(dorisStreamLoadManager.getHostPort(), dorisStreamLoadManager.getDb(),
                                    txnId, curCheckpointId));
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        Preconditions.checkState(dorisStreamLoadManager != null);
        long nextCheckpointId = checkpointId + 1;
        this.dorisStreamLoadManager.startLoad(labelGenerator.generateLabel(nextCheckpointId), nextCheckpointId);
        curCheckpointId = nextCheckpointId;
        dorisStreamLoadManager.setLoading(true);
        return Collections.singletonList(dorisWriterState);
    }

    private void checkLoadException() {
        if (loadException != null) {
            throw new RuntimeException("error while loading data.", loadException);
        }
    }

    @VisibleForTesting
    public boolean isLoading() {
        return dorisStreamLoadManager.isLoading();
    }

    @VisibleForTesting
    public void setDorisStreamLoad(DorisStreamLoadImpl streamLoad) {
        dorisStreamLoadManager.setDorisStreamLoad(streamLoad);
    }

    @Override
    public void close() throws Exception {
        if (dorisStreamLoadManager != null) {
            dorisStreamLoadManager.close();
        }
    }

}
