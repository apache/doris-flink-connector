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

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Flink 2.x specific wrapper for the shared core {@link DorisWriter} implementation.
 *
 * <p>This class adapts Flink 2.x's {@link WriterInitContext} to the version-neutral core writer in
 * the base module while implementing {@link DorisAbstractWriter} for use by the 2.x connector
 * module.
 */
public class DorisWriterAdapter<IN>
        implements DorisAbstractWriter<IN, DorisWriterState, DorisCommittable> {

    private final DorisWriter<IN> delegate;

    public DorisWriterAdapter(
            WriterInitContext initContext,
            Collection<DorisWriterState> state,
            DorisRecordSerializer<IN> serializer,
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions executionOptions) {

        long lastCheckpointId =
                initContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        int subtaskId = initContext.getTaskInfo().getIndexOfThisSubtask();
        SinkWriterMetricGroup sinkMetricGroup = initContext.metricGroup();

        this.delegate =
                new DorisWriter<>(
                        lastCheckpointId,
                        subtaskId,
                        sinkMetricGroup,
                        state,
                        serializer,
                        dorisOptions,
                        dorisReadOptions,
                        executionOptions);
    }

    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        delegate.write(in, context);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        delegate.flush(endOfInput);
    }

    @Override
    public Collection<DorisCommittable> prepareCommit() throws IOException, InterruptedException {
        return delegate.prepareCommit();
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        return delegate.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
