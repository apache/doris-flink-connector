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

package org.apache.doris.flink.sink.writer.tvf;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.S3TvfOptions;
import org.apache.doris.flink.sink.writer.DorisAbstractWriter;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/** Flink 1.x wrapper for the shared S3 TVF writer. */
public class S3TvfWriterAdapter<IN>
        implements DorisAbstractWriter<IN, DorisWriterState, S3TvfCommittable> {

    private final S3TvfWriter<IN> delegate;

    public S3TvfWriterAdapter(
            Sink.InitContext initContext,
            DorisRecordSerializer<IN> serializer,
            DorisOptions dorisOptions,
            DorisExecutionOptions executionOptions) {
        S3TvfOptions s3Options =
                Preconditions.checkNotNull(
                        executionOptions.getS3TvfOptions(), "S3 TVF options must be configured.");
        Preconditions.checkArgument(
                serializer instanceof S3TvfRowDataSerializer,
                "TVF write mode requires S3TvfRowDataSerializer.");

        String[] tableIdentifier = dorisOptions.getTableIdentifier().split("\\.", -1);
        Preconditions.checkArgument(
                tableIdentifier.length == 2
                        && !tableIdentifier[0].isEmpty()
                        && !tableIdentifier[1].isEmpty(),
                "table.identifier must use the database.table format in TVF write mode.");

        long restoredCheckpointId =
                initContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        S3TvfRowDataSerializer rowDataSerializer = (S3TvfRowDataSerializer) serializer;
        this.delegate =
                new S3TvfWriter<>(
                        restoredCheckpointId,
                        initContext.getSubtaskId(),
                        serializer,
                        new S3ClientObjectStore(s3Options),
                        tableIdentifier[0],
                        tableIdentifier[1],
                        s3Options.getPrefix(),
                        executionOptions.getLabelPrefix(),
                        rowDataSerializer.getSelectedColumns(),
                        rowDataSerializer.isDeleteSignEnabled(),
                        executionOptions.getBufferFlushMaxBytes());
    }

    @Override
    public void write(IN value, Context context) throws IOException {
        delegate.write(value);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        delegate.flush();
    }

    @Override
    public Collection<S3TvfCommittable> prepareCommit() throws IOException {
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
