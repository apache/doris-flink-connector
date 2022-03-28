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

package org.apache.doris.flink.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.committer.DorisCommitter;
import org.apache.doris.flink.sink.writer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.DorisWriter;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.DorisWriterStateSerializer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Load data into Doris based on 2PC.
 * see {@link DorisWriter} and {@link DorisCommitter}.
 * @param <IN> type of record.
 */
public class DorisSink<IN> implements Sink<IN, DorisCommittable, DorisWriterState, DorisCommittable> {

    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions dorisExecutionOptions;
    private final DorisRecordSerializer<IN> serializer;

    public DorisSink(DorisOptions dorisOptions,
                     DorisReadOptions dorisReadOptions,
                     DorisExecutionOptions dorisExecutionOptions,
                     DorisRecordSerializer<IN> serializer) {
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.dorisExecutionOptions = dorisExecutionOptions;
        this.serializer = serializer;
    }

    @Override
    public SinkWriter<IN, DorisCommittable, DorisWriterState> createWriter(InitContext initContext, List<DorisWriterState> state) throws IOException {
        DorisWriter<IN> dorisWriter = new DorisWriter<IN>(initContext, state, serializer, dorisOptions, dorisReadOptions, dorisExecutionOptions);
        dorisWriter.initializeLoad(state);
        return dorisWriter;
    }

    @Override
    public Optional<SimpleVersionedSerializer<DorisWriterState>> getWriterStateSerializer() {
        return Optional.of(new DorisWriterStateSerializer());
    }

    @Override
    public Optional<Committer<DorisCommittable>> createCommitter() throws IOException {
        return Optional.of(new DorisCommitter(dorisOptions, dorisReadOptions, dorisExecutionOptions.getMaxRetries()));
    }

    @Override
    public Optional<GlobalCommitter<DorisCommittable, DorisCommittable>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DorisCommittable>> getCommittableSerializer() {
        return Optional.of(new DorisCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<DorisCommittable>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    public static <IN> Builder<IN> builder() {
        return new Builder<>();
    }

    /**
     * build for DorisSink.
     * @param <IN> record type.
     */
    public static class Builder<IN> {
        private DorisOptions dorisOptions;
        private DorisReadOptions dorisReadOptions;
        private DorisExecutionOptions dorisExecutionOptions;
        private DorisRecordSerializer<IN> serializer;

        public Builder<IN> setDorisOptions(DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public Builder<IN> setDorisReadOptions(DorisReadOptions dorisReadOptions) {
            this.dorisReadOptions = dorisReadOptions;
            return this;
        }

        public Builder<IN> setDorisExecutionOptions(DorisExecutionOptions dorisExecutionOptions) {
            this.dorisExecutionOptions = dorisExecutionOptions;
            return this;
        }

        public Builder<IN> setSerializer(DorisRecordSerializer<IN> serializer) {
            this.serializer = serializer;
            return this;
        }

        public DorisSink<IN> build() {
            Preconditions.checkNotNull(dorisOptions);
            Preconditions.checkNotNull(dorisExecutionOptions);
            Preconditions.checkNotNull(serializer);
            EscapeHandler.handleEscape(dorisExecutionOptions.getStreamLoadProp());
            if(dorisReadOptions == null) {
                dorisReadOptions = DorisReadOptions.builder().build();
            }
            return new DorisSink<>(dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        }
    }
}
