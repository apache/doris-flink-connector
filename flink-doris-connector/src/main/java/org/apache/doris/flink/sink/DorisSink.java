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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.sink.committer.DorisCommitter;
import org.apache.doris.flink.sink.writer.DorisWriter;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.DorisWriterStateSerializer;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Load data into Doris based on 2PC. see {@link DorisWriter} and {@link DorisCommitter}.
 *
 * @param <IN> type of record.
 */
public class DorisSink<IN>
        implements StatefulSink<IN, DorisWriterState>,
                TwoPhaseCommittingSink<IN, DorisCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSink.class);
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions dorisExecutionOptions;
    private final DorisRecordSerializer<IN> serializer;

    public DorisSink(
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions dorisExecutionOptions,
            DorisRecordSerializer<IN> serializer) {
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.dorisExecutionOptions = dorisExecutionOptions;
        this.serializer = serializer;
        checkKeyType();
    }

    /** The uniq model has 2pc close by default unless 2pc is forced open. */
    private void checkKeyType() {
        if (dorisExecutionOptions.enabled2PC()
                && !dorisExecutionOptions.force2PC()
                && RestService.isUniqueKeyType(dorisOptions, dorisReadOptions, LOG)) {
            dorisExecutionOptions.setEnable2PC(false);
        }
    }

    @Override
    public DorisWriter<IN> createWriter(InitContext initContext) throws IOException {
        DorisWriter<IN> dorisWriter =
                new DorisWriter<>(
                        initContext,
                        Collections.emptyList(),
                        serializer,
                        dorisOptions,
                        dorisReadOptions,
                        dorisExecutionOptions);
        return dorisWriter;
    }

    @Override
    public Committer<DorisCommittable> createCommitter() throws IOException {
        return new DorisCommitter(
                dorisOptions, dorisReadOptions, dorisExecutionOptions.getMaxRetries());
    }

    @Override
    public DorisWriter<IN> restoreWriter(
            InitContext initContext, Collection<DorisWriterState> recoveredState)
            throws IOException {
        DorisWriter<IN> dorisWriter =
                new DorisWriter<>(
                        initContext,
                        recoveredState,
                        serializer,
                        dorisOptions,
                        dorisReadOptions,
                        dorisExecutionOptions);
        return dorisWriter;
    }

    @Override
    public SimpleVersionedSerializer<DorisWriterState> getWriterStateSerializer() {
        return new DorisWriterStateSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DorisCommittable> getCommittableSerializer() {
        return new DorisCommittableSerializer();
    }

    public static <IN> Builder<IN> builder() {
        return new Builder<>();
    }

    /**
     * build for DorisSink.
     *
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
            if (dorisReadOptions == null) {
                dorisReadOptions = DorisReadOptions.builder().build();
            }
            return new DorisSink<>(
                    dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        }
    }
}
