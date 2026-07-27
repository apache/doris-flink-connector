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

package org.apache.doris.flink.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.doris.flink.source.assigners.DorisSourceSplitAssigner;
import org.apache.doris.flink.source.enumerator.DorisSourceCheckpoint;
import org.apache.doris.flink.source.enumerator.DorisSourceCheckpointSerializer;
import org.apache.doris.flink.source.enumerator.DorisSourceEnumerator;
import org.apache.doris.flink.source.reader.DorisRecordEmitter;
import org.apache.doris.flink.source.reader.DorisSourceReader;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisSourceSplitSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** FLIP-27 source for bounded snapshots and continuous Doris row-binlog increments. */
@PublicEvolving
public class DorisSource<OUT>
        implements Source<OUT, DorisSourceSplit, DorisSourceCheckpoint>, ResultTypeQueryable<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSource.class);
    private static final long MIN_BINLOG_POLL_INTERVAL_MS = 1_000L;

    private final DorisOptions options;
    private final DorisReadOptions readOptions;

    // Boundedness
    private final Boundedness boundedness;
    private final DorisDeserializationSchema<OUT> deserializer;

    public DorisSource(
            DorisOptions options,
            DorisReadOptions readOptions,
            Boundedness boundedness,
            DorisDeserializationSchema<OUT> deserializer) {
        this.options = options;
        this.readOptions = readOptions;
        this.boundedness = boundedness;
        this.deserializer = deserializer;
    }

    @Override
    public Boundedness getBoundedness() {
        return readOptions.getScanMode().hasIncrementalPhase()
                ? Boundedness.CONTINUOUS_UNBOUNDED
                : this.boundedness;
    }

    @Override
    public SourceReader<OUT, DorisSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new DorisSourceReader<>(
                options,
                readOptions,
                new DorisRecordEmitter<>(deserializer),
                readerContext,
                readerContext.getConfiguration());
    }

    @Override
    public SplitEnumerator<DorisSourceSplit, DorisSourceCheckpoint> createEnumerator(
            SplitEnumeratorContext<DorisSourceSplit> context) throws Exception {
        return new DorisSourceEnumerator(
                context,
                DorisSourceSplitAssigner.create(options, readOptions, context.currentParallelism()),
                readOptions.getBinlogPollIntervalMs());
    }

    @Override
    public SplitEnumerator<DorisSourceSplit, DorisSourceCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<DorisSourceSplit> context, DorisSourceCheckpoint checkpoint)
            throws Exception {
        LOG.info("Restore Doris source checkpoint in phase {}", checkpoint.getPhase());
        return new DorisSourceEnumerator(
                context,
                DorisSourceSplitAssigner.restore(
                        options, readOptions, checkpoint, context.currentParallelism()),
                readOptions.getBinlogPollIntervalMs());
    }

    @Override
    public SimpleVersionedSerializer<DorisSourceSplit> getSplitSerializer() {
        return DorisSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<DorisSourceCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DorisSourceCheckpointSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializer.getProducedType();
    }

    public static <OUT> DorisSourceBuilder<OUT> builder() {
        return new DorisSourceBuilder();
    }

    /**
     * build for DorisSource.
     *
     * @param <OUT> record type.
     */
    public static class DorisSourceBuilder<OUT> {

        private DorisOptions options;
        private DorisReadOptions readOptions;

        // Boundedness
        private Boundedness boundedness;
        private DorisDeserializationSchema<OUT> deserializer;

        DorisSourceBuilder() {
            boundedness = Boundedness.BOUNDED;
        }

        /**
         * Sets the DorisOptions for the DorisSource.
         *
         * @param options the common options of the doris cluster.
         * @return this DorisSourceBuilder.
         */
        public DorisSourceBuilder<OUT> setDorisOptions(DorisOptions options) {
            this.options = options;
            return this;
        }

        /**
         * Sets the DorisReadOptions for the DorisSource.
         *
         * @param readOptions the read options of the DorisSource.
         * @return this DorisSourceBuilder.
         */
        public DorisSourceBuilder<OUT> setDorisReadOptions(DorisReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        /** Sets the boundedness for snapshot mode. Incremental modes are always unbounded. */
        public DorisSourceBuilder<OUT> setBoundedness(Boundedness boundedness) {
            this.boundedness = boundedness;
            return this;
        }

        /**
         * Sets the {@link DorisDeserializationSchema deserializer} of the Record for DorisSource.
         *
         * @param deserializer the deserializer for Doris Record.
         * @return this DorisSourceBuilder.
         */
        public DorisSourceBuilder<OUT> setDeserializer(
                DorisDeserializationSchema<OUT> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        /**
         * Build the {@link DorisSource}.
         *
         * @return a DorisSource with the settings made for this builder.
         */
        public DorisSource<OUT> build() {
            if (readOptions == null) {
                readOptions = DorisReadOptions.builder().build();
            }
            Preconditions.checkNotNull(options, "Doris options must be configured");
            Preconditions.checkNotNull(deserializer, "Doris deserializer must be configured");
            Preconditions.checkArgument(
                    !readOptions.getUseOldApi()
                            || readOptions.getScanMode() == DorisSourceScanMode.SNAPSHOT,
                    "source.use-old-api=true only supports source.scan.mode=snapshot");
            if (readOptions.getScanMode() == DorisSourceScanMode.FROM_TIMESTAMP) {
                Preconditions.checkArgument(
                        org.apache.doris.flink.source.split.DorisStreamSplit.isValidTimestamp(
                                readOptions.getScanTimestamp()),
                        "source.scan.timestamp must match yyyy-MM-dd HH:mm:ss");
            } else {
                Preconditions.checkArgument(
                        readOptions.getScanTimestamp() == null,
                        "source.scan.timestamp is only valid in from-timestamp mode");
            }
            if (readOptions.getScanMode().hasIncrementalPhase()) {
                String tableIdentifier = options.getTableIdentifier();
                Preconditions.checkArgument(
                        tableIdentifier != null && tableIdentifier.split("\\.", -1).length == 2,
                        "incremental source modes only support table.identifier=db.table");
                Preconditions.checkArgument(
                        readOptions.getFilterQuery() == null && readOptions.getRowLimit() == null,
                        "incremental source modes do not support filter or limit pushdown");
                Preconditions.checkArgument(
                        readOptions.getUseFlightSql(),
                        "incremental source modes require source.use-flight-sql=true");
            }
            Preconditions.checkArgument(
                    readOptions.getBinlogPollIntervalMs() >= MIN_BINLOG_POLL_INTERVAL_MS,
                    "source.binlog.poll-interval must be at least 1 second");
            return new DorisSource<>(options, readOptions, boundedness, deserializer);
        }
    }
}
