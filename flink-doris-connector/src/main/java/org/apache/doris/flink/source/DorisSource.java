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
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.source.assigners.DorisSplitAssigner;
import org.apache.doris.flink.source.assigners.SimpleSplitAssigner;
import org.apache.doris.flink.source.enumerator.DorisSourceEnumerator;
import org.apache.doris.flink.source.enumerator.PendingSplitsCheckpoint;
import org.apache.doris.flink.source.enumerator.PendingSplitsCheckpointSerializer;
import org.apache.doris.flink.source.reader.DorisRecordEmitter;
import org.apache.doris.flink.source.reader.DorisSourceReader;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisSourceSplitSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** DorisSource based on FLIP-27 which is a BOUNDED stream. */
@PublicEvolving
public class DorisSource<OUT>
        implements Source<OUT, DorisSourceSplit, PendingSplitsCheckpoint>,
                ResultTypeQueryable<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSource.class);
    private static final String SINGLE_SPLIT = "SingleSplit";

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
        return this.boundedness;
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
    public SplitEnumerator<DorisSourceSplit, PendingSplitsCheckpoint> createEnumerator(
            SplitEnumeratorContext<DorisSourceSplit> context) throws Exception {
        List<DorisSourceSplit> dorisSourceSplits = new ArrayList<>();
        String[] tableIdentifiers = RestService.parseIdentifier(options.getTableIdentifier(), LOG);

        if (tableIdentifiers.length == 2) {
            List<PartitionDefinition> partitions =
                    RestService.findPartitions(options, readOptions, LOG);
            for (int index = 0; index < partitions.size(); index++) {
                PartitionDefinition partitionDef = partitions.get(index);
                String splitId = partitionDef.getBeAddress() + "_" + index;
                dorisSourceSplits.add(new DorisSourceSplit(splitId, partitionDef));
            }
        } else {
            Preconditions.checkArgument(
                    readOptions.getUseFlightSql(),
                    "UseFlightSql must be true when table.identifier is catalog.db.table");
            // catalog query or customer query
            dorisSourceSplits.add(
                    new DorisSourceSplit(
                            SINGLE_SPLIT,
                            PartitionDefinition.emptyPartition(options.getTableIdentifier())));
        }

        DorisSplitAssigner splitAssigner = new SimpleSplitAssigner(dorisSourceSplits);
        return new DorisSourceEnumerator(context, splitAssigner);
    }

    @Override
    public SplitEnumerator<DorisSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<DorisSourceSplit> context, PendingSplitsCheckpoint checkpoint)
            throws Exception {
        Collection<DorisSourceSplit> splits = checkpoint.getSplits();
        LOG.info("Restore splits from checkpoint, size {}, splits {}", splits.size(), splits);
        DorisSplitAssigner splitAssigner = new SimpleSplitAssigner(splits);
        return new DorisSourceEnumerator(context, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<DorisSourceSplit> getSplitSerializer() {
        return DorisSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
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

        /** Sets the Boundedness for the DorisSource, Currently only BOUNDED is supported. */
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
            return new DorisSource<>(options, readOptions, boundedness, deserializer);
        }
    }
}
