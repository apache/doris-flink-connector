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

package org.apache.doris.flink.flight;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.doris.flink.flight.arrow.DorisFlightRecordEmitter;
import org.apache.doris.flink.flight.arrow.DorisFlightSourceReader;
import org.apache.doris.flink.flight.assigners.FlightSplitAssigner;
import org.apache.doris.flink.flight.enumerator.DorisFlightSourceEnumerator;
import org.apache.doris.flink.flight.enumerator.PendingFlightSplitsCheckpointSerializer;
import org.apache.doris.flink.flight.split.DorisFlightSourceSplit;
import org.apache.doris.flink.flight.split.DorisFlightSourceSplitSerializer;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.source.enumerator.PendingSplitsCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** DorisSource based on FLIP-27 which is a BOUNDED stream. */
public class DorisFlightSource<OUT>
        implements Source<OUT, DorisFlightSourceSplit, PendingSplitsCheckpoint>,
                ResultTypeQueryable<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisFlightSource.class);

    private final DorisOptions options;
    private final DorisReadOptions readOptions;

    // Boundedness
    private final Boundedness boundedness;
    private final DorisDeserializationSchema<OUT> deserializer;

    public DorisFlightSource(
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
    public SourceReader<OUT, DorisFlightSourceSplit> createReader(
            SourceReaderContext readerContext) {
        return new DorisFlightSourceReader<>(
                options,
                readOptions,
                new DorisFlightRecordEmitter<>(deserializer),
                readerContext,
                readerContext.getConfiguration());
    }

    @Override
    public SplitEnumerator<DorisFlightSourceSplit, PendingSplitsCheckpoint> createEnumerator(
            SplitEnumeratorContext<DorisFlightSourceSplit> context) throws Exception {
        List<DorisFlightSourceSplit> dorisSourceSplits = new ArrayList<>();
        List<PartitionDefinition> partitions =
                RestService.findPartitions(options, readOptions, LOG);
        partitions.forEach(
                m -> {
                    List<List<Long>> tablets =
                            Lists.partition(new ArrayList<>(m.getTabletIds()), 2);
                    for (int i = 0; i < tablets.size(); i++) {
                        PartitionDefinition partitionDef =
                                new PartitionDefinition(
                                        m.getDatabase(),
                                        m.getTable(),
                                        m.getBeAddress(),
                                        Sets.newHashSet(tablets.get(i)),
                                        m.getQueryPlan());
                        String splitId = m.getBeAddress() + "_" + i;
                        dorisSourceSplits.add(new DorisFlightSourceSplit(splitId, partitionDef));
                    }
                });
        /*
            if your want to split by partition without tablet, you can use the following code:
            for (int index = 0; index < partitions.size(); index++) {
            PartitionDefinition partitionDef = partitions.get(index);
            String splitId = partitionDef.getBeAddress() + "_" + index;
            dorisSourceSplits.add(new DorisFlightSourceSplit(splitId, partitionDef));
        }*/
        FlightSplitAssigner splitAssigner = new FlightSplitAssigner(dorisSourceSplits);
        return new DorisFlightSourceEnumerator(context, splitAssigner);
    }

    @Override
    public SplitEnumerator<DorisFlightSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<DorisFlightSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        Collection<DorisFlightSourceSplit> splits = checkpoint.getSplits();
        FlightSplitAssigner splitAssigner = new FlightSplitAssigner(splits);
        return new DorisFlightSourceEnumerator(context, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<DorisFlightSourceSplit> getSplitSerializer() {
        return DorisFlightSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
        return new PendingFlightSplitsCheckpointSerializer(getSplitSerializer());
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
        private Long limit = -1L;

        DorisSourceBuilder() {
            boundedness = Boundedness.BOUNDED;
        }

        public DorisSourceBuilder<OUT> setDorisOptions(DorisOptions options) {
            this.options = options;
            return this;
        }

        public DorisSourceBuilder<OUT> setDorisReadOptions(DorisReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public DorisSourceBuilder<OUT> setBoundedness(Boundedness boundedness) {
            this.boundedness = boundedness;
            return this;
        }

        public DorisSourceBuilder<OUT> setDeserializer(
                DorisDeserializationSchema<OUT> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public DorisSourceBuilder<OUT> setLimit(Long limit) {
            this.limit = limit;
            return this;
        }

        public DorisFlightSource<OUT> build() {
            if (readOptions == null) {
                readOptions = DorisReadOptions.builder().build();
            }
            return new DorisFlightSource<>(options, readOptions, boundedness, deserializer);
        }
    }
}
