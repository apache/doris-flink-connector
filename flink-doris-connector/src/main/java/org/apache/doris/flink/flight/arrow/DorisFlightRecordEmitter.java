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

package org.apache.doris.flink.flight.arrow;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.doris.flink.flight.split.DorisFlightSourceSplitState;
import org.apache.doris.flink.source.reader.DorisSourceReader;

import java.util.List;

/** The {@link RecordEmitter} implementation for {@link DorisSourceReader}. */
public class DorisFlightRecordEmitter<T>
        implements RecordEmitter<List, T, DorisFlightSourceSplitState> {

    private final DorisDeserializationSchema<T> dorisDeserializationSchema;
    private final OutputCollector<T> outputCollector;

    public DorisFlightRecordEmitter(DorisDeserializationSchema<T> dorisDeserializationSchema) {
        this.dorisDeserializationSchema = dorisDeserializationSchema;
        this.outputCollector = new OutputCollector<>();
    }

    @Override
    public void emitRecord(
            List value, SourceOutput<T> output, DorisFlightSourceSplitState splitState)
            throws Exception {
        outputCollector.output = output;
        dorisDeserializationSchema.deserialize(value, outputCollector);
    }

    private static class OutputCollector<T> implements Collector<T> {
        private SourceOutput<T> output;

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
