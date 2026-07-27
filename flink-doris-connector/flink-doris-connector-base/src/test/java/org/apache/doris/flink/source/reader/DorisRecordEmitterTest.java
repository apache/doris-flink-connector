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

package org.apache.doris.flink.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DorisRecordEmitterTest {

    @Test
    void passesCompleteSourceRecordToDeserializationSchema() throws Exception {
        DorisRecordEmitter<DorisSourceRecord> emitter =
                new DorisRecordEmitter<>(new SourceRecordDeserializer());
        SourceOutput<DorisSourceRecord> output = mock(SourceOutput.class);
        DorisSourceRecord record =
                DorisSourceRecord.incremental(Collections.singletonList(7), 101L, 1001L, 1L);

        emitter.emitRecord(record, output, null);

        verify(output).collect(record);
    }

    @Test
    void emitsStandardMutableListForFlinkSerialization() throws Exception {
        DorisRecordEmitter<List<?>> emitter =
                new DorisRecordEmitter<>(new SimpleListDeserializationSchema());
        SourceOutput<List<?>> output = mock(SourceOutput.class);
        DorisSourceRecord record = DorisSourceRecord.snapshot(Collections.singletonList(7));

        emitter.emitRecord(record, output, null);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(output).collect(captor.capture());
        assertThat(captor.getValue()).isInstanceOf(ArrayList.class).containsExactly(7);
    }

    @Test
    void ownsMutableFieldValues() {
        List<Integer> fieldValues = new ArrayList<>(Collections.singletonList(7));
        DorisSourceRecord record = DorisSourceRecord.snapshot(fieldValues);

        fieldValues.set(0, 8);
        assertThat(record.getFieldValues()).isEqualTo(Collections.singletonList(7));

        record.getFieldValues().clear();
        assertThat(record.getFieldValues()).isEmpty();
    }

    @Test
    void mapsAllBinlogOperationsAndRejectsUnknownValues() {
        assertThat(record(0L).getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(record(1L).getRowKind()).isEqualTo(RowKind.DELETE);
        assertThat(record(2L).getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);
        assertThat(record(3L).getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
        assertThatThrownBy(() -> record(4L)).hasMessageContaining("Unsupported");
        assertThatThrownBy(() -> record(4_294_967_296L)).hasMessageContaining("Unsupported");
        assertThatThrownBy(
                        () ->
                                DorisSourceRecord.incremental(
                                        Collections.singletonList(7), null, 1001L, 0L))
                .hasMessageContaining("cannot be null");
    }

    private static DorisSourceRecord record(long op) {
        return DorisSourceRecord.incremental(Collections.singletonList(7), 101L, 1001L, op);
    }

    private static final class SourceRecordDeserializer
            implements DorisDeserializationSchema<DorisSourceRecord> {
        @Override
        public void deserialize(List<?> record, Collector<DorisSourceRecord> out) {
            throw new AssertionError("The complete Doris source record should be deserialized");
        }

        @Override
        public void deserialize(DorisSourceRecord record, Collector<DorisSourceRecord> out) {
            out.collect(record);
        }

        @Override
        public TypeInformation<DorisSourceRecord> getProducedType() {
            return TypeInformation.of(DorisSourceRecord.class);
        }
    }
}
