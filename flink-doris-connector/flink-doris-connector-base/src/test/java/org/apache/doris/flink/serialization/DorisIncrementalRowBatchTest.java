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

package org.apache.doris.flink.serialization;

import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.source.reader.DorisSourceRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DorisIncrementalRowBatchTest {

    @Test
    void rejectsIncrementalBatchWithMissingNamedHiddenField() throws Exception {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                BigIntVector lsn = new BigIntVector("__DORIS_BINLOG_LSN__", allocator);
                IntVector id = new IntVector("id", allocator);
                BigIntVector op = new BigIntVector("__DORIS_BINLOG_OP__", allocator);
                VarCharVector name = new VarCharVector("name", allocator);
                BigIntVector wrongTso = new BigIntVector("not_the_tso", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(lsn, id, op, name, wrongTso)) {
            root.setRowCount(1);
            ArrowReader reader = mock(ArrowReader.class);
            when(reader.getVectorSchemaRoot()).thenReturn(root);
            Schema schema = new Schema();
            schema.put("id", "INT", "", 0, 0, "");
            schema.put("name", "VARCHAR", "", 0, 0, "");

            assertThatThrownBy(() -> new RowBatch(reader, schema, true).readFlightArrow())
                    .hasMessageContaining("exactly one TSO, LSN, and OP");
        }
    }

    @Test
    void propagatesArrowReaderIOException() throws Exception {
        ArrowReader reader = mock(ArrowReader.class);
        when(reader.getVectorSchemaRoot()).thenThrow(new java.io.IOException("broken batch"));

        assertThatThrownBy(() -> new RowBatch(reader, new Schema(), true).readFlightArrow())
                .hasMessageContaining("Failed to read Doris Flight Arrow batch")
                .hasRootCauseMessage("broken batch");
    }

    @Test
    void extractsHiddenColumnsByNameInsteadOfPosition() throws Exception {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                BigIntVector lsn = new BigIntVector("__DORIS_BINLOG_LSN__", allocator);
                IntVector id = new IntVector("id", allocator);
                BigIntVector op = new BigIntVector("__DORIS_BINLOG_OP__", allocator);
                VarCharVector name = new VarCharVector("name", allocator);
                BigIntVector tso = new BigIntVector("__DORIS_BINLOG_TSO__", allocator);
                VectorSchemaRoot root = VectorSchemaRoot.of(lsn, id, op, name, tso)) {
            lsn.setSafe(0, 1001L);
            id.setSafe(0, 7);
            op.setSafe(0, 2L);
            name.setSafe(0, "before".getBytes(StandardCharsets.UTF_8));
            tso.setSafe(0, 101L);
            root.setRowCount(1);

            ArrowReader reader = mock(ArrowReader.class);
            when(reader.getVectorSchemaRoot()).thenReturn(root);
            Schema schema = new Schema();
            schema.put("id", "INT", "", 0, 0, "");
            schema.put("name", "VARCHAR", "", 0, 0, "");

            DorisSourceRecord record =
                    new RowBatch(reader, schema, true).readFlightArrow().nextSourceRecord();

            assertThat(record.getFieldValues()).isEqualTo(Arrays.asList(7, "before"));
            assertThat(record.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);
            assertThat(record.getBinlogTso()).isEqualTo(101L);
            assertThat(record.getBinlogLsn()).isEqualTo(1001L);
            assertThat(record.getBinlogOp()).isEqualTo(2L);
        }
    }
}
