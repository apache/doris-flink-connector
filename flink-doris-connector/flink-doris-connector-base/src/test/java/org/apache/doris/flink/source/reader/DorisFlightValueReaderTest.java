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

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.source.DorisBinlogIncrementType;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DorisFlightValueReaderTest {

    @Test
    void closesRemainingResourcesAfterOneCloseFails() throws Exception {
        AutoCloseable first = mock(AutoCloseable.class);
        AutoCloseable second = mock(AutoCloseable.class);
        doThrow(new java.io.IOException("first failed")).when(first).close();

        assertThatThrownBy(() -> DorisFlightValueReader.closeAll(first, second))
                .hasMessageContaining("first failed");
        verify(second).close();
    }

    @Test
    void buildsSnapshotSqlWithTabletRestriction() {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("sales.orders")
                        .build();
        DorisReadOptions readOptions = DorisReadOptions.builder().setReadFields("`id`").build();
        PartitionDefinition partition =
                new PartitionDefinition(
                        "sales",
                        "orders",
                        "be:9060",
                        new LinkedHashSet<>(Arrays.asList(2L, 1L)),
                        "plan");

        assertThat(DorisFlightValueReader.buildSnapshotSql(options, readOptions, partition))
                .contains("SELECT `id` FROM `sales`.`orders`")
                .contains("TABLET(1,2)");
    }

    @Test
    void buildsExplicitOrderedIncrementalSql() {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("sales.orders")
                        .build();
        DorisReadOptions readOptions = DorisReadOptions.builder().setReadFields("`id`").build();
        DorisStreamSplit split = DorisStreamSplit.of("2026-07-20 10:00:00", "2026-07-20 10:00:10");

        String sql =
                DorisFlightValueReader.buildIncrementalSql(
                        options, readOptions, split, DorisBinlogIncrementType.DETAIL);

        assertThat(sql)
                .contains("`sales`.`orders`@incr")
                .contains("'startTimestamp' = '2026-07-20 10:00:00'")
                .contains("'endTimestamp' = '2026-07-20 10:00:10'")
                .contains("'incrementType' = 'DETAIL'")
                .endsWith(
                        "ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__, __DORIS_BINLOG_OP__");
    }
}
