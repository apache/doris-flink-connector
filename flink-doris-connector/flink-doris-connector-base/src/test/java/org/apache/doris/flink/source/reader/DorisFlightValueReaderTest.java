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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.memory.RootAllocator;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.source.DorisBinlogIncrementType;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DorisFlightValueReaderTest {

    @Test
    void closesAcquiredResourcesWhenInitializationFails() throws Exception {
        DorisSnapshotSplit split = mock(DorisSnapshotSplit.class);
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("sales.orders")
                        .build();
        DorisReadOptions readOptions = DorisReadOptions.builder().setFlightSqlPort(8815).build();
        AdbcDatabase database = mock(AdbcDatabase.class);
        AdbcConnection connection = mock(AdbcConnection.class);
        when(database.connect()).thenReturn(connection);
        when(connection.createStatement()).thenThrow(new RuntimeException("create failed"));

        try (MockedStatic<RestService> restService = mockStatic(RestService.class);
                MockedConstruction<RootAllocator> allocators =
                        mockConstruction(RootAllocator.class);
                MockedConstruction<FlightSqlDriver> drivers =
                        mockConstruction(
                                FlightSqlDriver.class,
                                (driver, context) ->
                                        when(driver.open(anyMap())).thenReturn(database))) {
            restService
                    .when(() -> RestService.getSchema(eq(options), eq(readOptions), any()))
                    .thenReturn(new Schema());
            restService
                    .when(() -> RestService.randomEndpoint(eq(options.getFenodes()), any()))
                    .thenReturn("127.0.0.1:8030");

            assertThatThrownBy(() -> new DorisFlightValueReader(split, options, readOptions))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("create failed");

            verify(connection).close();
            verify(database).close();
            verify(allocators.constructed().get(0)).close();
        }
    }

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
