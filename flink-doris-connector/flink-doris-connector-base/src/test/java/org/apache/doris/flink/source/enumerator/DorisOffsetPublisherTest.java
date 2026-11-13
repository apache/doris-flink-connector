// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.doris.flink.source.enumerator;

import org.apache.doris.flink.connection.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DorisOffsetPublisherTest {
    private final JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
    private final Connection connection = mock(Connection.class);
    private final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    private final ResultSet tables = mock(ResultSet.class);
    private final PreparedStatement statement = mock(PreparedStatement.class);

    @BeforeEach
    void setUp() throws Exception {
        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getTables("ops", null, "flink_source_offsets", null)).thenReturn(tables);
        when(connection.prepareStatement(any())).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);
    }

    @Test
    void validatesExistingOffsetTable() throws Exception {
        when(tables.next()).thenReturn(true);
        when(tables.getString("TABLE_NAME")).thenReturn("flink_source_offsets");
        DorisOffsetPublisher publisher =
                new DorisOffsetPublisher(
                        connectionProvider, "ops.flink_source_offsets", "prod.sales.orders");

        assertThatCode(publisher::validateOffsetTable).doesNotThrowAnyException();
    }

    @Test
    void rejectsWildcardTableNameMatch() throws Exception {
        when(tables.next()).thenReturn(true, false);
        when(tables.getString("TABLE_NAME")).thenReturn("flinkXsourceXoffsets");
        DorisOffsetPublisher publisher =
                new DorisOffsetPublisher(
                        connectionProvider, "ops.flink_source_offsets", "prod.sales.orders");

        assertThatThrownBy(publisher::validateOffsetTable)
                .hasMessageContaining("Offset table does not exist: ops.flink_source_offsets");
    }

    @Test
    void failsValidationWhenJdbcDriverIsMissing() throws Exception {
        when(connectionProvider.getOrEstablishConnection())
                .thenThrow(new ClassNotFoundException("com.mysql.cj.jdbc.Driver"));
        DorisOffsetPublisher publisher =
                new DorisOffsetPublisher(
                        connectionProvider, "ops.flink_source_offsets", "prod.sales.orders");

        assertThatThrownBy(publisher::validateOffsetTable)
                .hasMessageContaining("Failed to validate offset table: ops.flink_source_offsets")
                .hasCauseInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void writesCompletedOffsetWithPreparedStatement() throws Exception {
        DorisOffsetPublisher publisher =
                new DorisOffsetPublisher(
                        connectionProvider, "ops.flink_source_offsets", "prod.sales.orders");

        publisher.publish("2026-07-20 10:00:10");

        verify(connection)
                .prepareStatement(
                        "INSERT INTO `ops`.`flink_source_offsets` "
                                + "(`consumer_id`, `offset_timestamp`, `update_time`) "
                                + "VALUES (?, ?, CURRENT_TIMESTAMP(3))");
        verify(statement).setString(1, "prod.sales.orders");
        verify(statement).setString(2, "2026-07-20 10:00:10");
        verify(statement).executeUpdate();
    }

    @Test
    void propagatesWriteFailureSoTheEnumeratorCanRetry() throws Exception {
        when(statement.executeUpdate())
                .thenThrow(new java.sql.SQLException("unavailable"))
                .thenReturn(1);
        DorisOffsetPublisher publisher =
                new DorisOffsetPublisher(
                        connectionProvider, "ops.flink_source_offsets", "prod.sales.orders");

        assertThatThrownBy(() -> publisher.publish("2026-07-20 10:00:10"))
                .isInstanceOf(java.sql.SQLException.class);
        publisher.publish("2026-07-20 10:00:10");
        verify(statement, times(2)).executeUpdate();
    }
}
