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

package org.apache.doris.flink.source.reader;

import org.apache.doris.flink.connection.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DorisOffsetPublisherTest {
    private final JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
    private final Connection connection = mock(Connection.class);
    private final PreparedStatement statement = mock(PreparedStatement.class);

    @BeforeEach
    void setUp() throws Exception {
        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);
    }

    @Test
    void writesCompletedOffsetWithPreparedStatement() throws Exception {
        DorisOffsetPublisher publisher =
                new DorisOffsetPublisher(
                        connectionProvider, "ops.flink_source_offsets", "prod.sales.orders");

        publisher.publish("2026-07-20 10:00:10", error -> assertThat(error).isNull());

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
    void reportsEachFailureWithoutThrowing() throws Exception {
        when(statement.executeUpdate()).thenThrow(new java.sql.SQLException("unavailable"));
        DorisOffsetPublisher publisher =
                new DorisOffsetPublisher(
                        connectionProvider, "ops.flink_source_offsets", "prod.sales.orders");

        assertThatCode(
                        () -> {
                            AtomicInteger failures = new AtomicInteger();
                            publisher.publish(
                                    "2026-07-20 10:00:10",
                                    error -> {
                                        assertThat(error).isInstanceOf(java.sql.SQLException.class);
                                        failures.incrementAndGet();
                                    });
                            publisher.publish(
                                    "2026-07-20 10:00:10",
                                    error -> {
                                        assertThat(error).isInstanceOf(java.sql.SQLException.class);
                                        failures.incrementAndGet();
                                    });
                            assertThat(failures.get()).isEqualTo(2);
                        })
                .doesNotThrowAnyException();
        verify(statement, times(2)).executeUpdate();
    }
}
