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

package org.apache.doris.flink.source.assigners;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.connection.SimpleJdbcConnectionProvider;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.source.DorisSourceScanMode;
import org.apache.doris.flink.source.enumerator.DorisSourceCheckpoint;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DorisSourceOffsetRestoreTest {
    private static final String SAVED = "2026-10-09 12:00:00";
    private static final String CONFIGURED = "2026-10-09 10:00:00";
    private static final String NOW = "2026-10-09 13:00:00";
    private final DorisOptions options =
            DorisOptions.builder()
                    .setFenodes("localhost:8030")
                    .setTableIdentifier("db.t")
                    .setJdbcUrl("jdbc:mysql://localhost:9030")
                    .build();
    private final Connection connection = mock(Connection.class);
    private final PreparedStatement statement = mock(PreparedStatement.class);
    private final ResultSet result = mock(ResultSet.class);

    @BeforeEach
    void setUp() throws Exception {
        when(connection.prepareStatement(any())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(result);
        when(result.next()).thenReturn(true, false);
        when(result.getString("offset_timestamp")).thenReturn(SAVED);
    }

    @ParameterizedTest
    @EnumSource(
            value = DorisSourceScanMode.class,
            names = {"INITIAL", "LATEST", "FROM_TIMESTAMP"})
    void resumesEveryIncrementalModeFromSavedBoundary(DorisSourceScanMode mode) throws Exception {
        DorisReadOptions readOptions = readOptions(mode);
        try (MockedConstruction<SimpleJdbcConnectionProvider> providers = providers();
                MockedStatic<RestService> rest = rest(readOptions)) {
            DorisSourceSplitAssigner assigner =
                    DorisSourceSplitAssigner.create(options, readOptions, 2);
            assertThat(assigner.getPhase()).isEqualTo(DorisSourceCheckpoint.Phase.STREAM);
            assertThat(assigner.hasPendingSnapshotSplits()).isFalse();
            assertThat(assigner.getScanMode()).isEqualTo(mode);
            assertThat(assigner.snapshotState(0L).getNextStreamStartTimestamp()).isEqualTo(SAVED);
            assigner.onDiscoveredEndTimestamp(assigner.discoverNextStreamEndTimestamp());
            DorisStreamSplit window = (DorisStreamSplit) assigner.getNext().get();
            assertThat(window.getStartTimestamp()).isEqualTo(SAVED);
            assertThat(window.getEndTimestamp()).isEqualTo(NOW);
            rest.verify(
                    () -> RestService.findPartitions(eq(options), eq(readOptions), any()), never());
            verify(connection)
                    .prepareStatement(
                            "SELECT `offset_timestamp` FROM `ops`.`offsets` WHERE `consumer_id` = ?");
            verify(statement).setString(1, "orders-consumer");
            verify(result).close();
            verify(statement).close();
            verify(providers.constructed().get(0)).closeConnection();
        }
    }

    @Test
    void checkpointRestorationDoesNotOpenOffsetConnection() {
        DorisReadOptions readOptions = readOptions(DorisSourceScanMode.INITIAL);
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM, SAVED, Collections.emptyList());
        try (MockedConstruction<SimpleJdbcConnectionProvider> providers = providers()) {
            DorisSourceSplitAssigner assigner =
                    DorisSourceSplitAssigner.restore(options, readOptions, checkpoint, 2);
            assertThat(assigner.snapshotState(0L).getNextStreamStartTimestamp()).isEqualTo(SAVED);
            assertThat(assigner.getPhase()).isEqualTo(DorisSourceCheckpoint.Phase.STREAM);
            assertThat(providers.constructed()).isEmpty();
        }
    }

    @Test
    void initialWithoutRecordPlansFirstSnapshot() throws Exception {
        when(result.next()).thenReturn(false);
        DorisReadOptions readOptions = readOptions(DorisSourceScanMode.INITIAL);
        try (MockedConstruction<SimpleJdbcConnectionProvider> ignored = providers();
                MockedStatic<RestService> rest = rest(readOptions)) {
            DorisSourceSplitAssigner assigner =
                    DorisSourceSplitAssigner.create(options, readOptions, 2);
            assertThat(assigner.getPhase()).isEqualTo(DorisSourceCheckpoint.Phase.SNAPSHOT);
            assertThat(assigner.snapshotState(0L).getNextStreamStartTimestamp()).isEqualTo(NOW);
            rest.verify(() -> RestService.findPartitions(eq(options), eq(readOptions), any()));
        }
    }

    @Test
    void fromTimestampWithoutRecordUsesConfiguredStart() throws Exception {
        when(result.next()).thenReturn(false);
        DorisReadOptions readOptions = readOptions(DorisSourceScanMode.FROM_TIMESTAMP);
        try (MockedConstruction<SimpleJdbcConnectionProvider> ignored = providers()) {
            assertThat(
                            DorisSourceSplitAssigner.create(options, readOptions, 2)
                                    .snapshotState(0L)
                                    .getNextStreamStartTimestamp())
                    .isEqualTo(CONFIGURED);
        }
    }

    @Test
    void latestWithoutRecordStartsFromCurrentTime() throws Exception {
        when(result.next()).thenReturn(false);
        DorisReadOptions readOptions = readOptions(DorisSourceScanMode.LATEST);
        try (MockedConstruction<SimpleJdbcConnectionProvider> ignored = providers();
                MockedStatic<RestService> rest = rest(readOptions)) {
            DorisSourceSplitAssigner assigner =
                    DorisSourceSplitAssigner.create(options, readOptions, 2);
            assertThat(assigner.getPhase()).isEqualTo(DorisSourceCheckpoint.Phase.STREAM);
            assertThat(assigner.hasPendingSnapshotSplits()).isFalse();
            assertThat(assigner.snapshotState(0L).getNextStreamStartTimestamp()).isEqualTo(NOW);
            rest.verify(
                    () -> RestService.findPartitions(eq(options), eq(readOptions), any()), never());
        }
    }

    @Test
    void latestWithoutOffsetTableKeepsCurrentTimeStart() {
        DorisReadOptions readOptions =
                DorisReadOptions.builder().setScanMode(DorisSourceScanMode.LATEST).build();
        try (MockedConstruction<SimpleJdbcConnectionProvider> providers = providers();
                MockedStatic<RestService> ignored = rest(readOptions)) {
            assertThat(
                            DorisSourceSplitAssigner.create(options, readOptions, 2)
                                    .snapshotState(0L)
                                    .getNextStreamStartTimestamp())
                    .isEqualTo(NOW);
            assertThat(providers.constructed()).isEmpty();
        }
    }

    @Test
    void duplicateOffsetsFailInsteadOfChoosingOne() throws Exception {
        when(result.next()).thenReturn(true, true);
        assertStartupFails(DorisSourceScanMode.INITIAL, "Multiple offsets");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                "2026-02-30 12:00:00",
                "2026-10-09 25:00:00",
                "2026-10-09 12:00:00.123",
                "not-a-timestamp"
            })
    void invalidSavedOffsetsFailInsteadOfStartingSnapshot(String value) throws Exception {
        when(result.getString("offset_timestamp")).thenReturn(value);
        assertStartupFails(DorisSourceScanMode.INITIAL, "Invalid offset timestamp");
    }

    @Test
    void queryFailureDoesNotFallBackToConfiguredTimestamp() throws Exception {
        when(statement.executeQuery()).thenThrow(new SQLException("SELECT denied"));
        assertStartupFails(DorisSourceScanMode.FROM_TIMESTAMP, "Failed to read offset");
    }

    @Test
    void connectionFailureDoesNotFallBackToSnapshot() throws Exception {
        DorisReadOptions readOptions = readOptions(DorisSourceScanMode.INITIAL);
        try (MockedConstruction<SimpleJdbcConnectionProvider> ignored =
                        mockConstruction(
                                SimpleJdbcConnectionProvider.class,
                                (provider, context) ->
                                        when(provider.getOrEstablishConnection())
                                                .thenThrow(new SQLException("unavailable")));
                MockedStatic<RestService> rest = rest(readOptions)) {
            assertThatThrownBy(() -> DorisSourceSplitAssigner.create(options, readOptions, 2))
                    .hasMessageContaining("Failed to read offset");
        }
    }

    private void assertStartupFails(DorisSourceScanMode mode, String message) {
        DorisReadOptions readOptions = readOptions(mode);
        try (MockedConstruction<SimpleJdbcConnectionProvider> ignored = providers();
                MockedStatic<RestService> rest = rest(readOptions)) {
            assertThatThrownBy(() -> DorisSourceSplitAssigner.create(options, readOptions, 2))
                    .hasMessageContaining(message);
        }
    }

    private MockedConstruction<SimpleJdbcConnectionProvider> providers() {
        return mockConstruction(
                SimpleJdbcConnectionProvider.class,
                (provider, context) ->
                        when(provider.getOrEstablishConnection()).thenReturn(connection));
    }

    private MockedStatic<RestService> rest(DorisReadOptions readOptions) {
        MockedStatic<RestService> rest = mockStatic(RestService.class);
        rest.when(() -> RestService.resolveCurrentTimestamp(eq(options), eq(readOptions), any()))
                .thenReturn(NOW);
        rest.when(() -> RestService.parseIdentifier(eq("db.t"), any()))
                .thenReturn(new String[] {"db", "t"});
        rest.when(() -> RestService.findPartitions(eq(options), eq(readOptions), any()))
                .thenReturn(Collections.emptyList());
        rest.when(
                        () ->
                                RestService.findIncrementalTablets(
                                        eq(options), eq(readOptions), any(), any(), any()))
                .thenReturn(Collections.singletonList(1L));
        return rest;
    }

    private DorisReadOptions readOptions(DorisSourceScanMode mode) {
        return DorisReadOptions.builder()
                .setScanMode(mode)
                .setScanTimestamp(mode == DorisSourceScanMode.FROM_TIMESTAMP ? CONFIGURED : null)
                .setBinlogOffsetTable("ops.offsets")
                .setBinlogConsumerId("orders-consumer")
                .build();
    }
}
