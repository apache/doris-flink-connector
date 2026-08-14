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

import org.apache.flink.api.connector.source.Boundedness;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DorisSourceOptionsTest {

    @Test
    void parsesScanModes() {
        assertThat(DorisSourceScanMode.fromOption("snapshot"))
                .isEqualTo(DorisSourceScanMode.SNAPSHOT);
        assertThat(DorisSourceScanMode.fromOption("INITIAL"))
                .isEqualTo(DorisSourceScanMode.INITIAL);
        assertThat(DorisSourceScanMode.fromOption("latest")).isEqualTo(DorisSourceScanMode.LATEST);
        assertThat(DorisSourceScanMode.fromOption("from-timestamp"))
                .isEqualTo(DorisSourceScanMode.FROM_TIMESTAMP);
        assertThat(DorisSourceScanMode.INITIAL.hasIncrementalPhase()).isTrue();
        assertThat(DorisSourceScanMode.INITIAL.isSnapshotPhaseRequired()).isTrue();
        assertThat(DorisSourceScanMode.SNAPSHOT.hasIncrementalPhase()).isFalse();
    }

    @Test
    void parsesIncrementTypes() {
        assertThat(DorisBinlogIncrementType.fromOption("detail").toSqlValue()).isEqualTo("DETAIL");
        assertThat(DorisBinlogIncrementType.fromOption("min_delta").toSqlValue())
                .isEqualTo("MIN_DELTA");
        assertThat(DorisBinlogIncrementType.fromOption("append_only").toSqlValue())
                .isEqualTo("APPEND_ONLY");
    }

    @Test
    void rejectsUnknownOptions() {
        assertThatThrownBy(() -> DorisSourceScanMode.fromOption("invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source.scan.mode");
        assertThatThrownBy(() -> DorisSourceScanMode.fromOption("from_timestamp"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source.scan.mode");
        assertThatThrownBy(() -> DorisBinlogIncrementType.fromOption("invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source.binlog.increment-type");
    }

    @Test
    void usesBackwardCompatibleDefaults() {
        DorisReadOptions options = DorisReadOptions.defaults();

        assertThat(options.getScanMode()).isEqualTo(DorisSourceScanMode.SNAPSHOT);
        assertThat(options.getScanTimestamp()).isNull();
        assertThat(options.getBinlogIncrementType()).isEqualTo(DorisBinlogIncrementType.DETAIL);
        assertThat(options.getBinlogPollIntervalMs()).isEqualTo(10_000L);
        assertThat(options.getBinlogOffsetTable()).isNull();
        assertThat(options.getBinlogConsumerId()).isNull();
    }

    @Test
    void copyPreservesIncrementalOptions() {
        DorisReadOptions options =
                DorisReadOptions.builder()
                        .setScanMode(DorisSourceScanMode.FROM_TIMESTAMP)
                        .setScanTimestamp("2026-07-20 10:00:00")
                        .setBinlogIncrementType(DorisBinlogIncrementType.MIN_DELTA)
                        .setBinlogPollIntervalMs(3_000L)
                        .setBinlogOffsetTable("ops.flink_source_offsets")
                        .setBinlogConsumerId("prod.sales.orders")
                        .build();

        DorisReadOptions copy = options.copy();

        assertThat(copy).isEqualTo(options).isNotSameAs(options);
        assertThat(copy.getScanMode()).isEqualTo(DorisSourceScanMode.FROM_TIMESTAMP);
        assertThat(copy.getScanTimestamp()).isEqualTo("2026-07-20 10:00:00");
        assertThat(copy.getBinlogIncrementType()).isEqualTo(DorisBinlogIncrementType.MIN_DELTA);
        assertThat(copy.getBinlogPollIntervalMs()).isEqualTo(3_000L);
        assertThat(copy.getBinlogOffsetTable()).isEqualTo("ops.flink_source_offsets");
        assertThat(copy.getBinlogConsumerId()).isEqualTo("prod.sales.orders");
    }

    @Test
    void validatesIncrementalSourceRequirements() {
        assertThatThrownBy(
                        () ->
                                buildSource(
                                        DorisReadOptions.builder()
                                                .setScanMode(DorisSourceScanMode.LATEST)
                                                .setUseFlightSql(false)
                                                .build()))
                .hasMessageContaining("source.use-flight-sql=true");
        assertThatThrownBy(
                        () ->
                                buildSource(
                                        DorisReadOptions.builder()
                                                .setScanMode(DorisSourceScanMode.LATEST)
                                                .setFilterQuery("id > 1")
                                                .build()))
                .hasMessageContaining("filter or limit");
        assertThatThrownBy(
                        () ->
                                buildSource(
                                        DorisReadOptions.builder()
                                                .setScanMode(DorisSourceScanMode.SNAPSHOT)
                                                .setScanTimestamp("2026-07-20 10:00:00")
                                                .build()))
                .hasMessageContaining("only valid in from-timestamp");
        assertThatThrownBy(
                        () ->
                                buildSource(
                                        DorisReadOptions.builder()
                                                .setScanMode(DorisSourceScanMode.FROM_TIMESTAMP)
                                                .setScanTimestamp("2026-07-20")
                                                .build()))
                .hasMessageContaining("yyyy-MM-dd HH:mm:ss");
        assertThatThrownBy(
                        () ->
                                buildSource(
                                        DorisReadOptions.builder()
                                                .setScanMode(DorisSourceScanMode.LATEST)
                                                .build(),
                                        "catalog.db.table"))
                .hasMessageContaining("only support table.identifier=db.table");
        assertThatThrownBy(
                        () ->
                                buildSource(
                                        DorisReadOptions.builder()
                                                .setBinlogPollIntervalMs(999L)
                                                .build()))
                .hasMessageContaining("at least 1 second");
        assertThat(buildSource(DorisReadOptions.builder().setBinlogPollIntervalMs(1_000L).build()))
                .isNotNull();
        assertThat(
                        buildSource(
                                DorisReadOptions.builder()
                                        .setScanMode(DorisSourceScanMode.SNAPSHOT)
                                        .build(),
                                "catalog.db.table"))
                .isNotNull();
    }

    @Test
    void validatesOffsetPersistenceOptions() {
        assertThatThrownBy(
                        () ->
                                buildSource(
                                        DorisReadOptions.builder()
                                                .setScanMode(DorisSourceScanMode.LATEST)
                                                .setBinlogOffsetTable("ops.flink_source_offsets")
                                                .build()))
                .hasMessageContaining("must be configured together");
        assertThatThrownBy(
                        () ->
                                buildSource(
                                        DorisReadOptions.builder()
                                                .setScanMode(DorisSourceScanMode.SNAPSHOT)
                                                .setBinlogOffsetTable("ops.flink_source_offsets")
                                                .setBinlogConsumerId("prod.sales.orders")
                                                .build()))
                .hasMessageContaining("only valid in incremental source modes");
        assertThatThrownBy(
                        () ->
                                buildSource(
                                        DorisReadOptions.builder()
                                                .setScanMode(DorisSourceScanMode.LATEST)
                                                .setBinlogOffsetTable("ops.flink_source_offsets")
                                                .setBinlogConsumerId("prod.sales.orders")
                                                .build()))
                .hasMessageContaining("jdbc-url");
        assertThat(
                        buildSource(
                                DorisReadOptions.builder()
                                        .setScanMode(DorisSourceScanMode.LATEST)
                                        .setBinlogOffsetTable("ops.flink_source_offsets")
                                        .setBinlogConsumerId("prod.sales.orders")
                                        .build(),
                                "db.table",
                                "jdbc:mysql://127.0.0.1:9030"))
                .isNotNull();
    }

    @Test
    void incrementalSourceIsUnbounded() {
        DorisSource<?> source =
                buildSource(
                        DorisReadOptions.builder()
                                .setScanMode(DorisSourceScanMode.FROM_TIMESTAMP)
                                .setScanTimestamp("2026-07-20 10:00:00")
                                .build());

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
    }

    private static DorisSource<?> buildSource(DorisReadOptions readOptions) {
        return buildSource(readOptions, "db.table");
    }

    private static DorisSource<?> buildSource(
            DorisReadOptions readOptions, String tableIdentifier) {
        return buildSource(readOptions, tableIdentifier, null);
    }

    private static DorisSource<?> buildSource(
            DorisReadOptions readOptions, String tableIdentifier, String jdbcUrl) {
        return DorisSource.<List<?>>builder()
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes("127.0.0.1:8030")
                                .setTableIdentifier(tableIdentifier)
                                .setJdbcUrl(jdbcUrl)
                                .build())
                .setDorisReadOptions(readOptions)
                .setDeserializer(new SimpleListDeserializationSchema())
                .build();
    }
}
