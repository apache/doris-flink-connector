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
        return DorisSource.<List<?>>builder()
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes("127.0.0.1:8030")
                                .setTableIdentifier(tableIdentifier)
                                .build())
                .setDorisReadOptions(readOptions)
                .setDeserializer(new SimpleListDeserializationSchema())
                .build();
    }
}
