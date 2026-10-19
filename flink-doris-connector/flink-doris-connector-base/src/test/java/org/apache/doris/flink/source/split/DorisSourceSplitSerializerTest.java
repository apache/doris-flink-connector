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

package org.apache.doris.flink.source.split;

import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.sink.OptionUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DorisSourceSplitSerializerTest {

    private final DorisSourceSplitSerializer serializer = new DorisSourceSplitSerializer();

    @Test
    void roundTripsSnapshotSplit() throws Exception {
        DorisSnapshotSplit split =
                new DorisSnapshotSplit("snapshot-1", OptionUtils.buildPartitionDef());

        DorisSnapshotSplit restored = (DorisSnapshotSplit) roundTrip(split);
        assertThat(restored).isEqualTo(split);
        assertThat(restored.splitId()).isEqualTo("snapshot-1");
        assertThat(restored.getPartitionDefinition().getQueryPlan())
                .isEqualTo(split.getPartitionDefinition().getQueryPlan());
    }

    @Test
    void roundTripsSnapshotQueryPlanLargerThanWriteUtfLimit() throws Exception {
        String queryPlan = String.join("", Collections.nCopies(70_000, "x"));
        DorisSnapshotSplit split =
                new DorisSnapshotSplit(
                        "large-plan",
                        new PartitionDefinition(
                                "db", "table", "be:9060", Collections.singleton(1L), queryPlan));

        DorisSnapshotSplit restored = (DorisSnapshotSplit) roundTrip(split);

        assertThat(restored.splitId()).isEqualTo("large-plan");
        assertThat(restored.getPartitionDefinition().getQueryPlan()).isEqualTo(queryPlan);
    }

    @Test
    void roundTripsStreamSplit() throws Exception {
        DorisStreamSplit split =
                DorisStreamSplit.of(
                        0,
                        "2026-07-20 10:00:00",
                        "2026-07-20 10:00:10",
                        Collections.singleton(100L));

        assertThat(split.splitId()).startsWith("stream-20260720100000-20260720100010-0-");
        assertThat(roundTrip(split)).isEqualTo(split);
        DorisStreamSplit another =
                DorisStreamSplit.of(
                        0,
                        split.getStartTimestamp(),
                        split.getEndTimestamp(),
                        split.getTabletIds());
        assertThat(another.splitId()).isNotEqualTo(split.splitId());
    }

    @Test
    void retainsTabletGroupAcrossRestore() throws Exception {
        DorisStreamSplit split =
                DorisStreamSplit.of(
                        0,
                        "2026-07-20 10:00:00",
                        "2026-07-20 10:00:10",
                        java.util.Arrays.asList(101L, 103L));
        DorisStreamSplit restored = (DorisStreamSplit) roundTrip(split);
        assertThat(restored.getTabletIds()).containsExactly(101L, 103L);
        assertThat(restored).isEqualTo(split);
    }

    @Test
    void rejectsLegacySerializerVersions() {
        assertThatThrownBy(() -> serializer.deserialize(3, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown version");
        assertThatThrownBy(() -> serializer.deserialize(4, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown version");
    }

    @Test
    void rejectsInvalidStreamTimestamp() {
        assertThatThrownBy(
                        () ->
                                new DorisStreamSplit(
                                        "stream-invalid",
                                        "2026-07-20",
                                        "2026-07-20 10:00:10",
                                        Collections.singleton(100L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("yyyy-MM-dd HH:mm:ss");
    }

    @Test
    void rejectsNonIncreasingStreamRange() {
        assertThatThrownBy(
                        () ->
                                DorisStreamSplit.of(
                                        0,
                                        "2026-07-20 10:00:10",
                                        "2026-07-20 10:00:10",
                                        Collections.singleton(100L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("before");
    }

    @Test
    void rejectsStreamSplitWithoutTablets() {
        assertThatThrownBy(
                        () ->
                                DorisStreamSplit.of(
                                        0,
                                        "2026-07-20 10:00:00",
                                        "2026-07-20 10:00:10",
                                        Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one tablet");
        assertThatThrownBy(
                        () ->
                                new DorisStreamSplit(
                                        "stream-empty",
                                        "2026-07-20 10:00:00",
                                        "2026-07-20 10:00:10",
                                        Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one tablet");
    }

    private DorisSourceSplit roundTrip(DorisSourceSplit split) throws Exception {
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
    }
}
