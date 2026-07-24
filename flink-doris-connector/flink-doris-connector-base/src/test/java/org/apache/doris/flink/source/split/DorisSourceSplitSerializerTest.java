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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DorisSourceSplitSerializerTest {

    private final DorisSourceSplitSerializer serializer = new DorisSourceSplitSerializer();

    @Test
    void usesVersionThreeForTaggedSplitFormat() {
        assertThat(serializer.getVersion()).isEqualTo(3);
    }

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
        DorisStreamSplit split = DorisStreamSplit.of("2026-07-20 10:00:00", "2026-07-20 10:00:10");

        assertThat(split.splitId()).isEqualTo("stream-20260720T100000-20260720T100010");
        assertThat(roundTrip(split)).isEqualTo(split);
    }

    @Test
    void restoresLegacySnapshotVersions() throws Exception {
        DorisSnapshotSplit split =
                new DorisSnapshotSplit("legacy-snapshot", OptionUtils.buildPartitionDef());

        DorisSnapshotSplit restoredV1 =
                (DorisSnapshotSplit) serializer.deserialize(1, legacySnapshotBytes(1, split));
        DorisSnapshotSplit restoredV2 =
                (DorisSnapshotSplit) serializer.deserialize(2, legacySnapshotBytes(2, split));

        assertThat(restoredV1.splitId()).isEqualTo("splitId");
        assertThat(restoredV1.getPartitionDefinition()).isEqualTo(split.getPartitionDefinition());
        assertThat(restoredV2.splitId()).isEqualTo("legacy-snapshot");
        assertThat(restoredV2.getPartitionDefinition()).isEqualTo(split.getPartitionDefinition());
    }

    @Test
    void rejectsInvalidStreamTimestamp() {
        assertThatThrownBy(
                        () ->
                                new DorisStreamSplit(
                                        "stream-invalid", "2026-07-20", "2026-07-20 10:00:10"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("yyyy-MM-dd HH:mm:ss");
    }

    @Test
    void rejectsNonIncreasingStreamRange() {
        assertThatThrownBy(() -> DorisStreamSplit.of("2026-07-20 10:00:10", "2026-07-20 10:00:10"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("before");
    }

    @Test
    void rejectsUnknownSerializerVersion() {
        assertThatThrownBy(() -> serializer.deserialize(99, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown version");
    }

    private DorisSourceSplit roundTrip(DorisSourceSplit split) throws Exception {
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
    }

    private static byte[] legacySnapshotBytes(int version, DorisSnapshotSplit split)
            throws Exception {
        PartitionDefinition partition = split.getPartitionDefinition();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeUTF(partition.getDatabase());
            out.writeUTF(partition.getTable());
            out.writeUTF(partition.getBeAddress());
            out.writeInt(partition.getTabletIds().size());
            for (Long tabletId : new TreeSet<>(partition.getTabletIds())) {
                out.writeLong(tabletId);
            }
            byte[] queryPlan = partition.getQueryPlan().getBytes(StandardCharsets.UTF_8);
            out.writeInt(queryPlan.length);
            out.write(queryPlan);
            if (version >= 2) {
                out.writeUTF(split.splitId());
            }
        }
        return bytes.toByteArray();
    }
}
