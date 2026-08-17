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

package org.apache.doris.flink.source.enumerator;

import org.apache.doris.flink.sink.OptionUtils;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.flink.source.split.DorisSourceSplitSerializer;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DorisSourceCheckpointSerializerTest {

    private final DorisSourceCheckpointSerializer serializer =
            new DorisSourceCheckpointSerializer(DorisSourceSplitSerializer.INSTANCE);

    @Test
    void roundTripsSnapshotCoordinatorState() throws Exception {
        DorisSnapshotSplit first =
                new DorisSnapshotSplit("snapshot-1", OptionUtils.buildPartitionDef());
        DorisSnapshotSplit second =
                new DorisSnapshotSplit("snapshot-2", OptionUtils.buildPartitionDef());
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.SNAPSHOT,
                        "2026-07-20 10:00:00",
                        2,
                        Arrays.asList(first, second));

        DorisSourceCheckpoint restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint));

        assertThat(restored).isEqualTo(checkpoint);
        assertThat(restored.getSourceParallelism()).isEqualTo(2);
        assertThat(restored.getPendingSplits()).containsExactly(first, second);
    }

    @Test
    void roundTripsStreamCoordinatorState() throws Exception {
        DorisStreamSplit first = DorisStreamSplit.of("2026-07-20 10:00:00", "2026-07-20 10:00:10");
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:10",
                        1,
                        Arrays.asList(first));

        DorisSourceCheckpoint restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint));

        assertThat(restored).isEqualTo(checkpoint);
        assertThat(restored.getPendingSplits()).containsExactly(first);
    }

    @Test
    void rejectsUnknownVersion() {
        assertThatThrownBy(() -> serializer.deserialize(99, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown version");
    }
}
