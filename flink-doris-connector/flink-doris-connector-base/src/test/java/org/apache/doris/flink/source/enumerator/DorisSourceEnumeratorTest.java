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

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.source.DorisSourceScanMode;
import org.apache.doris.flink.source.assigners.DorisSourceSplitAssigner;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link DorisSourceEnumerator}. */
public class DorisSourceEnumeratorTest {
    private static long splitId = 1L;
    private TestingSplitEnumeratorContext<DorisSourceSplit> context;
    private DorisSnapshotSplit split;
    private DorisSourceEnumerator enumerator;

    @BeforeEach
    void setup() {
        this.context = new TestingSplitEnumeratorContext<>(2);
        this.split = createRandomSplit();
        this.enumerator = createEnumerator(context, split);
        this.enumerator.start();
    }

    @Test
    void testCheckpointNoSplitRequested() throws Exception {
        DorisSourceCheckpoint state = enumerator.snapshotState(1L);
        assertThat(state.getPendingSplits()).contains(split);
    }

    @Test
    void testRestoreEnumerator() throws Exception {
        DorisSourceCheckpoint state = enumerator.snapshotState(1L);
        DorisSourceEnumerator restored = createEnumerator(context, state);
        restored.start();
        assertThat(restored.snapshotState(2L).getPendingSplits()).contains(split);
    }

    @Test
    void testSplitRequestForRegisteredReader() throws Exception {
        context.registerReader(1, "somehost");
        enumerator.addReader(1);
        enumerator.handleSplitRequest(1, "somehost");
        assertThat(enumerator.snapshotState(1L).getPendingSplits()).isEmpty();
        assertThat(context.getSplitAssignments().get(1).getAssignedSplits()).contains(split);
    }

    @Test
    void testSplitRequestForNonRegisteredReader() throws Exception {
        enumerator.handleSplitRequest(1, "somehost");
        assertThat(context.getSplitAssignments()).doesNotContainKey(1);
        assertThat(enumerator.snapshotState(1L).getPendingSplits()).contains(split);
    }

    @Test
    void testNoMoreSplits() {
        // first split assignment
        context.registerReader(1, "somehost");
        enumerator.addReader(1);
        enumerator.handleSplitRequest(1, "somehost");

        // second request has no more split
        enumerator.handleSplitRequest(1, "somehost");

        assertThat(context.getSplitAssignments().get(1).getAssignedSplits()).contains(split);
        assertThat(context.getSplitAssignments().get(1).hasReceivedNoMoreSplitsSignal()).isTrue();
    }

    private static DorisSnapshotSplit createRandomSplit() {
        Set<Long> tabletIds = new HashSet<>();
        tabletIds.add(1001L);
        return new DorisSnapshotSplit(
                String.valueOf(splitId),
                new PartitionDefinition("db", "tbl", "127.0.0.1", tabletIds, "queryPlan"));
    }

    private static DorisSourceEnumerator createEnumerator(
            final SplitEnumeratorContext<DorisSourceSplit> context,
            final DorisSourceSplit... splits) {
        ArrayList<DorisSnapshotSplit> snapshots = new ArrayList<>();
        for (DorisSourceSplit split : splits) {
            snapshots.add((DorisSnapshotSplit) split);
        }
        return createEnumerator(
                context,
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.SNAPSHOT,
                        null,
                        context.currentParallelism(),
                        snapshots));
    }

    private static DorisSourceEnumerator createEnumerator(
            final SplitEnumeratorContext<DorisSourceSplit> context,
            DorisSourceCheckpoint checkpoint) {
        DorisSourceSplitAssigner splitAssigner =
                new DorisSourceSplitAssigner(
                        DorisOptions.builder()
                                .setFenodes("127.0.0.1:8030")
                                .setTableIdentifier("db.tbl")
                                .build(),
                        DorisReadOptions.builder()
                                .setScanMode(DorisSourceScanMode.SNAPSHOT)
                                .setBinlogPollIntervalMs(10_000L)
                                .build(),
                        checkpoint,
                        context.currentParallelism());
        return new DorisSourceEnumerator(context, splitAssigner, 10_000L);
    }
}
