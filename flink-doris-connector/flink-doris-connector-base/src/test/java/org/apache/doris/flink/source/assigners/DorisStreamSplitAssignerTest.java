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

import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DorisStreamSplitAssignerTest {
    @ParameterizedTest
    @ValueSource(ints = {2, 4})
    void distributesRegularlySpacedTabletIdsAcrossReaders(int stride) {
        List<Long> tablets =
                LongStream.range(0, 100)
                        .map(index -> 1790655436160L + index * stride)
                        .boxed()
                        .collect(Collectors.toList());
        DorisStreamSplitAssigner assigner =
                new DorisStreamSplitAssigner(
                        "2026-07-20 10:00:00",
                        Collections.emptyList(),
                        () -> "2026-07-20 10:00:10",
                        () -> tablets,
                        4);
        assigner.onDiscoveredEndTimestamp(assigner.discoverNextStreamEndTimestamp());

        assertThat(assigner.remainingSplits()).hasSize(4);
        assertThat(assigner.remainingSplits())
                .flatExtracting(DorisStreamSplit::getTabletIds)
                .containsExactlyInAnyOrderElementsOf(tablets);
    }

    @Test
    void discoversAddedTabletsWithoutMovingExistingTablets() {
        AtomicReference<String> end = new AtomicReference<>("2026-07-20 10:00:10");
        AtomicReference<Collection<Long>> tablets =
                new AtomicReference<>(Arrays.asList(100L, 103L));
        DorisStreamSplitAssigner assigner =
                new DorisStreamSplitAssigner(
                        "2026-07-20 10:00:00",
                        Collections.emptyList(),
                        end::get,
                        () -> tablets.get(),
                        2);
        assigner.onDiscoveredEndTimestamp(assigner.discoverNextStreamEndTimestamp());
        assertThat(assigner.getNext(0).get().getTabletIds()).containsExactly(100L);
        assertThat(assigner.getNext(1).get().getTabletIds()).containsExactly(103L);
        tablets.set(Arrays.asList(100L, 103L, 102L));
        end.set("2026-07-20 10:00:20");
        assigner.onDiscoveredEndTimestamp(assigner.discoverNextStreamEndTimestamp());
        DorisStreamSplit second = assigner.getNext(0).get();
        assertThat(second.getTabletIds()).containsExactly(100L, 102L);
        assertThat(second.getStartTimestamp()).isEqualTo("2026-07-20 10:00:10");
        assertThat(assigner.getNext(1).get().getTabletIds()).containsExactly(103L);
    }

    @Test
    void advancesAnEmptyWindowWithoutCreatingAnUnrestrictedQuery() {
        DorisStreamSplitAssigner assigner =
                new DorisStreamSplitAssigner(
                        "2026-07-20 10:00:00",
                        Collections.emptyList(),
                        () -> "2026-07-20 10:00:10",
                        () -> Collections.emptyList(),
                        2);
        assigner.onDiscoveredEndTimestamp(assigner.discoverNextStreamEndTimestamp());
        assertThat(assigner.remainingSplits()).isEmpty();
        assertThat(assigner.getNextStreamStartTimestamp()).isEqualTo("2026-07-20 10:00:10");
    }

    @Test
    void restoresAndReturnsGroupsToTheirOriginalReaders() {
        DorisStreamSplit first =
                DorisStreamSplit.of(
                        "2026-07-20 10:00:00", "2026-07-20 10:00:10", Collections.singleton(100L));
        DorisStreamSplit second =
                DorisStreamSplit.of(
                        "2026-07-20 10:00:00", "2026-07-20 10:00:10", Collections.singleton(103L));
        DorisStreamSplitAssigner assigner =
                new DorisStreamSplitAssigner(
                        "2026-07-20 10:00:10",
                        Arrays.asList(first, second),
                        () -> "2026-07-20 10:00:20",
                        () -> {
                            throw new AssertionError("Restore must not rediscover pending tablets");
                        },
                        2);
        assertThat(assigner.getNext(1)).contains(second);
        assigner.addSplits(Collections.singleton(second));
        assertThat(assigner.remainingSplits()).containsExactly(first, second);
        assertThat(assigner.getNext(0)).contains(first);
        assertThat(assigner.getNext(0)).isEmpty();
        assertThat(assigner.getNext(1)).contains(second);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 6})
    void restoresEveryPendingTabletAfterRescaling(int parallelism) {
        List<Long> tablets = LongStream.range(100, 120).boxed().collect(Collectors.toList());
        DorisStreamSplitAssigner original =
                new DorisStreamSplitAssigner(
                        "2026-07-20 10:00:00",
                        Collections.emptyList(),
                        () -> "2026-07-20 10:00:10",
                        () -> tablets,
                        4);
        original.onDiscoveredEndTimestamp(original.discoverNextStreamEndTimestamp());
        DorisStreamSplitAssigner restored =
                new DorisStreamSplitAssigner(
                        "2026-07-20 10:00:10",
                        original.remainingSplits(),
                        () -> "2026-07-20 10:00:20",
                        () -> {
                            throw new AssertionError("Restore must not rediscover pending tablets");
                        },
                        parallelism);
        // Checkpoint again before consuming any of the reassigned work.
        restored =
                new DorisStreamSplitAssigner(
                        "2026-07-20 10:00:10",
                        restored.remainingSplits(),
                        () -> "2026-07-20 10:00:20",
                        () -> tablets,
                        parallelism);
        List<DorisStreamSplit> assigned = new ArrayList<>();
        for (int reader = 0; reader < parallelism; reader++) {
            java.util.Optional<DorisStreamSplit> next;
            while ((next = restored.getNext(reader)).isPresent()) {
                assigned.add(next.get());
            }
        }
        assertThat(assigned)
                .flatExtracting(DorisStreamSplit::getTabletIds)
                .containsExactlyInAnyOrderElementsOf(tablets);
        assertThat(assigned).extracting(DorisStreamSplit::splitId).doesNotHaveDuplicates();
        assertThat(assigned)
                .allSatisfy(
                        split -> {
                            assertThat(split.getStartTimestamp()).isEqualTo("2026-07-20 10:00:00");
                            assertThat(split.getEndTimestamp()).isEqualTo("2026-07-20 10:00:10");
                        });
        assertThat(restored.hasPendingSplits()).isFalse();
    }

    @Test
    void rejectsAWindowThatWouldOverwritePendingGroups() {
        DorisStreamSplit pending =
                DorisStreamSplit.of(
                        "2026-07-20 10:00:00", "2026-07-20 10:00:10", Collections.singleton(103L));
        DorisStreamSplitAssigner assigner =
                new DorisStreamSplitAssigner(
                        "2026-07-20 10:00:10",
                        Collections.singleton(pending),
                        () -> "2026-07-20 10:00:20",
                        () -> Collections.singleton(100L),
                        2);
        assertThatThrownBy(
                        () ->
                                assigner.onDiscoveredEndTimestamp(
                                        assigner.discoverNextStreamEndTimestamp()))
                .hasMessageContaining("unfinished work");
        assertThat(assigner.remainingSplits()).containsExactly(pending);
    }
}
