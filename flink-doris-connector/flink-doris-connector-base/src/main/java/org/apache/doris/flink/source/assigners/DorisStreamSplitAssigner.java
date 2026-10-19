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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.MathUtils;

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.source.split.DorisStreamSplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** Discovers finite windows and groups their tablets by a stable reader assignment. */
final class DorisStreamSplitAssigner implements DorisSplitAssigner<DorisStreamSplit> {
    private static final Comparator<DorisStreamSplit> SPLIT_ORDER =
            Comparator.comparing(DorisStreamSplit::splitId);

    private final Supplier<String> currentTimestampSupplier;
    private final BiFunction<String, String, Collection<Long>> tabletSupplier;
    private final int parallelism;
    private String nextStreamStartTimestamp;
    // Replay older windows first, including when rescaling merges several readers' queues.
    private final Map<Integer, Queue<DorisStreamSplit>> pendingSplits = new TreeMap<>();

    DorisStreamSplitAssigner(
            String nextStreamStartTimestamp,
            Collection<DorisStreamSplit> pendingSplits,
            Supplier<String> currentTimestampSupplier,
            BiFunction<String, String, Collection<Long>> tabletSupplier,
            int parallelism) {
        if (nextStreamStartTimestamp == null) {
            throw new DorisRuntimeException("Missing stream start timestamp");
        }
        this.nextStreamStartTimestamp = nextStreamStartTimestamp;
        this.currentTimestampSupplier = currentTimestampSupplier;
        this.tabletSupplier = tabletSupplier;
        this.parallelism = parallelism;
        addSplits(pendingSplits);
    }

    @Override
    public Optional<DorisStreamSplit> getNext() {
        return pendingSplits.isEmpty()
                ? Optional.empty()
                : getNext(pendingSplits.keySet().iterator().next());
    }

    Optional<DorisStreamSplit> getNext(int readerId) {
        Queue<DorisStreamSplit> splits = pendingSplits.get(readerId);
        if (splits == null) {
            return Optional.empty();
        }
        DorisStreamSplit next = splits.remove();
        if (splits.isEmpty()) {
            pendingSplits.remove(readerId);
        }
        return Optional.of(next);
    }

    @Override
    public void addSplits(Collection<DorisStreamSplit> splits) {
        for (DorisStreamSplit split : splits) {
            if (split.getEndTimestamp().compareTo(nextStreamStartTimestamp) > 0) {
                throw new DorisRuntimeException(
                        "Pending Doris Stream split ends after the discovery cursor");
            }
            // A restored group can span several readers after rescaling.
            Map<Integer, List<Long>> groups = groupTablets(split.getTabletIds());
            for (Map.Entry<Integer, List<Long>> group : groups.entrySet()) {
                pendingSplits
                        .computeIfAbsent(
                                group.getKey(), ignored -> new PriorityQueue<>(SPLIT_ORDER))
                        .add(
                                groups.size() == 1
                                        ? split
                                        : DorisStreamSplit.of(
                                                group.getKey(),
                                                split.getStartTimestamp(),
                                                split.getEndTimestamp(),
                                                group.getValue()));
            }
        }
    }

    @Override
    public List<DorisStreamSplit> remainingSplits() {
        List<DorisStreamSplit> remaining = new ArrayList<>();
        pendingSplits.values().forEach(remaining::addAll);
        remaining.sort(SPLIT_ORDER);
        return remaining;
    }

    @Override
    public boolean hasPendingSplits() {
        return !pendingSplits.isEmpty();
    }

    Tuple2<String, Collection<Long>> discoverNextStreamEndTimestamp() {
        String endTimestamp = currentTimestampSupplier.get();
        if (endTimestamp == null) {
            throw new DorisRuntimeException("Missing stream end timestamp");
        }
        return Tuple2.of(
                endTimestamp,
                endTimestamp.equals(nextStreamStartTimestamp)
                        ? Collections.emptyList()
                        : tabletSupplier.apply(nextStreamStartTimestamp, endTimestamp));
    }

    void onDiscoveredEndTimestamp(Tuple2<String, Collection<Long>> discovery) {
        if (hasPendingSplits()) {
            throw new DorisRuntimeException("Discovered Stream window overlaps unfinished work");
        }
        String endTimestamp = discovery.f0;
        int comparison = endTimestamp.compareTo(nextStreamStartTimestamp);
        if (comparison < 0) {
            throw new DorisRuntimeException(
                    "Current Doris timestamp moved backwards from "
                            + nextStreamStartTimestamp
                            + " to "
                            + endTimestamp);
        }
        if (comparison == 0) {
            return;
        }
        for (Map.Entry<Integer, List<Long>> group : groupTablets(discovery.f1).entrySet()) {
            pendingSplits
                    .computeIfAbsent(group.getKey(), ignored -> new PriorityQueue<>(SPLIT_ORDER))
                    .add(
                            DorisStreamSplit.of(
                                    group.getKey(),
                                    nextStreamStartTimestamp,
                                    endTimestamp,
                                    group.getValue()));
        }
        nextStreamStartTimestamp = endTimestamp;
    }

    private Map<Integer, List<Long>> groupTablets(Collection<Long> tablets) {
        Map<Integer, List<Long>> groups = new TreeMap<>();
        for (Long tablet : tablets) {
            int reader = MathUtils.murmurHash(Long.hashCode(tablet)) % parallelism;
            groups.computeIfAbsent(reader, ignored -> new ArrayList<>()).add(tablet);
        }
        return groups;
    }

    String getNextStreamStartTimestamp() {
        return nextStreamStartTimestamp;
    }
}
