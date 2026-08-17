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

import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

/** Plans and assigns bounded Doris Snapshot splits. */
final class DorisSnapshotSplitAssigner implements DorisSplitAssigner<DorisSnapshotSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSnapshotSplitAssigner.class);
    // Examples: snapshot-127.0.0.1:9060-0 or snapshot-catalog.database.table.
    private static final String SNAPSHOT_SPLIT_PREFIX = "snapshot-";

    private final Deque<DorisSnapshotSplit> pendingSplits;

    DorisSnapshotSplitAssigner(DorisOptions options, DorisReadOptions readOptions) {
        this(planSnapshotSplits(options, readOptions));
    }

    DorisSnapshotSplitAssigner(Collection<DorisSnapshotSplit> pendingSplits) {
        this.pendingSplits = new ArrayDeque<>(pendingSplits);
    }

    @Override
    public Optional<DorisSnapshotSplit> getNext() {
        return Optional.ofNullable(pendingSplits.pollFirst());
    }

    @Override
    public void addSplits(Collection<DorisSnapshotSplit> splits) {
        List<DorisSnapshotSplit> returnedSplits = new ArrayList<>(splits);
        ListIterator<DorisSnapshotSplit> iterator =
                returnedSplits.listIterator(returnedSplits.size());
        while (iterator.hasPrevious()) {
            pendingSplits.addFirst(iterator.previous());
        }
    }

    @Override
    public List<DorisSnapshotSplit> remainingSplits() {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public boolean hasPendingSplits() {
        return !pendingSplits.isEmpty();
    }

    private static List<DorisSnapshotSplit> planSnapshotSplits(
            DorisOptions options, DorisReadOptions readOptions) {
        try {
            List<DorisSnapshotSplit> splits = new ArrayList<>();
            String[] tableIdentifiers =
                    RestService.parseIdentifier(options.getTableIdentifier(), LOG);
            if (tableIdentifiers.length == 2) {
                List<PartitionDefinition> partitions =
                        RestService.findPartitions(options, readOptions, LOG);
                for (int index = 0; index < partitions.size(); index++) {
                    PartitionDefinition partition = partitions.get(index);
                    String splitId = SNAPSHOT_SPLIT_PREFIX + partition.getBeAddress() + "-" + index;
                    splits.add(new DorisSnapshotSplit(splitId, partition));
                }
            } else {
                Preconditions.checkArgument(
                        readOptions.getUseFlightSql(),
                        "UseFlightSql must be true when table.identifier is catalog.db.table");
                splits.add(
                        new DorisSnapshotSplit(
                                SNAPSHOT_SPLIT_PREFIX + options.getTableIdentifier(),
                                PartitionDefinition.emptyPartition(options.getTableIdentifier())));
            }
            return splits;
        } catch (Exception e) {
            throw new DorisRuntimeException("Failed to plan Doris snapshot splits", e);
        }
    }
}
