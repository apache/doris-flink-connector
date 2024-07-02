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

import org.apache.doris.flink.source.enumerator.PendingSplitsCheckpoint;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

/** The {@code SimpleSplitAssigner} hands out splits in a random order. */
public class SimpleSplitAssigner implements DorisSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleSplitAssigner.class);
    private final ArrayList<DorisSourceSplit> splits;

    public SimpleSplitAssigner(Collection<DorisSourceSplit> splits) {
        this.splits = new ArrayList<>(splits);
    }

    @Override
    public Optional<DorisSourceSplit> getNext(@Nullable String hostname) {
        final int size = splits.size();
        return size == 0 ? Optional.empty() : Optional.of(splits.remove(size - 1));
    }

    @Override
    public void addSplits(Collection<DorisSourceSplit> splits) {
        LOG.info("Adding splits: {}", splits);
        splits.addAll(splits);
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) {
        return new PendingSplitsCheckpoint(splits);
    }

    @Override
    public String toString() {
        return "SimpleSplitAssigner " + splits;
    }
}
