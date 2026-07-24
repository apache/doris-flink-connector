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

import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisStreamSplit;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Coordinator-owned state for snapshot and stream split discovery. */
public final class DorisSourceCheckpoint {
    public enum Phase {
        SNAPSHOT,
        STREAM
    }

    private final Phase phase;
    @Nullable private final String nextStreamStartTimestamp;
    private final int sourceParallelism;
    private final List<DorisSourceSplit> pendingSplits;

    public DorisSourceCheckpoint(
            Phase phase,
            @Nullable String nextStreamStartTimestamp,
            int sourceParallelism,
            Collection<? extends DorisSourceSplit> pendingSplits) {
        this.phase = Objects.requireNonNull(phase, "phase");
        this.nextStreamStartTimestamp = nextStreamStartTimestamp;
        if (sourceParallelism <= 0) {
            throw new IllegalArgumentException("Source parallelism must be greater than zero");
        }
        this.sourceParallelism = sourceParallelism;
        this.pendingSplits = Collections.unmodifiableList(new ArrayList<>(pendingSplits));
        for (DorisSourceSplit split : this.pendingSplits) {
            boolean validType =
                    phase == Phase.SNAPSHOT
                            ? split instanceof DorisSnapshotSplit
                            : split instanceof DorisStreamSplit;
            if (!validType) {
                throw new IllegalArgumentException(
                        "Pending split type does not match checkpoint phase " + phase);
            }
        }
        if (phase == Phase.STREAM && nextStreamStartTimestamp == null) {
            throw new IllegalArgumentException(
                    "Stream checkpoint is missing next stream start timestamp");
        }
        if (phase == Phase.STREAM && this.pendingSplits.size() > 1) {
            throw new IllegalArgumentException(
                    "Stream checkpoint supports at most one pending split");
        }
    }

    public Phase getPhase() {
        return phase;
    }

    @Nullable
    public String getNextStreamStartTimestamp() {
        return nextStreamStartTimestamp;
    }

    public int getSourceParallelism() {
        return sourceParallelism;
    }

    public List<DorisSourceSplit> getPendingSplits() {
        return pendingSplits;
    }

    public DorisSourceCheckpoint withPhase(Phase newPhase) {
        return new DorisSourceCheckpoint(
                newPhase, nextStreamStartTimestamp, sourceParallelism, pendingSplits);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DorisSourceCheckpoint)) {
            return false;
        }
        DorisSourceCheckpoint that = (DorisSourceCheckpoint) object;
        return sourceParallelism == that.sourceParallelism
                && phase == that.phase
                && Objects.equals(nextStreamStartTimestamp, that.nextStreamStartTimestamp)
                && pendingSplits.equals(that.pendingSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(phase, nextStreamStartTimestamp, sourceParallelism, pendingSplits);
    }
}
