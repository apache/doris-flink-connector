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

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/** Discovers and assigns finite Doris Stream split intervals. */
final class DorisStreamSplitAssigner implements DorisSplitAssigner<DorisStreamSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamSplitAssigner.class);

    private final Supplier<String> currentTimestampSupplier;
    private String nextStreamStartTimestamp;
    @Nullable private DorisStreamSplit pendingSplit;

    DorisStreamSplitAssigner(
            String nextStreamStartTimestamp,
            Collection<DorisStreamSplit> pendingSplits,
            Supplier<String> currentTimestampSupplier) {
        if (nextStreamStartTimestamp == null) {
            throw new DorisRuntimeException("Missing stream start timestamp");
        }
        if (pendingSplits.size() > 1) {
            throw new DorisRuntimeException(
                    "Doris Stream assigner supports at most one pending split");
        }
        this.nextStreamStartTimestamp = nextStreamStartTimestamp;
        this.currentTimestampSupplier = currentTimestampSupplier;
        this.pendingSplit = pendingSplits.isEmpty() ? null : new ArrayList<>(pendingSplits).get(0);
        validatePendingSplit();
    }

    @Override
    public Optional<DorisStreamSplit> getNext() {
        DorisStreamSplit next = pendingSplit;
        pendingSplit = null;
        return Optional.ofNullable(next);
    }

    @Override
    public void addSplits(Collection<DorisStreamSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }
        if (pendingSplit != null || splits.size() > 1) {
            throw new DorisRuntimeException(
                    "Doris Stream assigner supports at most one pending split");
        }
        pendingSplit = new ArrayList<>(splits).get(0);
        validatePendingSplit();
    }

    @Override
    public List<DorisStreamSplit> remainingSplits() {
        return pendingSplit == null
                ? Collections.emptyList()
                : Collections.singletonList(pendingSplit);
    }

    @Override
    public boolean hasPendingSplits() {
        return pendingSplit != null;
    }

    String discoverNextStreamEndTimestamp() {
        String endTimestamp = currentTimestampSupplier.get();
        if (endTimestamp == null) {
            throw new DorisRuntimeException("Missing stream end timestamp");
        }
        return endTimestamp;
    }

    void onDiscoveredEndTimestamp(String endTimestamp) {
        if (pendingSplit != null) {
            return;
        }
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
        DorisStreamSplit split = DorisStreamSplit.of(nextStreamStartTimestamp, endTimestamp);
        pendingSplit = split;
        nextStreamStartTimestamp = endTimestamp;
        LOG.info("Discovered Doris stream split {} ending at {}.", split.splitId(), endTimestamp);
    }

    String getNextStreamStartTimestamp() {
        return nextStreamStartTimestamp;
    }

    private void validatePendingSplit() {
        if (pendingSplit != null
                && !pendingSplit.getEndTimestamp().equals(nextStreamStartTimestamp)) {
            throw new DorisRuntimeException(
                    "Pending Doris Stream split end timestamp does not match the next start timestamp");
        }
    }
}
