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

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.source.DorisSourceScanMode;
import org.apache.doris.flink.source.enumerator.DorisSourceCheckpoint;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.flink.source.split.DorisSourceSplit;
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

/** Coordinates phase-specific split assigners and checkpointed Doris source state. */
public class DorisSourceSplitAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceSplitAssigner.class);

    private final DorisSourceScanMode scanMode;
    private final int sourceParallelism;
    private final DorisSnapshotSplitAssigner snapshotSplitAssigner;
    @Nullable private final DorisStreamSplitAssigner streamSplitAssigner;

    private DorisSourceCheckpoint.Phase phase;

    public DorisSourceSplitAssigner(
            DorisOptions options, DorisReadOptions readOptions, int sourceParallelism) {
        this(
                options,
                readOptions,
                sourceParallelism,
                () -> RestService.resolveCurrentTimestamp(options, readOptions, LOG),
                null);
    }

    public DorisSourceSplitAssigner(
            DorisOptions options,
            DorisReadOptions readOptions,
            DorisSourceCheckpoint checkpoint,
            int sourceParallelism) {
        this(
                options,
                readOptions,
                checkpoint,
                sourceParallelism,
                () -> RestService.resolveCurrentTimestamp(options, readOptions, LOG));
    }

    protected DorisSourceSplitAssigner(
            DorisOptions options,
            DorisReadOptions readOptions,
            int sourceParallelism,
            Supplier<String> currentTimestampSupplier,
            @Nullable Collection<DorisSnapshotSplit> snapshotSplits) {
        validateSourceParallelism(sourceParallelism);
        this.scanMode = readOptions.getScanMode();
        this.sourceParallelism = sourceParallelism;

        DorisStreamSplitAssigner streamAssigner = null;
        if (scanMode.hasIncrementalPhase()) {
            String startTimestamp =
                    scanMode == DorisSourceScanMode.FROM_TIMESTAMP
                            ? readOptions.getScanTimestamp()
                            : currentTimestampSupplier.get();
            streamAssigner =
                    new DorisStreamSplitAssigner(
                            startTimestamp, Collections.emptyList(), currentTimestampSupplier);
        }
        this.streamSplitAssigner = streamAssigner;

        if (!scanMode.isSnapshotPhaseRequired()) {
            this.snapshotSplitAssigner = new DorisSnapshotSplitAssigner(Collections.emptyList());
        } else if (snapshotSplits == null) {
            this.snapshotSplitAssigner = new DorisSnapshotSplitAssigner(options, readOptions);
        } else {
            // for test
            this.snapshotSplitAssigner = new DorisSnapshotSplitAssigner(snapshotSplits);
        }
        this.phase =
                scanMode.isSnapshotPhaseRequired()
                        ? DorisSourceCheckpoint.Phase.SNAPSHOT
                        : DorisSourceCheckpoint.Phase.STREAM;
        LOG.info(
                "Initialized Doris source assigner in {} mode at phase {} with {} pending snapshot splits.",
                scanMode,
                phase,
                snapshotSplitAssigner.remainingSplits().size());
    }

    // restore from checkpoint
    protected DorisSourceSplitAssigner(
            DorisOptions options,
            DorisReadOptions readOptions,
            DorisSourceCheckpoint checkpoint,
            int sourceParallelism,
            Supplier<String> currentTimestampSupplier) {
        validateSourceParallelism(sourceParallelism);
        this.scanMode = readOptions.getScanMode();
        this.sourceParallelism = sourceParallelism;
        this.phase = checkpoint.getPhase();
        if (phase == DorisSourceCheckpoint.Phase.STREAM
                && checkpoint.getSourceParallelism() != sourceParallelism) {
            throw new DorisRuntimeException(
                    "Doris stream source does not support restoring with a different parallelism: "
                            + checkpoint.getSourceParallelism()
                            + " -> "
                            + sourceParallelism);
        }

        List<DorisSnapshotSplit> snapshotSplits = new ArrayList<>();
        List<DorisStreamSplit> streamSplits = new ArrayList<>();
        for (DorisSourceSplit split : checkpoint.getPendingSplits()) {
            if (split instanceof DorisSnapshotSplit) {
                snapshotSplits.add((DorisSnapshotSplit) split);
            } else if (split instanceof DorisStreamSplit) {
                streamSplits.add((DorisStreamSplit) split);
            } else {
                throw new DorisRuntimeException(
                        "Unsupported Doris source split type: " + split.getClass().getName());
            }
        }

        this.snapshotSplitAssigner = new DorisSnapshotSplitAssigner(snapshotSplits);
        DorisStreamSplitAssigner streamAssigner = null;
        if (scanMode.hasIncrementalPhase()) {
            streamAssigner =
                    new DorisStreamSplitAssigner(
                            checkpoint.getNextStreamStartTimestamp(),
                            streamSplits,
                            currentTimestampSupplier);
        }
        this.streamSplitAssigner = streamAssigner;
        LOG.info(
                "Restored Doris source assigner in phase {} with {} pending splits and next stream start timestamp {}.",
                phase,
                checkpoint.getPendingSplits().size(),
                checkpoint.getNextStreamStartTimestamp());
    }

    public Optional<DorisSourceSplit> getNext() {
        if (phase == DorisSourceCheckpoint.Phase.SNAPSHOT) {
            return snapshotSplitAssigner.getNext().map(split -> split);
        }
        return getStreamAssigner().getNext().map(split -> split);
    }

    public void addSplits(Collection<DorisSourceSplit> splits) {
        if (phase == DorisSourceCheckpoint.Phase.SNAPSHOT) {
            List<DorisSnapshotSplit> snapshotSplits = new ArrayList<>(splits.size());
            for (DorisSourceSplit split : splits) {
                if (!(split instanceof DorisSnapshotSplit)) {
                    throw new DorisRuntimeException(
                            "Cannot add a Stream split during the Snapshot phase");
                }
                snapshotSplits.add((DorisSnapshotSplit) split);
            }
            snapshotSplitAssigner.addSplits(snapshotSplits);
            return;
        }

        List<DorisStreamSplit> streamSplits = new ArrayList<>(splits.size());
        for (DorisSourceSplit split : splits) {
            if (!(split instanceof DorisStreamSplit)) {
                throw new DorisRuntimeException(
                        "Cannot add a Snapshot split during the Stream phase");
            }
            streamSplits.add((DorisStreamSplit) split);
        }
        getStreamAssigner().addSplits(streamSplits);
    }

    public DorisSourceCheckpoint snapshotState(long checkpointId) {
        Collection<? extends DorisSourceSplit> pendingSplits =
                phase == DorisSourceCheckpoint.Phase.SNAPSHOT
                        ? snapshotSplitAssigner.remainingSplits()
                        : getStreamAssigner().remainingSplits();
        return new DorisSourceCheckpoint(
                phase, nextStreamStartTimestamp(), sourceParallelism, pendingSplits);
    }

    public DorisSourceCheckpoint.Phase getPhase() {
        return phase;
    }

    public DorisSourceScanMode getScanMode() {
        return scanMode;
    }

    public boolean hasPendingSnapshotSplits() {
        return snapshotSplitAssigner.hasPendingSplits();
    }

    public String discoverNextStreamEndTimestamp() {
        return getStreamAssigner().discoverNextStreamEndTimestamp();
    }

    public void onDiscoveredEndTimestamp(String endTimestamp) {
        if (phase == DorisSourceCheckpoint.Phase.STREAM) {
            getStreamAssigner().onDiscoveredEndTimestamp(endTimestamp);
        }
    }

    /** Switches the assigner to the Stream phase at the resolved start timestamp. */
    public void enterStreamPhase() {
        getStreamAssigner();
        if (snapshotSplitAssigner.hasPendingSplits()) {
            throw new DorisRuntimeException(
                    "Cannot enter the Stream phase with pending Snapshot splits");
        }
        phase = DorisSourceCheckpoint.Phase.STREAM;
        LOG.info("Entered Doris stream phase with start timestamp {}.", nextStreamStartTimestamp());
    }

    @Nullable
    private String nextStreamStartTimestamp() {
        return streamSplitAssigner == null
                ? null
                : streamSplitAssigner.getNextStreamStartTimestamp();
    }

    private DorisStreamSplitAssigner getStreamAssigner() {
        if (streamSplitAssigner == null) {
            throw new DorisRuntimeException("Doris source has no Stream phase");
        }
        return streamSplitAssigner;
    }

    private static void validateSourceParallelism(int sourceParallelism) {
        if (sourceParallelism <= 0) {
            throw new IllegalArgumentException("Source parallelism must be greater than zero");
        }
    }
}
