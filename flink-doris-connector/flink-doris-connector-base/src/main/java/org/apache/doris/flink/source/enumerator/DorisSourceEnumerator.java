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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.source.assigners.DorisSourceSplitAssigner;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/** Connects Flink coordinator callbacks to the Doris split assigner. */
public class DorisSourceEnumerator
        implements SplitEnumerator<DorisSourceSplit, DorisSourceCheckpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceEnumerator.class);

    private final SplitEnumeratorContext<DorisSourceSplit> context;
    private final DorisSourceSplitAssigner splitAssigner;
    private final long streamDiscoveryIntervalMs;
    @Nullable private final DorisOffsetPublisher offsetPublisher;
    // A reader enters this set only when it owns no unfinished split and asks for more work.
    private final TreeSet<Integer> readersAwaitingSplit = new TreeSet<>();
    // Transfers Stream discovery demand from the coordinator thread to the async worker.
    private volatile boolean streamDiscoveryRequired;
    // Remains set until the coordinator applies the result, including empty-window polls.
    private final AtomicBoolean streamDiscoveryInFlight = new AtomicBoolean();

    // The first checkpoint that records Snapshot completion and awaits completion notification.
    @Nullable private Long snapshotCompletionCheckpointId;
    private boolean streamDiscoveryStarted;
    @Nullable private String lastCompletedWindow;
    private final NavigableMap<Long, String> checkpointOffsets = new TreeMap<>();
    @Nullable private String pendingOffset;
    private boolean offsetPublishInFlight;
    private volatile boolean closed;

    public DorisSourceEnumerator(
            SplitEnumeratorContext<DorisSourceSplit> context,
            DorisSourceSplitAssigner splitAssigner,
            long streamDiscoveryIntervalMs) {
        this(context, splitAssigner, streamDiscoveryIntervalMs, null);
    }

    public DorisSourceEnumerator(
            SplitEnumeratorContext<DorisSourceSplit> context,
            DorisSourceSplitAssigner splitAssigner,
            long streamDiscoveryIntervalMs,
            @Nullable DorisOffsetPublisher offsetPublisher) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.streamDiscoveryIntervalMs = streamDiscoveryIntervalMs;
        this.offsetPublisher = offsetPublisher;
    }

    @Override
    public void start() {
        startStreamDiscovery();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            return;
        }
        readersAwaitingSplit.add(subtaskId);
        assignRequestedSplits();
    }

    /** Assigns available work according to the current Snapshot or Stream phase. */
    private void assignRequestedSplits() {
        removeUnregisteredReaders();
        for (Integer reader : new ArrayList<>(readersAwaitingSplit)) {
            Optional<DorisSourceSplit> nextSplit = splitAssigner.getNext(reader);
            if (nextSplit.isPresent()) {
                DorisSourceSplit split = nextSplit.get();
                context.assignSplit(split, reader);
                readersAwaitingSplit.remove(reader);
                LOG.info("Assigned split {} to subtask {}", split, reader);
            } else if (splitAssigner.getPhase() == DorisSourceCheckpoint.Phase.SNAPSHOT
                    && splitAssigner.getScanMode().isSnapshotOnly()) {
                context.signalNoMoreSplits(reader);
                readersAwaitingSplit.remove(reader);
                LOG.info(
                        "No more snapshot splits are available; signaling subtask {} to finish",
                        reader);
            }
        }
        if (splitAssigner.getPhase() != DorisSourceCheckpoint.Phase.STREAM) {
            return;
        }
        // Advance the completed boundary only after all readers finish the window.
        boolean allIdle =
                !splitAssigner.hasPendingStreamSplits()
                        && context.registeredReaders().size() == context.currentParallelism()
                        && readersAwaitingSplit.containsAll(context.registeredReaders().keySet());
        if (allIdle) {
            lastCompletedWindow = splitAssigner.getNextStreamStartTimestamp();
        }
        streamDiscoveryRequired = allIdle;
    }

    /** Starts one periodic loop for discovering finite Stream splits. */
    private void startStreamDiscovery() {
        if (streamDiscoveryStarted
                || splitAssigner.getPhase() != DorisSourceCheckpoint.Phase.STREAM) {
            return;
        }
        streamDiscoveryStarted = true;
        context.callAsync(
                this::tryDiscoverNextStreamEndTimestamp,
                this::handleDiscoveredEndTimestamp,
                0L,
                streamDiscoveryIntervalMs);
        LOG.info(
                "Started Doris stream split discovery with interval {} ms.",
                streamDiscoveryIntervalMs);
    }

    /**
     * Attempts to resolve a Stream boundary when a reader is waiting and no request is in flight.
     */
    @Nullable
    private Tuple2<String, Collection<Long>> tryDiscoverNextStreamEndTimestamp() {
        if (!streamDiscoveryRequired || !streamDiscoveryInFlight.compareAndSet(false, true)) {
            return null;
        }
        return splitAssigner.discoverNextStreamEndTimestamp();
    }

    /** Applies a discovered boundary and assigns the resulting Stream split. */
    private void handleDiscoveredEndTimestamp(
            Tuple2<String, Collection<Long>> result, Throwable error) {
        failOnError(error);
        if (result == null) {
            // A skipped poll does not own the in-flight discovery and must not rearm it.
            return;
        }
        if (!streamDiscoveryRequired) {
            // A reader failed while discovery was running; finish replay before advancing.
            streamDiscoveryInFlight.set(false);
            return;
        }
        splitAssigner.onDiscoveredEndTimestamp(result);
        assignRequestedSplits();
        streamDiscoveryInFlight.set(false);
    }

    @Override
    public DorisSourceCheckpoint snapshotState(long checkpointId) {
        if (offsetPublisher != null && lastCompletedWindow != null) {
            checkpointOffsets.put(checkpointId, lastCompletedWindow);
        }
        DorisSourceCheckpoint checkpoint = splitAssigner.snapshotState(checkpointId);
        if (!allSnapshotReadersIdle()) {
            return checkpoint;
        }
        if (snapshotCompletionCheckpointId == null) {
            snapshotCompletionCheckpointId = checkpointId;
            LOG.info(
                    "All Doris snapshot readers are idle; waiting for checkpoint {} to complete before entering stream phase.",
                    checkpointId);
        }
        return checkpoint.withPhase(DorisSourceCheckpoint.Phase.STREAM);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        String offset = checkpointOffsets.get(checkpointId);
        if (offset != null) {
            pendingOffset = offset;
            checkpointOffsets.headMap(checkpointId, true).clear();
            LOG.debug(
                    "Checkpoint {} completed; Doris source offset {} is pending publication",
                    checkpointId,
                    offset);
        }
        publishPendingOffset();
        if (!shouldEnterStreamPhase(checkpointId)) {
            return;
        }
        splitAssigner.enterStreamPhase();
        snapshotCompletionCheckpointId = null;
        LOG.info(
                "Doris snapshot completion checkpoint {} succeeded; entering stream phase.",
                checkpointId);
        assignRequestedSplits();
        startStreamDiscovery();
    }

    private void publishPendingOffset() {
        if (closed || offsetPublisher == null || offsetPublishInFlight || pendingOffset == null) {
            return;
        }
        String offset = pendingOffset;
        pendingOffset = null;
        offsetPublishInFlight = true;
        context.callAsync(
                () -> {
                    // Queued writes must not reopen the connection after close.
                    synchronized (offsetPublisher) {
                        if (!closed) {
                            offsetPublisher.publish(offset);
                        }
                    }
                    return null;
                },
                (ignored, error) -> {
                    if (closed) {
                        return;
                    }
                    offsetPublishInFlight = false;
                    if (error != null) {
                        LOG.warn("Failed to publish Doris source offset {}", offset, error);
                        if (pendingOffset == null) {
                            // Retry on a later checkpoint unless a newer offset is already pending.
                            pendingOffset = offset;
                            return;
                        }
                    }
                    publishPendingOffset();
                });
    }

    @Override
    public void addSplitsBack(List<DorisSourceSplit> splits, int subtaskId) {
        List<String> splitIds = new ArrayList<>(splits.size());
        for (DorisSourceSplit split : splits) {
            splitIds.add(split.splitId());
        }
        LOG.info("Adding splits {} back from subtask {}", splitIds, subtaskId);
        if (splitAssigner.getPhase() == DorisSourceCheckpoint.Phase.SNAPSHOT) {
            snapshotCompletionCheckpointId = null;
        }
        if (splitAssigner.getPhase() == DorisSourceCheckpoint.Phase.STREAM) {
            readersAwaitingSplit.remove(subtaskId);
            streamDiscoveryRequired = false;
            // Reconfirm completion after returned work has been replayed.
            lastCompletedWindow = null;
            checkpointOffsets.clear();
        }
        splitAssigner.addSplits(splits);
        assignRequestedSplits();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        checkpointOffsets.remove(checkpointId);
    }

    @Override
    public void addReader(int subtaskId) {}

    /** Returns whether the completed checkpoint can commit the transition to the Stream phase. */
    private boolean shouldEnterStreamPhase(long completedCheckpointId) {
        return snapshotCompletionCheckpointId != null
                && completedCheckpointId >= snapshotCompletionCheckpointId
                && allSnapshotReadersIdle();
    }

    /**
     * Returns whether no Snapshot work remains pending or in flight.
     *
     * <p>{@link org.apache.doris.flink.source.reader.DorisSourceReader} requests more work only
     * when it owns no unfinished split. Therefore, an empty pending queue together with every
     * registered reader waiting for a split means that all Snapshot splits are complete.
     */
    private boolean allSnapshotReadersIdle() {
        if (splitAssigner.getPhase() != DorisSourceCheckpoint.Phase.SNAPSHOT
                || splitAssigner.getScanMode().isSnapshotOnly()
                || splitAssigner.hasPendingSnapshotSplits()) {
            return false;
        }
        Set<Integer> registeredReaders = removeUnregisteredReaders();
        return registeredReaders.size() == context.currentParallelism()
                && readersAwaitingSplit.containsAll(registeredReaders);
    }

    /** Removes waiting entries for readers that are no longer registered. */
    private Set<Integer> removeUnregisteredReaders() {
        Set<Integer> registeredReaders = context.registeredReaders().keySet();
        readersAwaitingSplit.removeIf(reader -> !registeredReaders.contains(reader));
        return registeredReaders;
    }

    private static void failOnError(Throwable error) {
        if (error != null) {
            throw new DorisRuntimeException("Doris source coordinator operation failed", error);
        }
    }

    @Override
    public void close() {
        closed = true;
        if (offsetPublisher != null) {
            synchronized (offsetPublisher) {
                offsetPublisher.close();
            }
        }
    }
}
