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

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.source.DorisSourceScanMode;
import org.apache.doris.flink.source.assigners.DorisSourceSplitAssigner;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DorisHybridSourceEnumeratorTest {

    @Test
    void initialWaitsForAllReadersAndCompletedCheckpointBeforeStream() {
        AtomicReference<Callable<String>> pollTask = new AtomicReference<>();
        AtomicReference<BiConsumer<String, Throwable>> pollCallback = new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(pollTask, pollCallback, 0, 1);
        DorisSnapshotSplit snapshot = snapshot("snapshot-0");
        AtomicInteger timestampCalls = new AtomicInteger();
        DorisSourceEnumerator enumerator =
                enumerator(
                        context,
                        DorisSourceScanMode.INITIAL,
                        () ->
                                timestampCalls.getAndIncrement() == 0
                                        ? "2026-07-20 10:00:00"
                                        : "2026-07-20 10:00:10",
                        Collections.singletonList(snapshot));

        enumerator.start();
        enumerator.handleSplitRequest(0, "reader-0");
        enumerator.handleSplitRequest(1, "reader-1");

        verify(context).assignSplit(snapshot, 0);
        assertThat(enumerator.snapshotState(6L).getPhase())
                .isEqualTo(DorisSourceCheckpoint.Phase.SNAPSHOT);
        verify(context, never())
                .callAsync(any(Callable.class), any(BiConsumer.class), anyLong(), anyLong());

        // The reader requests again only after its restored or assigned split is finished.
        enumerator.handleSplitRequest(0, "reader-0");
        DorisSourceCheckpoint transitionCheckpoint = enumerator.snapshotState(7L);
        assertThat(transitionCheckpoint.getPhase()).isEqualTo(DorisSourceCheckpoint.Phase.STREAM);
        assertThat(transitionCheckpoint.getPendingSplits()).isEmpty();
        assertThat(timestampCalls).hasValue(1);

        enumerator.notifyCheckpointComplete(7L);
        verify(context).callAsync(any(Callable.class), any(BiConsumer.class), eq(0L), eq(10_000L));
        invoke(pollTask.get(), pollCallback.get());

        ArgumentCaptor<DorisSourceSplit> captor = ArgumentCaptor.forClass(DorisSourceSplit.class);
        verify(context, times(2)).assignSplit(captor.capture(), eq(0));
        DorisStreamSplit streamSplit = (DorisStreamSplit) captor.getAllValues().get(1);
        assertThat(streamSplit.getStartTimestamp()).isEqualTo("2026-07-20 10:00:00");
        assertThat(streamSplit.getEndTimestamp()).isEqualTo("2026-07-20 10:00:10");
        DorisSourceCheckpoint streamCheckpoint = enumerator.snapshotState(8L);
        assertThat(streamCheckpoint.getNextStreamStartTimestamp()).isEqualTo("2026-07-20 10:00:10");
        assertThat(streamCheckpoint.getPendingSplits()).isEmpty();
    }

    @Test
    void latestAssignsStreamOnlyToSubtaskZero() {
        AtomicReference<Callable<String>> pollTask = new AtomicReference<>();
        AtomicReference<BiConsumer<String, Throwable>> pollCallback = new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(pollTask, pollCallback, 0, 1);
        AtomicInteger timestampCalls = new AtomicInteger();
        DorisSourceEnumerator enumerator =
                enumerator(
                        context,
                        DorisSourceScanMode.LATEST,
                        () ->
                                timestampCalls.getAndIncrement() == 0
                                        ? "2026-07-20 10:00:00"
                                        : "2026-07-20 10:00:05",
                        Collections.emptyList());

        enumerator.start();
        enumerator.handleSplitRequest(1, "reader-1");
        invoke(pollTask.get(), pollCallback.get());

        assertThat(timestampCalls).hasValue(1);
        verify(context, never()).assignSplit(isA(DorisStreamSplit.class), eq(1));

        enumerator.handleSplitRequest(0, "reader-0");
        invoke(pollTask.get(), pollCallback.get());

        verify(context).assignSplit(isA(DorisStreamSplit.class), eq(0));
        verify(context, never()).assignSplit(isA(DorisStreamSplit.class), eq(1));
    }

    @Test
    void fromTimestampKeepsUserBoundaryAndSkipsEmptyWindow() {
        AtomicReference<Callable<String>> pollTask = new AtomicReference<>();
        AtomicReference<BiConsumer<String, Throwable>> pollCallback = new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(pollTask, pollCallback, 0);
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:00",
                        1,
                        Collections.emptyList());
        DorisSourceEnumerator enumerator =
                restoredEnumerator(
                        context,
                        DorisSourceScanMode.FROM_TIMESTAMP,
                        () -> "2026-07-20 10:00:00",
                        checkpoint);

        enumerator.start();
        enumerator.handleSplitRequest(0, "reader-0");
        invoke(pollTask.get(), pollCallback.get());

        DorisSourceCheckpoint restored = enumerator.snapshotState(1L);
        assertThat(restored.getNextStreamStartTimestamp()).isEqualTo("2026-07-20 10:00:00");
        assertThat(restored.getPendingSplits()).isEmpty();
        verify(context, never()).assignSplit(isA(DorisStreamSplit.class), eq(0));
    }

    @Test
    void restoresSnapshotCheckpointWithoutPlanningAndAllowsRescale() {
        SplitEnumeratorContext<DorisSourceSplit> context = synchronousContext(0);
        DorisSnapshotSplit snapshot = snapshot("snapshot-restored");
        AtomicInteger timestampCalls = new AtomicInteger();
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.SNAPSHOT,
                        "2026-07-20 10:00:00",
                        2,
                        Collections.singletonList(snapshot));
        DorisSourceEnumerator enumerator =
                restoredEnumerator(
                        context,
                        DorisSourceScanMode.INITIAL,
                        () -> {
                            timestampCalls.incrementAndGet();
                            return "2026-07-20 10:00:10";
                        },
                        checkpoint);

        enumerator.start();
        enumerator.handleSplitRequest(0, "reader-0");

        verify(context).assignSplit(snapshot, 0);
        assertThat(timestampCalls).hasValue(0);
        assertThat(enumerator.snapshotState(1L).getSourceParallelism()).isEqualTo(1);
    }

    @Test
    void rejectsStreamRestoreWithDifferentParallelism() {
        SplitEnumeratorContext<DorisSourceSplit> context = synchronousContext(0);
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:00",
                        2,
                        Collections.emptyList());

        assertThatThrownBy(
                        () ->
                                restoredEnumerator(
                                        context,
                                        DorisSourceScanMode.LATEST,
                                        () -> "2026-07-20 10:00:10",
                                        checkpoint))
                .hasMessageContaining("does not support restoring with a different parallelism");
    }

    @Test
    void advancesStreamCursorWhenSplitIsPlanned() {
        AtomicReference<Callable<String>> pollTask = new AtomicReference<>();
        AtomicReference<BiConsumer<String, Throwable>> pollCallback = new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(pollTask, pollCallback, 0);
        AtomicInteger timestampCalls = new AtomicInteger();
        DorisSourceEnumerator enumerator =
                enumerator(
                        context,
                        DorisSourceScanMode.LATEST,
                        () ->
                                timestampCalls.getAndIncrement() == 0
                                        ? "2026-07-20 10:00:00"
                                        : "2026-07-20 10:00:10",
                        Collections.emptyList());

        enumerator.start();
        enumerator.handleSplitRequest(0, "reader-0");
        invoke(pollTask.get(), pollCallback.get());

        ArgumentCaptor<DorisSourceSplit> captor = ArgumentCaptor.forClass(DorisSourceSplit.class);
        verify(context).assignSplit(captor.capture(), eq(0));
        DorisStreamSplit split = (DorisStreamSplit) captor.getValue();
        assertThat(split.getStartTimestamp()).isEqualTo("2026-07-20 10:00:00");
        assertThat(split.getEndTimestamp()).isEqualTo("2026-07-20 10:00:10");
        DorisSourceCheckpoint checkpoint = enumerator.snapshotState(1L);
        assertThat(checkpoint.getNextStreamStartTimestamp()).isEqualTo("2026-07-20 10:00:10");
        assertThat(checkpoint.getPendingSplits()).isEmpty();
    }

    @Test
    void returnedStreamSplitDoesNotMoveCursorBack() {
        DorisStreamSplit first = DorisStreamSplit.of("2026-07-20 10:00:00", "2026-07-20 10:00:10");
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:10",
                        1,
                        Collections.singletonList(first));
        DorisSourceSplitAssigner assigner =
                new DorisSourceSplitAssigner(
                        dorisOptions(),
                        readOptions(DorisSourceScanMode.LATEST, null),
                        checkpoint,
                        1);

        assertThat(assigner.getNext()).contains(first);
        assigner.addSplits(Collections.singletonList(first));

        DorisSourceCheckpoint returned = assigner.snapshotState(1L);
        assertThat(returned.getPendingSplits()).containsExactly(first);
        assertThat(returned.getNextStreamStartTimestamp()).isEqualTo("2026-07-20 10:00:10");
    }

    @Test
    void returnedSnapshotSplitCancelsCompletionCandidate() {
        SplitEnumeratorContext<DorisSourceSplit> context = synchronousContext(0);
        DorisSnapshotSplit snapshot = snapshot("snapshot-returned");
        DorisSourceEnumerator enumerator =
                enumerator(
                        context,
                        DorisSourceScanMode.INITIAL,
                        () -> "2026-07-20 10:00:00",
                        Collections.singletonList(snapshot));

        enumerator.start();
        enumerator.handleSplitRequest(0, "reader-0");
        enumerator.handleSplitRequest(0, "reader-0");
        assertThat(enumerator.snapshotState(7L).getPhase())
                .isEqualTo(DorisSourceCheckpoint.Phase.STREAM);

        enumerator.addSplitsBack(Collections.singletonList(snapshot), 0);

        assertThat(enumerator.snapshotState(8L).getPhase())
                .isEqualTo(DorisSourceCheckpoint.Phase.SNAPSHOT);
        verify(context, times(2)).assignSplit(snapshot, 0);
    }

    @Test
    void discoversStreamBoundariesWithoutAnAdditionalCompletionDelay() {
        AtomicInteger timestampCalls = new AtomicInteger();
        DorisSourceSplitAssigner assigner =
                new TestingDorisSourceSplitAssigner(
                        dorisOptions(),
                        readOptions(DorisSourceScanMode.LATEST, null),
                        1,
                        () ->
                                timestampCalls.getAndIncrement() == 0
                                        ? "2026-07-20 10:00:00"
                                        : timestampCalls.get() == 2
                                                ? "2026-07-20 10:00:10"
                                                : "2026-07-20 10:00:20",
                        Collections.emptyList());

        assertThat(assigner.discoverNextStreamEndTimestamp()).isEqualTo("2026-07-20 10:00:10");
        assigner.onDiscoveredEndTimestamp("2026-07-20 10:00:10");
        assertThat(assigner.getNext()).isPresent();

        assertThat(assigner.discoverNextStreamEndTimestamp()).isEqualTo("2026-07-20 10:00:20");
        assertThat(timestampCalls).hasValue(3);
    }

    @Test
    void failsWhenDorisTimestampMovesBackwards() {
        AtomicReference<Callable<String>> pollTask = new AtomicReference<>();
        AtomicReference<BiConsumer<String, Throwable>> pollCallback = new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(pollTask, pollCallback, 0);
        AtomicInteger calls = new AtomicInteger();
        DorisSourceEnumerator enumerator =
                enumerator(
                        context,
                        DorisSourceScanMode.LATEST,
                        () ->
                                calls.getAndIncrement() == 0
                                        ? "2026-07-20 10:00:10"
                                        : "2026-07-20 10:00:00",
                        Collections.emptyList());

        enumerator.start();
        enumerator.handleSplitRequest(0, "reader-0");

        assertThatThrownBy(() -> invoke(pollTask.get(), pollCallback.get()))
                .hasMessage(
                        "Current Doris timestamp moved backwards from 2026-07-20 10:00:10 to 2026-07-20 10:00:00");
    }

    private static DorisSourceEnumerator enumerator(
            SplitEnumeratorContext<DorisSourceSplit> context,
            DorisSourceScanMode mode,
            Supplier<String> timestampSupplier,
            List<DorisSnapshotSplit> snapshots) {
        DorisReadOptions testReadOptions = readOptions(mode, null);
        DorisSourceSplitAssigner splitAssigner =
                new TestingDorisSourceSplitAssigner(
                        dorisOptions(),
                        testReadOptions,
                        context.currentParallelism(),
                        timestampSupplier,
                        snapshots);
        return new DorisSourceEnumerator(
                context, splitAssigner, testReadOptions.getBinlogPollIntervalMs());
    }

    private static DorisSourceEnumerator restoredEnumerator(
            SplitEnumeratorContext<DorisSourceSplit> context,
            DorisSourceScanMode mode,
            Supplier<String> timestampSupplier,
            DorisSourceCheckpoint checkpoint) {
        DorisReadOptions testReadOptions =
                readOptions(mode, checkpoint.getNextStreamStartTimestamp());
        DorisSourceSplitAssigner splitAssigner =
                new TestingDorisSourceSplitAssigner(
                        dorisOptions(),
                        testReadOptions,
                        context.currentParallelism(),
                        timestampSupplier,
                        checkpoint);
        return new DorisSourceEnumerator(
                context, splitAssigner, testReadOptions.getBinlogPollIntervalMs());
    }

    private static DorisOptions dorisOptions() {
        return DorisOptions.builder()
                .setFenodes("127.0.0.1:8030")
                .setTableIdentifier("db.table")
                .build();
    }

    private static DorisReadOptions readOptions(
            DorisSourceScanMode mode, String configuredStartTimestamp) {
        DorisReadOptions.Builder builder =
                DorisReadOptions.builder().setScanMode(mode).setBinlogPollIntervalMs(10_000L);
        if (mode == DorisSourceScanMode.FROM_TIMESTAMP) {
            builder.setScanTimestamp(configuredStartTimestamp);
        }
        return builder.build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static SplitEnumeratorContext<DorisSourceSplit> synchronousContext(int... readerIds) {
        SplitEnumeratorContext<DorisSourceSplit> context = mock(SplitEnumeratorContext.class);
        Map<Integer, ReaderInfo> readers = registeredReaders(readerIds);
        when(context.registeredReaders()).thenReturn(readers);
        when(context.currentParallelism()).thenReturn(readerIds.length);
        doAnswer(
                        invocation -> {
                            Callable callable = invocation.getArgument(0);
                            BiConsumer callback = invocation.getArgument(1);
                            invoke(callable, callback);
                            return null;
                        })
                .when(context)
                .callAsync(any(Callable.class), any(BiConsumer.class), anyLong(), anyLong());
        return context;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static SplitEnumeratorContext<DorisSourceSplit> controlledPeriodicContext(
            AtomicReference<Callable<String>> pollTask,
            AtomicReference<BiConsumer<String, Throwable>> pollCallback,
            int... readerIds) {
        SplitEnumeratorContext<DorisSourceSplit> context = mock(SplitEnumeratorContext.class);
        Map<Integer, ReaderInfo> readers = registeredReaders(readerIds);
        when(context.registeredReaders()).thenReturn(readers);
        when(context.currentParallelism()).thenReturn(readerIds.length);
        doAnswer(
                        invocation -> {
                            pollTask.set(invocation.getArgument(0));
                            pollCallback.set(invocation.getArgument(1));
                            return null;
                        })
                .when(context)
                .callAsync(any(Callable.class), any(BiConsumer.class), anyLong(), anyLong());
        return context;
    }

    private static Map<Integer, ReaderInfo> registeredReaders(int... readerIds) {
        Map<Integer, ReaderInfo> readers = new LinkedHashMap<>();
        Arrays.stream(readerIds).forEach(id -> readers.put(id, new ReaderInfo(id, "reader-" + id)));
        return readers;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void invoke(Callable callable, BiConsumer callback) {
        Object result;
        try {
            result = callable.call();
        } catch (Throwable error) {
            callback.accept(null, error);
            return;
        }
        callback.accept(result, null);
    }

    private static DorisSnapshotSplit snapshot(String splitId) {
        return new DorisSnapshotSplit(
                splitId,
                new PartitionDefinition(
                        "db", "table", "be:9060", Collections.singleton(1L), "plan"));
    }

    private static final class TestingDorisSourceSplitAssigner extends DorisSourceSplitAssigner {
        private TestingDorisSourceSplitAssigner(
                DorisOptions options,
                DorisReadOptions readOptions,
                int sourceParallelism,
                Supplier<String> timestampSupplier,
                List<DorisSnapshotSplit> snapshots) {
            super(options, readOptions, sourceParallelism, timestampSupplier, snapshots);
        }

        private TestingDorisSourceSplitAssigner(
                DorisOptions options,
                DorisReadOptions readOptions,
                int sourceParallelism,
                Supplier<String> timestampSupplier,
                DorisSourceCheckpoint checkpoint) {
            super(options, readOptions, checkpoint, sourceParallelism, timestampSupplier);
        }
    }
}
