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
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.source.DorisSourceScanMode;
import org.apache.doris.flink.source.assigners.DorisSourceSplitAssigner;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> pollTask =
                new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> pollCallback =
                new AtomicReference<>();
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
    void initialTransitionsWhenSnapshotIsEmpty() {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> pollTask =
                new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> pollCallback =
                new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(pollTask, pollCallback, 0);
        AtomicInteger timestampCalls = new AtomicInteger();
        DorisSourceEnumerator enumerator =
                enumerator(
                        context,
                        DorisSourceScanMode.INITIAL,
                        () ->
                                timestampCalls.getAndIncrement() == 0
                                        ? "2026-07-20 10:00:00"
                                        : "2026-07-20 10:00:10",
                        Collections.emptyList());

        enumerator.start();
        enumerator.handleSplitRequest(0, "reader-0");
        DorisSourceCheckpoint transitionCheckpoint = enumerator.snapshotState(1L);

        assertThat(transitionCheckpoint.getPhase()).isEqualTo(DorisSourceCheckpoint.Phase.STREAM);
        assertThat(transitionCheckpoint.getPendingSplits()).isEmpty();
        verify(context, never())
                .callAsync(any(Callable.class), any(BiConsumer.class), anyLong(), anyLong());

        enumerator.notifyCheckpointComplete(1L);
        verify(context).callAsync(any(Callable.class), any(BiConsumer.class), eq(0L), eq(10_000L));
        invoke(pollTask.get(), pollCallback.get());

        verify(context).assignSplit(isA(DorisStreamSplit.class), eq(0));
        assertThat(timestampCalls).hasValue(2);
    }

    @Test
    void olderCheckpointCompletionDoesNotTriggerTransition() {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> pollTask =
                new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> pollCallback =
                new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(pollTask, pollCallback, 0);
        DorisSnapshotSplit snapshot = snapshot("snapshot-older-checkpoint");
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

        enumerator.notifyCheckpointComplete(6L);

        verify(context, never())
                .callAsync(any(Callable.class), any(BiConsumer.class), anyLong(), anyLong());

        enumerator.notifyCheckpointComplete(7L);
        verify(context).callAsync(any(Callable.class), any(BiConsumer.class), eq(0L), eq(10_000L));
    }

    @Test
    void skipsReadersWhoseTabletGroupIsEmpty() {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> pollTask =
                new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> pollCallback =
                new AtomicReference<>();
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
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> pollTask =
                new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> pollCallback =
                new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(pollTask, pollCallback, 0);
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:00",
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
    void restoresSnapshotCheckpointWithoutPlanning() {
        SplitEnumeratorContext<DorisSourceSplit> context = synchronousContext(0);
        DorisSnapshotSplit snapshot = snapshot("snapshot-restored");
        AtomicInteger timestampCalls = new AtomicInteger();
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.SNAPSHOT,
                        "2026-07-20 10:00:00",
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
    }

    @Test
    void restoredPendingStreamSplitIsAssignedBeforeDiscovery() {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> pollTask =
                new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> pollCallback =
                new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(pollTask, pollCallback, 0);
        DorisStreamSplit pending =
                DorisStreamSplit.of(
                        0,
                        "2026-07-20 10:00:00",
                        "2026-07-20 10:00:10",
                        Collections.singleton(100L));
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:10",
                        Collections.singletonList(pending));
        AtomicInteger timestampCalls = new AtomicInteger();
        DorisSourceEnumerator enumerator =
                restoredEnumerator(
                        context,
                        DorisSourceScanMode.LATEST,
                        () -> {
                            timestampCalls.incrementAndGet();
                            return "2026-07-20 10:00:20";
                        },
                        checkpoint);

        enumerator.start();
        enumerator.handleSplitRequest(0, "reader-0");

        verify(context).assignSplit(pending, 0);
        assertThat(timestampCalls).hasValue(0);

        invoke(pollTask.get(), pollCallback.get());
        assertThat(timestampCalls).hasValue(0);
        DorisSourceCheckpoint assigned = enumerator.snapshotState(1L);
        assertThat(assigned.getPendingSplits()).isEmpty();
        assertThat(assigned.getNextStreamStartTimestamp()).isEqualTo("2026-07-20 10:00:10");
    }

    @Test
    void rescaledRestoreDrainsPendingAndReaderSplitsBeforeDiscovery() throws Exception {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> task = new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> callback =
                new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(task, callback, 0, 1);
        DorisStreamSplit first =
                DorisStreamSplit.of(
                        0,
                        "2026-07-20 10:00:00",
                        "2026-07-20 10:00:10",
                        Collections.singleton(100L));
        DorisStreamSplit second =
                DorisStreamSplit.of(
                        1,
                        "2026-07-20 10:00:00",
                        "2026-07-20 10:00:10",
                        Collections.singleton(102L));
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:10",
                        Arrays.asList(first, second));
        AtomicInteger discoveries = new AtomicInteger();
        DorisSourceSplitAssigner assigner =
                DorisSourceSplitAssigner.restoreForTesting(
                        readOptions(DorisSourceScanMode.LATEST, null),
                        checkpoint,
                        2,
                        () -> {
                            discoveries.incrementAndGet();
                            return "2026-07-20 10:00:20";
                        },
                        (startTimestamp, endTimestamp) -> Arrays.asList(100L, 102L, 103L));
        DorisSourceEnumerator enumerator = new DorisSourceEnumerator(context, assigner, 1000L);
        enumerator.start();
        enumerator.handleSplitRequest(0, null);
        verify(context).assignSplit(first, 0);
        invoke(task.get(), callback.get());
        assertThat(discoveries).hasValue(0);
        enumerator.handleSplitRequest(0, null);
        verify(context).assignSplit(second, 0);
        enumerator.handleSplitRequest(0, null);
        // Reader 1 still owns work restored by Flink, although the pending queue is empty.
        invoke(task.get(), callback.get());
        assertThat(discoveries).hasValue(0);
        assertThat(enumerator.snapshotState(1L).getPendingSplits()).isEmpty();
        enumerator.handleSplitRequest(1, null);
        invoke(task.get(), callback.get());
        assertThat(discoveries).hasValue(1);
        assertThat(enumerator.snapshotState(2L).getNextStreamStartTimestamp())
                .isEqualTo("2026-07-20 10:00:20");
    }

    @Test
    void advancesStreamCursorWhenSplitIsPlanned() {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> pollTask =
                new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> pollCallback =
                new AtomicReference<>();
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
        DorisStreamSplit first =
                DorisStreamSplit.of(
                        0,
                        "2026-07-20 10:00:00",
                        "2026-07-20 10:00:10",
                        Collections.singleton(100L));
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:10",
                        Collections.singletonList(first));
        DorisSourceSplitAssigner assigner =
                DorisSourceSplitAssigner.restore(
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
    void failsWhenDorisTimestampMovesBackwards() {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> pollTask =
                new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> pollCallback =
                new AtomicReference<>();
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

    @Test
    void waitsForEveryReaderBeforeDiscoveringAnotherWindow() {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> task = new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> callback =
                new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(task, callback, 0, 1);
        AtomicInteger discoveries = new AtomicInteger();
        AtomicReference<String> end = new AtomicReference<>("2026-07-20 10:00:10");
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:00",
                        Collections.emptyList());
        DorisSourceSplitAssigner assigner =
                DorisSourceSplitAssigner.restoreForTesting(
                        readOptions(DorisSourceScanMode.LATEST, null),
                        checkpoint,
                        2,
                        end::get,
                        (startTimestamp, endTimestamp) -> {
                            discoveries.incrementAndGet();
                            return Arrays.asList(100L, 103L);
                        });
        DorisSourceEnumerator enumerator = new DorisSourceEnumerator(context, assigner, 1000L);
        enumerator.start();
        enumerator.handleSplitRequest(0, null);
        invoke(task.get(), callback.get());
        assertThat(discoveries).hasValue(0);
        enumerator.handleSplitRequest(1, null);
        invoke(task.get(), callback.get());
        assertThat(discoveries).hasValue(1);
        ArgumentCaptor<DorisSourceSplit> splits = ArgumentCaptor.forClass(DorisSourceSplit.class);
        verify(context).assignSplit(splits.capture(), eq(0));
        assertThat(((DorisStreamSplit) splits.getValue()).getTabletIds()).containsExactly(100L);
        verify(context).assignSplit(splits.capture(), eq(1));
        assertThat(((DorisStreamSplit) splits.getValue()).getTabletIds()).containsExactly(103L);
        end.set("2026-07-20 10:00:20");
        enumerator.handleSplitRequest(0, null);
        invoke(task.get(), callback.get());
        assertThat(discoveries).hasValue(1);
        enumerator.handleSplitRequest(1, null);
        invoke(task.get(), callback.get());
        assertThat(discoveries).hasValue(2);
        verify(context, times(2)).assignSplit(splits.capture(), eq(0));
        DorisStreamSplit next = (DorisStreamSplit) splits.getValue();
        assertThat(next.getStartTimestamp()).isEqualTo("2026-07-20 10:00:10");
        assertThat(next.getEndTimestamp()).isEqualTo("2026-07-20 10:00:20");
        assertThat(next.getTabletIds()).containsExactly(100L);
    }

    @Test
    void staleSkippedCallbackCannotStartADuplicateDiscovery() throws Exception {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> task = new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> callback =
                new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(task, callback, 0);
        AtomicInteger calls = new AtomicInteger();
        DorisSourceEnumerator enumerator =
                enumerator(
                        context,
                        DorisSourceScanMode.LATEST,
                        () ->
                                calls.getAndIncrement() == 0
                                        ? "2026-07-20 10:00:00"
                                        : "2026-07-20 10:00:10",
                        Collections.emptyList());
        enumerator.start();
        assertThat(task.get().call()).isNull();
        enumerator.handleSplitRequest(0, null);
        Tuple2<String, Collection<Long>> discovered = task.get().call();
        // An earlier skipped poll's handler can arrive while this result is waiting in the
        // coordinator queue.
        callback.get().accept(null, null);
        assertThat(task.get().call()).isNull();
        assertThat(calls).hasValue(2);
        callback.get().accept(discovered, null);
        verify(context).assignSplit(isA(DorisStreamSplit.class), eq(0));
    }

    @Test
    void replaysOlderWindowsBeforeApplyingDiscoveryOrPublishingOffsets() throws Exception {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> task = new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> callback =
                new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(task, callback, 0);
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:30",
                        Collections.emptyList());
        DorisSourceSplitAssigner assigner =
                DorisSourceSplitAssigner.restoreForTesting(
                        readOptions(DorisSourceScanMode.LATEST, null),
                        checkpoint,
                        1,
                        () -> "2026-07-20 10:00:40",
                        (start, end) -> Collections.singleton(100L));
        DorisOffsetPublisher publisher = mock(DorisOffsetPublisher.class);
        DorisSourceEnumerator enumerator =
                new DorisSourceEnumerator(context, assigner, 1000L, publisher);
        enumerator.start();
        enumerator.handleSplitRequest(0, null);
        enumerator.snapshotState(7L);
        Tuple2<String, Collection<Long>> discovered = task.get().call();

        DorisStreamSplit first =
                DorisStreamSplit.of(
                        0,
                        "2026-07-20 10:00:00",
                        "2026-07-20 10:00:10",
                        Collections.singleton(100L));
        DorisStreamSplit second =
                DorisStreamSplit.of(
                        1,
                        "2026-07-20 10:00:10",
                        "2026-07-20 10:00:20",
                        Collections.singleton(100L));
        DorisStreamSplit third =
                DorisStreamSplit.of(
                        0,
                        "2026-07-20 10:00:20",
                        "2026-07-20 10:00:30",
                        Collections.singleton(100L));
        enumerator.addSplitsBack(Arrays.asList(second, first, third), 0);
        enumerator.handleSplitRequest(0, null);
        assertThat(assigner.getNextStreamStartTimestamp()).isEqualTo("2026-07-20 10:00:30");
        assertThat(assigner.snapshotState(8L).getPendingSplits()).containsExactly(second, third);
        enumerator.notifyCheckpointComplete(7L);

        enumerator.handleSplitRequest(0, null);
        enumerator.handleSplitRequest(0, null);
        enumerator.snapshotState(9L);
        enumerator.notifyCheckpointComplete(9L);
        verify(publisher, never()).publish(any());
        // All queued work is assigned, but the last replayed split is still being read.
        assertThat(task.get().call()).isNull();
        callback.get().accept(discovered, null);
        assertThat(assigner.getNextStreamStartTimestamp()).isEqualTo("2026-07-20 10:00:30");

        enumerator.handleSplitRequest(0, null);
        enumerator.snapshotState(10L);
        enumerator.notifyCheckpointComplete(10L);
        verify(publisher).publish("2026-07-20 10:00:30");
        invoke(task.get(), callback.get());
        ArgumentCaptor<DorisSourceSplit> assigned = ArgumentCaptor.forClass(DorisSourceSplit.class);
        verify(context, times(4)).assignSplit(assigned.capture(), eq(0));
        assertThat(assigned.getAllValues()).hasSize(4).startsWith(first, second, third);
        DorisStreamSplit next = (DorisStreamSplit) assigned.getAllValues().get(3);
        assertThat(next.getStartTimestamp()).isEqualTo("2026-07-20 10:00:30");
        assertThat(next.getEndTimestamp()).isEqualTo("2026-07-20 10:00:40");
        assertThat(next.getTabletIds()).containsExactly(100L);
        enumerator.close();
    }

    @Test
    void publishesTheCompletedWindowCapturedByTheCheckpoint() throws Exception {
        AtomicReference<Callable<Tuple2<String, Collection<Long>>>> task = new AtomicReference<>();
        AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> callback =
                new AtomicReference<>();
        SplitEnumeratorContext<DorisSourceSplit> context =
                controlledPeriodicContext(task, callback, 0, 1);
        AtomicReference<String> end = new AtomicReference<>("2026-07-20 10:00:10");
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:00",
                        Collections.emptyList());
        DorisSourceSplitAssigner assigner =
                DorisSourceSplitAssigner.restoreForTesting(
                        readOptions(DorisSourceScanMode.LATEST, null),
                        checkpoint,
                        2,
                        end::get,
                        (startTimestamp, endTimestamp) ->
                                Arrays.asList(
                                        100L, 101L, 102L, 103L, 104L, 105L, 106L, 107L, 108L,
                                        109L));
        DorisOffsetPublisher publisher = mock(DorisOffsetPublisher.class);
        DorisSourceEnumerator enumerator =
                new DorisSourceEnumerator(context, assigner, 1000L, publisher);
        enumerator.start();
        enumerator.handleSplitRequest(0, null);
        enumerator.handleSplitRequest(1, null);
        invoke(task.get(), callback.get());

        enumerator.handleSplitRequest(0, null);
        enumerator.snapshotState(6L);
        enumerator.notifyCheckpointComplete(6L);
        verify(publisher).publish("2026-07-20 10:00:00");

        enumerator.handleSplitRequest(1, null);
        enumerator.snapshotState(7L);
        end.set("2026-07-20 10:00:20");
        invoke(task.get(), callback.get());
        enumerator.handleSplitRequest(0, null);
        enumerator.handleSplitRequest(1, null);
        enumerator.snapshotState(8L);
        enumerator.notifyCheckpointAborted(8L);

        enumerator.notifyCheckpointComplete(7L);
        enumerator.notifyCheckpointComplete(6L);
        enumerator.notifyCheckpointComplete(8L);
        verify(publisher).publish("2026-07-20 10:00:10");
        verify(publisher, times(2)).publish(any());

        enumerator.snapshotState(9L);
        enumerator.notifyCheckpointComplete(9L);
        verify(publisher).publish("2026-07-20 10:00:20");
        enumerator.close();
        verify(publisher).close();
    }

    @Test
    void reconfirmsCompletedWindowAfterReturnedSplitsAreReplayed() throws Exception {
        SplitEnumeratorContext<DorisSourceSplit> context = synchronousContext(0);
        DorisStreamSplit split =
                DorisStreamSplit.of(
                        0,
                        "2026-07-20 10:00:00",
                        "2026-07-20 10:00:10",
                        Collections.singleton(100L));
        DorisSourceCheckpoint checkpoint =
                new DorisSourceCheckpoint(
                        DorisSourceCheckpoint.Phase.STREAM,
                        "2026-07-20 10:00:10",
                        Collections.singletonList(split));
        DorisOffsetPublisher publisher = mock(DorisOffsetPublisher.class);
        DorisSourceSplitAssigner assigner =
                DorisSourceSplitAssigner.restoreForTesting(
                        readOptions(DorisSourceScanMode.LATEST, null),
                        checkpoint,
                        1,
                        () -> "2026-07-20 10:00:10",
                        (startTimestamp, endTimestamp) -> Collections.singleton(100L));
        DorisSourceEnumerator enumerator =
                new DorisSourceEnumerator(context, assigner, 1000L, publisher);
        enumerator.handleSplitRequest(0, null);
        enumerator.handleSplitRequest(0, null);
        enumerator.snapshotState(7L);

        enumerator.addSplitsBack(Collections.singletonList(split), 0);
        enumerator.snapshotState(8L);
        enumerator.notifyCheckpointComplete(7L);
        enumerator.notifyCheckpointComplete(8L);
        verify(publisher, never()).publish(any());

        enumerator.handleSplitRequest(0, null);
        enumerator.handleSplitRequest(0, null);
        enumerator.snapshotState(9L);
        enumerator.notifyCheckpointComplete(9L);
        verify(publisher).publish("2026-07-20 10:00:10");
        enumerator.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void serializesOffsetWritesAndCoalescesNewerCompletedCheckpoints(boolean failFirstWrite)
            throws Exception {
        SplitEnumeratorContext<DorisSourceSplit> context = synchronousContext(0);
        Deque<Runnable> writes = captureOffsetWrites(context);
        DorisSourceSplitAssigner assigner = mock(DorisSourceSplitAssigner.class);
        when(assigner.getPhase()).thenReturn(DorisSourceCheckpoint.Phase.STREAM);
        when(assigner.getNext(0)).thenReturn(Optional.empty());
        when(assigner.getNextStreamStartTimestamp())
                .thenReturn("2026-07-20 10:00:10", "2026-07-20 10:00:20", "2026-07-20 10:00:30");
        DorisOffsetPublisher publisher = mock(DorisOffsetPublisher.class);
        if (failFirstWrite) {
            org.mockito.Mockito.doThrow(new java.sql.SQLException("unavailable"))
                    .when(publisher)
                    .publish("2026-07-20 10:00:10");
        }
        DorisSourceEnumerator enumerator =
                new DorisSourceEnumerator(context, assigner, 1000L, publisher);

        for (long checkpointId = 1; checkpointId <= 3; checkpointId++) {
            enumerator.handleSplitRequest(0, null);
            enumerator.snapshotState(checkpointId);
            enumerator.notifyCheckpointComplete(checkpointId);
        }
        assertThat(writes).hasSize(1);
        verify(publisher, never()).publish(any());
        writes.remove().run();
        assertThat(writes).hasSize(1);
        writes.remove().run();
        assertThat(writes).isEmpty();
        org.mockito.InOrder order = org.mockito.Mockito.inOrder(publisher);
        order.verify(publisher).publish("2026-07-20 10:00:10");
        order.verify(publisher).publish("2026-07-20 10:00:30");
        verify(publisher, times(2)).publish(any());
        enumerator.close();
    }

    @Test
    void retriesFailedPublicationOnLaterCheckpointAndSkipsQueuedWritesAfterClose()
            throws Exception {
        SplitEnumeratorContext<DorisSourceSplit> context = synchronousContext(0);
        Deque<Runnable> writes = captureOffsetWrites(context);
        DorisSourceSplitAssigner assigner = mock(DorisSourceSplitAssigner.class);
        when(assigner.getPhase()).thenReturn(DorisSourceCheckpoint.Phase.STREAM);
        when(assigner.getNext(0)).thenReturn(Optional.empty());
        when(assigner.getNextStreamStartTimestamp()).thenReturn("2026-07-20 10:00:10");
        DorisOffsetPublisher publisher = mock(DorisOffsetPublisher.class);
        org.mockito.Mockito.doThrow(new java.sql.SQLException("unavailable"))
                .doNothing()
                .when(publisher)
                .publish("2026-07-20 10:00:10");
        DorisSourceEnumerator enumerator =
                new DorisSourceEnumerator(context, assigner, 1000L, publisher);
        enumerator.handleSplitRequest(0, null);
        enumerator.snapshotState(1L);
        enumerator.notifyCheckpointComplete(1L);
        writes.remove().run();
        assertThat(writes).isEmpty();

        enumerator.snapshotState(2L);
        enumerator.notifyCheckpointComplete(2L);
        writes.remove().run();
        verify(publisher, times(2)).publish("2026-07-20 10:00:10");

        enumerator.snapshotState(3L);
        enumerator.notifyCheckpointComplete(3L);
        assertThat(writes).hasSize(1);
        enumerator.close();
        writes.remove().run();
        verify(publisher, times(2)).publish(any());
        verify(publisher).close();
        assertThat(writes).isEmpty();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Deque<Runnable> captureOffsetWrites(
            SplitEnumeratorContext<DorisSourceSplit> context) {
        Deque<Runnable> writes = new ArrayDeque<>();
        doAnswer(
                        invocation -> {
                            Callable callable = invocation.getArgument(0);
                            BiConsumer callback = invocation.getArgument(1);
                            writes.add(() -> invoke(callable, callback));
                            return null;
                        })
                .when(context)
                .callAsync(any(Callable.class), any(BiConsumer.class));
        return writes;
    }

    private static DorisSourceEnumerator enumerator(
            SplitEnumeratorContext<DorisSourceSplit> context,
            DorisSourceScanMode mode,
            Supplier<String> timestampSupplier,
            List<DorisSnapshotSplit> snapshots) {
        DorisReadOptions testReadOptions = readOptions(mode, null);
        DorisSourceSplitAssigner splitAssigner =
                DorisSourceSplitAssigner.createForTesting(
                        testReadOptions,
                        context.currentParallelism(),
                        timestampSupplier,
                        snapshots,
                        (startTimestamp, endTimestamp) -> Collections.singleton(0L));
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
                DorisSourceSplitAssigner.restoreForTesting(
                        testReadOptions,
                        checkpoint,
                        context.currentParallelism(),
                        timestampSupplier,
                        (startTimestamp, endTimestamp) -> Collections.singleton(0L));
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
                            invoke(invocation.getArgument(0), invocation.getArgument(1));
                            return null;
                        })
                .when(context)
                .callAsync(any(Callable.class), any(BiConsumer.class));
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
            AtomicReference<Callable<Tuple2<String, Collection<Long>>>> pollTask,
            AtomicReference<BiConsumer<Tuple2<String, Collection<Long>>, Throwable>> pollCallback,
            int... readerIds) {
        SplitEnumeratorContext<DorisSourceSplit> context = mock(SplitEnumeratorContext.class);
        Map<Integer, ReaderInfo> readers = registeredReaders(readerIds);
        when(context.registeredReaders()).thenReturn(readers);
        when(context.currentParallelism()).thenReturn(readerIds.length);
        doAnswer(
                        invocation -> {
                            invoke(invocation.getArgument(0), invocation.getArgument(1));
                            return null;
                        })
                .when(context)
                .callAsync(any(Callable.class), any(BiConsumer.class));
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
}
