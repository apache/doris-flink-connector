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

package org.apache.doris.flink.source;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.doris.flink.source.reader.DorisSourceRecord;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Exercises real tablet queries and Flink state redistribution with controlled reader progress. */
public class DorisParallelIncrementalSourceITCase extends AbstractDorisIncrementalITCase {
    private static final String OFFSET_TABLE = "tbl_parallel_source_offsets";
    private static final int ROW_COUNT = 256;
    // MiniCluster tasks share the test classloader; no test controls are serialized into state.
    private static final Map<String, Probe> PROBES = new ConcurrentHashMap<>();

    @Rule public final TemporaryFolder savepoints = new TemporaryFolder();

    @Test
    public void testParallelChangelogAndNewPartitionWithoutDuplicates() throws Exception {
        String table = "tbl_parallel_changelog";
        initializeParallelTable(table);
        String start = resolveCurrentDorisTimestamp(table);
        insertRows(table, 0);
        List<String> expected = expectedInserts(0);
        try (Probe probe = startProbe(table, start, 2, -1, null)) {
            awaitPublishedThroughWrites(table, probe);
            assertEvents(expected, probe.rows);
            Assert.assertEquals(
                    "Both source readers must actually emit rows", 2, probe.readers.size());

            executeDorisSql(String.format("UPDATE %s.%s SET name = 'updated'", DATABASE, table));
            for (int id = 0; id < ROW_COUNT; id++) {
                expected.add(event(RowKind.UPDATE_BEFORE, id, "row-" + id));
                expected.add(event(RowKind.UPDATE_AFTER, id, "updated"));
            }
            awaitPublishedThroughWrites(table, probe);
            assertEvents(expected, probe.rows);

            executeDorisSql(String.format("DELETE FROM %s.%s WHERE id %% 2 = 0", DATABASE, table));
            for (int id = 0; id < ROW_COUNT; id += 2) {
                expected.add(event(RowKind.DELETE, id, "updated"));
            }
            awaitPublishedThroughWrites(table, probe);
            assertEvents(expected, probe.rows);

            executeDorisSql(
                    String.format(
                            "ALTER TABLE %s.%s ADD PARTITION p1 VALUES LESS THAN (20000)",
                            DATABASE, table));
            insertRows(table, 10000);
            expected.addAll(expectedInserts(10000));
            awaitPublishedThroughWrites(table, probe);
            assertEvents(expected, probe.rows);
        }
    }

    @Test
    public void testOffsetWaitsForSlowReaderAndCompletedCheckpoint() throws Exception {
        String table = "tbl_parallel_slow_reader";
        initializeParallelTable(table);
        String start = resolveCurrentDorisTimestamp(table);
        insertRows(table, 0);
        try (Probe probe = startProbe(table, start, 2, 0, null)) {
            DorisStreamSplit unfinished = awaitPartiallyConsumedWindow(probe);
            waitUntilCondition(
                    () -> publishedOffset(probe) != null,
                    Deadline.fromNow(DEFAULT_TIMEOUT),
                    100L,
                    "No completed checkpoint offset was published");
            Assert.assertTrue(
                    "Offset advanced past an unfinished window",
                    publishedOffset(probe).compareTo(unfinished.getStartTimestamp()) <= 0);
            String previousUpdate = publishedUpdateTime(probe);
            completeCheckpoint(probe.job);
            waitUntilCondition(
                    () -> !previousUpdate.equals(publishedUpdateTime(probe)),
                    Deadline.fromNow(DEFAULT_TIMEOUT),
                    100L,
                    "The second completed checkpoint was not published");
            Assert.assertTrue(
                    "Checkpoint completion must not imply window completion",
                    publishedOffset(probe).compareTo(unfinished.getStartTimestamp()) <= 0);

            probe.resume.complete(null);
            awaitPublishedThroughWrites(table, probe);
            assertEvents(expectedInserts(0), probe.rows);
            Assert.assertTrue(publishedOffset(probe).compareTo(unfinished.getEndTimestamp()) >= 0);
        }
    }

    @Test
    public void testRestoresUnfinishedWindowWithLowerParallelism() throws Exception {
        restoreWithParallelism(1);
    }

    @Test
    public void testRestoresUnfinishedWindowWithHigherParallelism() throws Exception {
        restoreWithParallelism(3);
    }

    private void restoreWithParallelism(int parallelism) throws Exception {
        String table = "tbl_parallel_restore_" + parallelism;
        initializeParallelTable(table);
        String start = resolveCurrentDorisTimestamp(table);
        insertRows(table, 0);
        String savepoint;
        List<String> beforeRestore;
        try (Probe original = startProbe(table, start, 2, 0, null)) {
            awaitPartiallyConsumedWindow(original);
            savepoint =
                    original.job
                            .triggerSavepoint(
                                    savepoints.newFolder().toURI().toString(),
                                    SavepointFormatType.CANONICAL)
                            .get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertFalse(
                    "Savepoint must contain unfinished reader state",
                    original.states.get(0).isEmpty());
            beforeRestore = new ArrayList<>(original.rows);
        }
        try (Probe restored = startProbe(table, start, parallelism, -1, savepoint)) {
            awaitPublishedThroughWrites(table, restored);
            Assert.assertFalse("Restored reader state was not consumed", restored.rows.isEmpty());
            Set<String> allRows = new HashSet<>(beforeRestore);
            allRows.addAll(restored.rows);
            // Unfinished splits are replayed in full; recovery does not promise row-level
            // deduplication.
            Assert.assertEquals(new HashSet<>(expectedInserts(0)), allRows);
            Assert.assertTrue(
                    "Completed reader work was replayed instead of restoring the savepoint",
                    Collections.disjoint(beforeRestore, restored.rows));

            restored.readers.clear();
            insertRows(table, 1000);
            awaitPublishedThroughWrites(table, restored);
            assertEvents(
                    expectedInserts(1000),
                    restored.rows.stream()
                            .filter(row -> extractId(row) >= 1000)
                            .collect(Collectors.toList()));
            Assert.assertEquals(
                    "New windows must use the restored parallelism",
                    parallelism,
                    restored.readers.size());
        }
    }

    private DorisStreamSplit awaitPartiallyConsumedWindow(Probe probe) throws Exception {
        waitUntilCondition(
                () -> {
                    completeCheckpoint(probe.job);
                    return probe.readers.contains(1)
                            && probe.states.containsKey(0)
                            && !probe.states.get(0).isEmpty()
                            && probe.states.containsKey(1)
                            && probe.states.get(1).isEmpty();
                },
                Deadline.fromNow(DEFAULT_TIMEOUT),
                100L,
                "Expected reader 0 to retain a split while reader 1 completed its tablet group");
        Assert.assertFalse("Paused reader unexpectedly emitted data", probe.readers.contains(0));
        Assert.assertTrue(probe.rows.size() < ROW_COUNT);
        return (DorisStreamSplit) probe.states.get(0).get(0);
    }

    private void initializeParallelTable(String table) {
        executeDorisSql(
                String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
                String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
                String.format(
                        "CREATE TABLE %s.%s (id INT, name VARCHAR(128), ignored_col VARCHAR(128)) "
                                + "UNIQUE KEY(id) PARTITION BY RANGE(id) "
                                + "(PARTITION p0 VALUES LESS THAN (10000)) "
                                + "DISTRIBUTED BY HASH(id) BUCKETS 16 "
                                + "PROPERTIES ('replication_num'='1', "
                                + "'enable_unique_key_merge_on_write'='true', 'binlog.enable'='true', "
                                + "'binlog.format'='ROW', 'binlog.need_historical_value'='true')",
                        DATABASE, table),
                String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, OFFSET_TABLE),
                String.format(
                        "CREATE TABLE %s.%s (consumer_id VARCHAR(256) NOT NULL, "
                                + "offset_timestamp DATETIME NOT NULL, update_time DATETIMEV2(3) NOT NULL) "
                                + "UNIQUE KEY(consumer_id) DISTRIBUTED BY HASH(consumer_id) BUCKETS 1 "
                                + "PROPERTIES ('replication_num'='1', 'enable_unique_key_merge_on_write'='true')",
                        DATABASE, OFFSET_TABLE));
    }

    private void insertRows(String table, int firstId) {
        String values =
                IntStream.range(firstId, firstId + ROW_COUNT)
                        .mapToObj(id -> String.format("(%d,'row-%d','ignored')", id, id))
                        .collect(Collectors.joining(","));
        executeDorisSql(String.format("INSERT INTO %s.%s VALUES %s", DATABASE, table, values));
    }

    private Probe startProbe(
            String table, String start, int parallelism, int pausedReader, String savepoint)
            throws Exception {
        Probe probe = new Probe(pausedReader);
        PROBES.put(probe.id, probe);
        try {
            DorisOptions options =
                    DorisOptions.builder()
                            .setFenodes(getFenodes())
                            .setTableIdentifier(DATABASE + "." + table)
                            .setUsername(getDorisUsername())
                            .setPassword(getDorisPassword())
                            .setJdbcUrl(getDorisQueryUrl())
                            .build();
            DorisReadOptions readOptions =
                    DorisReadOptions.builder()
                            .setUseFlightSql(true)
                            .setFlightSqlPort(Integer.getInteger("doris_flight_sql_port", 9611))
                            .setReadFields("id,name")
                            .setScanMode(DorisSourceScanMode.FROM_TIMESTAMP)
                            .setScanTimestamp(start)
                            .setBinlogIncrementType(DorisBinlogIncrementType.DETAIL)
                            .setBinlogPollIntervalMs(1000L)
                            .setBinlogOffsetTable(DATABASE + "." + OFFSET_TABLE)
                            .setBinlogConsumerId(probe.id)
                            .build();
            Configuration configuration = new Configuration();
            if (savepoint != null) {
                SavepointRestoreSettings.toConfiguration(
                        SavepointRestoreSettings.forPath(savepoint), configuration);
            }
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(parallelism);
            env.setMaxParallelism(16);
            env.enableCheckpointing(MANUAL_CHECKPOINT_INTERVAL.toMillis());
            env.fromSource(
                            new ControlledSource(options, readOptions, probe.id),
                            WatermarkStrategy.noWatermarks(),
                            "parallel-binlog-source")
                    .uid("parallel-binlog-source")
                    .sinkTo(new DiscardingSink<>())
                    .uid("discard-binlog-output");
            probe.job = env.executeAsync("parallel-binlog-it");
            waitForJobStatus(
                    probe.job,
                    Collections.singletonList(JobStatus.RUNNING),
                    Deadline.fromNow(DEFAULT_TIMEOUT));
            waitForAllTasksRunning(probe.job);
            return probe;
        } catch (Exception error) {
            try {
                probe.close();
            } catch (Exception cleanup) {
                error.addSuppressed(cleanup);
            }
            throw error;
        }
    }

    private void awaitPublishedThroughWrites(String table, Probe probe) throws Exception {
        String fence = resolveCurrentDorisTimestamp(table);
        waitForDorisTimestampAfter(table, fence);
        waitUntilCondition(
                () -> {
                    completeCheckpoint(probe.job);
                    String offset = publishedOffset(probe);
                    return offset != null && offset.compareTo(fence) > 0;
                },
                Deadline.fromNow(DEFAULT_TIMEOUT),
                100L,
                "Published offset did not pass the writes at " + fence);
    }

    private String publishedOffset(Probe probe) throws Exception {
        return publishedValue(probe, "offset_timestamp");
    }

    private String publishedUpdateTime(Probe probe) throws Exception {
        return publishedValue(probe, "update_time");
    }

    private String publishedValue(Probe probe, String column) throws Exception {
        try (Connection connection = getDorisQueryConnection();
                PreparedStatement statement =
                        connection.prepareStatement(
                                String.format(
                                        "SELECT %s FROM %s.%s WHERE consumer_id = ?",
                                        column, DATABASE, OFFSET_TABLE))) {
            statement.setString(1, probe.id);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getString(1) : null;
            }
        }
    }

    private static List<String> expectedInserts(int firstId) {
        return IntStream.range(firstId, firstId + ROW_COUNT)
                .mapToObj(id -> event(RowKind.INSERT, id, "row-" + id))
                .collect(Collectors.toList());
    }

    private static String event(RowKind kind, int id, String name) {
        return kind.shortString() + ":" + id + ":" + name;
    }

    private static int extractId(String event) {
        return Integer.parseInt(event.split(":", 3)[1]);
    }

    private static void assertEvents(List<String> expected, List<String> actual) {
        Assert.assertEquals(
                expected.stream().collect(Collectors.groupingBy(row -> row, Collectors.counting())),
                actual.stream().collect(Collectors.groupingBy(row -> row, Collectors.counting())));
    }

    private static final class Probe implements AutoCloseable {
        private final String id = UUID.randomUUID().toString();
        private final int pausedReader;
        private final CompletableFuture<Void> resume = new CompletableFuture<>();
        private final List<String> rows = new CopyOnWriteArrayList<>();
        private final Set<Integer> readers = ConcurrentHashMap.newKeySet();
        private final Map<Integer, List<DorisSourceSplit>> states = new ConcurrentHashMap<>();
        private JobClient job;

        private Probe(int pausedReader) {
            this.pausedReader = pausedReader;
        }

        @Override
        public void close() throws Exception {
            try {
                if (job != null) {
                    job.cancel().get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    waitUntilCondition(
                            () -> job.getJobStatus().get().isTerminalState(),
                            Deadline.fromNow(DEFAULT_TIMEOUT),
                            100L,
                            "Incremental source job did not terminate");
                }
            } finally {
                PROBES.remove(id);
            }
        }
    }

    private static final class TrackingDeserializer implements DorisDeserializationSchema<String> {
        private final String probeId;
        private final int readerId;

        private TrackingDeserializer(String probeId, int readerId) {
            this.probeId = probeId;
            this.readerId = readerId;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }

        @Override
        public void deserialize(List<?> record, Collector<String> out) {
            throw new AssertionError("Incremental records must retain their binlog metadata");
        }

        @Override
        public void deserialize(DorisSourceRecord record, Collector<String> out) {
            Assert.assertEquals(
                    "Projection must exclude hidden binlog fields",
                    2,
                    record.getFieldValues().size());
            String value =
                    event(
                            record.getRowKind(),
                            ((Number) record.getFieldValues().get(0)).intValue(),
                            String.valueOf(record.getFieldValues().get(1)));
            out.collect(value);
            Probe probe = PROBES.get(probeId);
            probe.rows.add(value);
            probe.readers.add(readerId);
        }
    }

    private static final class ControlledSource extends DorisSource<String> {
        private final DorisOptions options;
        private final DorisReadOptions readOptions;
        private final String probeId;

        private ControlledSource(
                DorisOptions options, DorisReadOptions readOptions, String probeId) {
            super(
                    options,
                    readOptions,
                    Boundedness.CONTINUOUS_UNBOUNDED,
                    new TrackingDeserializer(probeId, -1));
            this.options = options;
            this.readOptions = readOptions;
            this.probeId = probeId;
        }

        @Override
        public SourceReader<String, DorisSourceSplit> createReader(SourceReaderContext context)
                throws Exception {
            int readerId = context.getIndexOfSubtask();
            SourceReader<String, DorisSourceSplit> delegate =
                    new DorisSource<>(
                                    options,
                                    readOptions,
                                    Boundedness.CONTINUOUS_UNBOUNDED,
                                    new TrackingDeserializer(probeId, readerId))
                            .createReader(context);
            Probe probe = PROBES.get(probeId);
            return new SourceReader<String, DorisSourceSplit>() {
                private boolean paused() {
                    return readerId == probe.pausedReader && !probe.resume.isDone();
                }

                @Override
                public void start() {
                    delegate.start();
                }

                @Override
                public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
                    return paused() ? InputStatus.NOTHING_AVAILABLE : delegate.pollNext(output);
                }

                @Override
                public CompletableFuture<Void> isAvailable() {
                    return paused() ? probe.resume : delegate.isAvailable();
                }

                @Override
                public List<DorisSourceSplit> snapshotState(long checkpointId) {
                    List<DorisSourceSplit> splits = delegate.snapshotState(checkpointId);
                    probe.states.put(readerId, new ArrayList<>(splits));
                    return splits;
                }

                @Override
                public void addSplits(List<DorisSourceSplit> splits) {
                    delegate.addSplits(splits);
                }

                @Override
                public void notifyNoMoreSplits() {
                    delegate.notifyNoMoreSplits();
                }

                @Override
                public void handleSourceEvents(SourceEvent event) {
                    delegate.handleSourceEvents(event);
                }

                @Override
                public void notifyCheckpointComplete(long checkpointId) throws Exception {
                    delegate.notifyCheckpointComplete(checkpointId);
                }

                @Override
                public void notifyCheckpointAborted(long checkpointId) throws Exception {
                    delegate.notifyCheckpointAborted(checkpointId);
                }

                @Override
                public void close() throws Exception {
                    delegate.close();
                }
            };
        }
    }
}
