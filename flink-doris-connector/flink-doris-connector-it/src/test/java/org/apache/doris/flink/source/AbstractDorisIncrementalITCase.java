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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.container.AbstractContainerTestBase;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/** Shared support for Doris incremental Source integration cases. */
public abstract class AbstractDorisIncrementalITCase extends AbstractITCaseService {
    protected static final String DATABASE = "test_incremental_source";
    protected static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);
    protected static final Duration CONTINUOUS_CHECKPOINT_INTERVAL = Duration.ofSeconds(1);
    protected static final Duration MANUAL_CHECKPOINT_INTERVAL = Duration.ofMinutes(10);

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDorisIncrementalITCase.class);

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @BeforeClass
    public static void initContainers() {
        Assume.assumeTrue(
                "Doris incremental source cases require -Dcustomer_env=true",
                Boolean.getBoolean("customer_env"));
        AbstractContainerTestBase.initContainers();
    }

    protected void initializeIncrementalTable(String table, String... values) {
        List<String> statements = new ArrayList<>();
        statements.add(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE));
        statements.add(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table));
        statements.add(
                String.format(
                        "CREATE TABLE %s.%s (\n"
                                + "  `id` INT,\n"
                                + "  `name` VARCHAR(128),\n"
                                + "  `ignored_col` VARCHAR(128)\n"
                                + ") UNIQUE KEY(`id`)\n"
                                + "DISTRIBUTED BY HASH(`id`) BUCKETS 2\n"
                                + "PROPERTIES (\n"
                                + "  \"replication_num\" = \"1\",\n"
                                + "  \"enable_unique_key_merge_on_write\" = \"true\",\n"
                                + "  \"binlog.enable\" = \"true\",\n"
                                + "  \"binlog.format\" = \"ROW\",\n"
                                + "  \"binlog.need_historical_value\" = \"true\"\n"
                                + ")",
                        DATABASE, table));
        if (values.length > 0) {
            statements.add(
                    String.format(
                            "INSERT INTO %s.%s VALUES %s",
                            DATABASE, table, String.join(",", values)));
        }
        try (Connection connection = getDorisQueryConnection()) {
            ContainerUtils.executeSQLStatement(connection, LOG, statements.toArray(new String[0]));
        } catch (SQLException e) {
            throw new DorisRuntimeException("Failed to close Doris query connection", e);
        }
    }

    protected void executeDorisSql(String... statements) {
        try (Connection connection = getDorisQueryConnection()) {
            ContainerUtils.executeSQLStatement(connection, LOG, statements);
        } catch (SQLException e) {
            throw new DorisRuntimeException("Failed to close Doris query connection", e);
        }
    }

    protected String resolveCurrentDorisTimestamp(String table) {
        return RestService.resolveCurrentTimestamp(
                dorisOptions(table), incrementalReadOptions(), LOG);
    }

    protected String waitForDorisTimestampAfter(String table, String timestamp) throws Exception {
        AtomicReference<String> currentTimestamp = new AtomicReference<>();
        waitUntilCondition(
                () -> {
                    String current = resolveCurrentDorisTimestamp(table);
                    currentTimestamp.set(current);
                    return current.compareTo(timestamp) > 0;
                },
                Deadline.fromNow(DEFAULT_TIMEOUT),
                100L,
                "Doris timestamp did not advance beyond " + timestamp);
        return currentTimestamp.get();
    }

    protected IncrementalResultCollector startSource(
            String table, String scanMode, String scanTimestamp) throws Exception {
        return startSource(
                table,
                scanMode,
                scanTimestamp,
                new Configuration(),
                CONTINUOUS_CHECKPOINT_INTERVAL);
    }

    protected IncrementalResultCollector startSource(
            String table,
            String scanMode,
            String scanTimestamp,
            Configuration configuration,
            Duration checkpointInterval)
            throws Exception {
        return startSource(
                table, scanMode, scanTimestamp, configuration, checkpointInterval, null, null);
    }

    protected IncrementalResultCollector startSourceWithOffsetPersistence(
            String table,
            String scanMode,
            String scanTimestamp,
            String offsetTable,
            String consumerId)
            throws Exception {
        return startSource(
                table,
                scanMode,
                scanTimestamp,
                new Configuration(),
                MANUAL_CHECKPOINT_INTERVAL,
                offsetTable,
                consumerId);
    }

    private IncrementalResultCollector startSource(
            String table,
            String scanMode,
            String scanTimestamp,
            Configuration configuration,
            Duration checkpointInterval,
            String offsetTable,
            String consumerId)
            throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(DEFAULT_PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(checkpointInterval.toMillis());
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.executeSql(
                sourceDdl(table, scanMode, scanTimestamp, offsetTable, consumerId));
        TableResult result =
                tableEnvironment.executeSql("SELECT id, name FROM doris_incremental_source");
        IncrementalResultCollector collector = new IncrementalResultCollector(result);
        waitForJobStatus(
                collector.getJobClient(),
                Collections.singletonList(JobStatus.RUNNING),
                Deadline.fromNow(DEFAULT_TIMEOUT));
        waitForAllTasksRunning(collector.getJobClient());
        return collector;
    }

    protected void completeCheckpoint(JobClient jobClient) throws Exception {
        waitForAllTasksRunning(jobClient);
        miniClusterResource
                .getMiniCluster()
                .triggerCheckpoint(jobClient.getJobID())
                .get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    protected void waitForAllTasksRunning(JobClient jobClient) throws Exception {
        CommonTestUtils.waitForAllTaskRunning(
                miniClusterResource.getMiniCluster(), jobClient.getJobID(), false);
    }

    protected void checkpointUntilContains(
            IncrementalResultCollector collector, List<ObservedRow> expected) throws Exception {
        waitUntilCondition(
                () -> {
                    completeCheckpoint(collector.getJobClient());
                    return collector.containsAll(expected);
                },
                Deadline.fromNow(DEFAULT_TIMEOUT),
                100L,
                "Missing source rows " + expected + "; observed=" + collector.getRows());
    }

    protected DorisOptions dorisOptions(String table) {
        return DorisOptions.builder()
                .setFenodes(getFenodes())
                .setTableIdentifier(DATABASE + "." + table)
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword())
                .build();
    }

    private DorisReadOptions incrementalReadOptions() {
        return DorisReadOptions.builder()
                .setUseFlightSql(true)
                .setFlightSqlPort(getFlightSqlPort())
                .build();
    }

    private int getFlightSqlPort() {
        return Integer.getInteger("doris_flight_sql_port", 9611);
    }

    private String sourceDdl(
            String table,
            String scanMode,
            String scanTimestamp,
            String offsetTable,
            String consumerId) {
        String offsetOptions =
                offsetTable == null
                        ? ""
                        : String.format(
                                ",\n"
                                        + "  'jdbc-url' = '%s',\n"
                                        + "  'source.binlog.offset-table' = '%s.%s',\n"
                                        + "  'source.binlog.consumer-id' = '%s'",
                                escapeSqlLiteral(getDorisQueryUrl()),
                                DATABASE,
                                offsetTable,
                                escapeSqlLiteral(consumerId));
        if (scanTimestamp == null) {
            return String.format(
                    "CREATE TABLE doris_incremental_source (\n"
                            + "  id INT,\n"
                            + "  name STRING,\n"
                            + "  ignored_col STRING,\n"
                            + "  PRIMARY KEY (id) NOT ENFORCED\n"
                            + ") WITH (\n"
                            + "  'connector' = '%s',\n"
                            + "  'fenodes' = '%s',\n"
                            + "  'table.identifier' = '%s.%s',\n"
                            + "  'username' = '%s',\n"
                            + "  'password' = '%s',\n"
                            + "  'source.scan.mode' = '%s',\n"
                            + "  'source.use-flight-sql' = 'true',\n"
                            + "  'source.flight-sql-port' = '%d',\n"
                            + "  'source.binlog.increment-type' = 'detail',\n"
                            + "  'source.binlog.poll-interval' = '1s'%s\n"
                            + ")",
                    DorisConfigOptions.IDENTIFIER,
                    escapeSqlLiteral(getFenodes()),
                    DATABASE,
                    table,
                    escapeSqlLiteral(getDorisUsername()),
                    escapeSqlLiteral(getDorisPassword()),
                    escapeSqlLiteral(scanMode),
                    getFlightSqlPort(),
                    offsetOptions);
        }

        return String.format(
                "CREATE TABLE doris_incremental_source (\n"
                        + "  id INT,\n"
                        + "  name STRING,\n"
                        + "  ignored_col STRING,\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = '%s',\n"
                        + "  'fenodes' = '%s',\n"
                        + "  'table.identifier' = '%s.%s',\n"
                        + "  'username' = '%s',\n"
                        + "  'password' = '%s',\n"
                        + "  'source.scan.mode' = '%s',\n"
                        + "  'source.scan.timestamp' = '%s',\n"
                        + "  'source.use-flight-sql' = 'true',\n"
                        + "  'source.flight-sql-port' = '%d',\n"
                        + "  'source.binlog.increment-type' = 'detail',\n"
                        + "  'source.binlog.poll-interval' = '1s'%s\n"
                        + ")",
                DorisConfigOptions.IDENTIFIER,
                escapeSqlLiteral(getFenodes()),
                DATABASE,
                table,
                escapeSqlLiteral(getDorisUsername()),
                escapeSqlLiteral(getDorisPassword()),
                escapeSqlLiteral(scanMode),
                escapeSqlLiteral(scanTimestamp),
                getFlightSqlPort(),
                offsetOptions);
    }

    private static String escapeSqlLiteral(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    protected static final class ObservedRow {
        private final RowKind rowKind;
        private final List<Object> fields;

        private ObservedRow(RowKind rowKind, List<Object> fields) {
            this.rowKind = rowKind;
            this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        }

        static ObservedRow from(Row row) {
            List<Object> fields = new ArrayList<>(row.getArity());
            for (int i = 0; i < row.getArity(); i++) {
                fields.add(row.getField(i));
            }
            return new ObservedRow(row.getKind(), fields);
        }

        static ObservedRow of(RowKind rowKind, Object... fields) {
            return new ObservedRow(rowKind, Arrays.asList(fields));
        }

        RowKind getRowKind() {
            return rowKind;
        }

        int getArity() {
            return fields.size();
        }

        int getInt(int pos) {
            return ((Number) fields.get(pos)).intValue();
        }

        String getString(int pos) {
            return String.valueOf(fields.get(pos));
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof ObservedRow)) {
                return false;
            }
            ObservedRow that = (ObservedRow) object;
            return rowKind == that.rowKind && Objects.equals(fields, that.fields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowKind, fields);
        }

        @Override
        public String toString() {
            return rowKind + fields.toString();
        }
    }

    protected static final class IncrementalResultCollector implements AutoCloseable {
        private final JobClient jobClient;
        private final CloseableIterator<Row> iterator;
        private final CopyOnWriteArrayList<ObservedRow> rows = new CopyOnWriteArrayList<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicBoolean closing = new AtomicBoolean();
        private final ExecutorService executor;

        private IncrementalResultCollector(TableResult tableResult) {
            this.jobClient =
                    tableResult
                            .getJobClient()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Incremental query has no JobClient"));
            this.iterator = tableResult.collect();
            this.executor =
                    Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread =
                                        new Thread(runnable, "doris-incremental-result-collector");
                                thread.setDaemon(true);
                                return thread;
                            });
            executor.submit(this::collectRows);
        }

        JobClient getJobClient() {
            return jobClient;
        }

        List<ObservedRow> getRows() {
            return new ArrayList<>(rows);
        }

        ObservedRow awaitFirstRow() throws Exception {
            awaitRows(values -> !values.isEmpty(), DEFAULT_TIMEOUT, "No source row was emitted");
            return rows.get(0);
        }

        ObservedRow awaitRow(Predicate<ObservedRow> predicate, String errorMessage)
                throws Exception {
            awaitRows(values -> values.stream().anyMatch(predicate), DEFAULT_TIMEOUT, errorMessage);
            return rows.stream().filter(predicate).findFirst().get();
        }

        void awaitContains(ObservedRow expected) throws Exception {
            awaitRows(
                    values -> values.contains(expected),
                    DEFAULT_TIMEOUT,
                    "Missing source row " + expected);
        }

        void awaitContainsAll(List<ObservedRow> expected) throws Exception {
            awaitRows(
                    values -> values.containsAll(expected),
                    DEFAULT_TIMEOUT,
                    "Missing source rows " + expected);
        }

        boolean containsAll(List<ObservedRow> expected) throws Exception {
            checkFailure();
            return rows.containsAll(expected);
        }

        void awaitRows(
                Predicate<List<ObservedRow>> predicate, Duration timeout, String errorMessage)
                throws Exception {
            waitUntilCondition(
                    () -> {
                        checkFailure();
                        return predicate.test(getRows());
                    },
                    Deadline.fromNow(timeout),
                    100L,
                    errorMessage + "; observed=" + getRows());
            checkFailure();
        }

        private void collectRows() {
            try {
                while (!closing.get() && iterator.hasNext()) {
                    rows.add(ObservedRow.from(iterator.next()));
                }
            } catch (Throwable throwable) {
                if (!closing.get()) {
                    failure.compareAndSet(null, throwable);
                }
            }
        }

        private void checkFailure() throws Exception {
            Throwable throwable = failure.get();
            if (throwable == null) {
                return;
            }
            if (throwable instanceof Exception) {
                throw (Exception) throwable;
            }
            throw new AssertionError("Incremental result collection failed", throwable);
        }

        @Override
        public void close() throws Exception {
            if (!closing.compareAndSet(false, true)) {
                return;
            }

            Exception cleanupFailure = null;
            try {
                JobStatus status =
                        jobClient
                                .getJobStatus()
                                .get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                if (!status.isTerminalState()) {
                    jobClient.cancel().get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }
            } catch (Exception e) {
                cleanupFailure = e;
            }

            try {
                iterator.close();
            } catch (Exception e) {
                if (cleanupFailure == null) {
                    cleanupFailure = e;
                } else {
                    cleanupFailure.addSuppressed(e);
                }
            } finally {
                executor.shutdownNow();
                executor.awaitTermination(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }

            if (cleanupFailure != null) {
                Assert.fail("Failed to close incremental source job: " + cleanupFailure);
            }
        }
    }
}
