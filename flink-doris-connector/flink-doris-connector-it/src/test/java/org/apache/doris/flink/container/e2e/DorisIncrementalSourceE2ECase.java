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

package org.apache.doris.flink.container.e2e;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.container.AbstractContainerTestBase;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** End-to-end case for replicating all supported Doris types from incremental records. */
public class DorisIncrementalSourceE2ECase extends AbstractITCaseService {
    private static final Logger LOG = LoggerFactory.getLogger(DorisIncrementalSourceE2ECase.class);
    private static final String SOURCE_DATABASE = "test_doris_incremental_all_types_source";
    private static final String SINK_DATABASE = "test_doris_incremental_all_types_sink";
    private static final String TABLE = "test_tbl";
    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final int COLUMN_COUNT = 20;

    private static final String SOURCE_TABLE_RESOURCE =
            "container/e2e/doris2doris/test_doris_incremental_all_types_source_tbl.sql";
    private static final String SINK_TABLE_RESOURCE =
            "container/e2e/doris2doris/test_doris_incremental_all_types_sink_tbl.sql";
    private static final String SOURCE_DATA_RESOURCE =
            "container/e2e/doris2doris/test_doris_incremental_all_types_source_data.sql";
    private static final String MUTATIONS_RESOURCE =
            "container/e2e/doris2doris/test_doris_incremental_all_types_mutations.sql";

    private static final List<String> INITIAL_ROWS =
            Arrays.asList(
                    "1,true,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,3.14,2.71828,12345.6789,2025-03-11,2025-03-11T12:34:56,A,Hello, Doris!,This is a string,[\"Alice\", \"Bob\"],{\"key1\":\"value1\", \"key2\":\"value2\"},{\"name\":\"Tom\", \"age\":30},{\"key\":\"value\"},{\"data\":123,\"type\":\"variant\"}",
                    "2,false,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-1.23,1.0E-4,-9999.9999,2024-12-25,2024-12-25T23:59:59,B,Doris Test,Another string!,[\"Charlie\", \"David\"],{\"k1\":\"v1\", \"k2\":\"v2\"},{\"name\":\"Jerry\", \"age\":25},{\"status\":\"ok\"},{\"data\":[1,2,3]}",
                    "3,true,0,0,0,0,0,0.0,0.0,0.0000,2023-06-15,2023-06-15T08:00,C,Test Doris,Sample text,[\"Eve\", \"Frank\"],{\"alpha\":\"beta\"},{\"name\":\"Alice\", \"age\":40},{\"nested\":{\"key\":\"value\"}},{\"variant\":\"test\"}",
                    "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null");

    private static final List<String> FINAL_ROWS =
            Arrays.asList(
                    "1,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null",
                    "3,true,0,0,0,0,0,0.0,0.0,0.0000,2023-06-15,2023-06-15T08:00,C,Test Doris,Sample text,[\"Eve\", \"Frank\"],{\"alpha\":\"beta\"},{\"name\":\"Alice\", \"age\":40},{\"nested\":{\"key\":\"value\"}},{\"variant\":\"test\"}",
                    "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null",
                    "5,true,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,3.14,2.71828,12345.6789,2025-03-11,2025-03-11T12:34:56,A,Hello, Doris!,This is a string,[\"Alice\", \"Bob\"],{\"key1\":\"value1\", \"key2\":\"value2\"},{\"name\":\"Tom\", \"age\":30},{\"key\":\"value\"},{\"data\":123,\"type\":\"variant\"}");

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

    @Test
    public void testDorisToDorisIncrementalAllTypes() throws Exception {
        initializeDorisTables();
        String startTimestamp = resolveCurrentDorisTimestamp();
        executeResource(SOURCE_DATA_RESOURCE);
        waitForDorisTimestampAfter(startTimestamp);

        JobClient jobClient = null;
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(DEFAULT_PARALLELISM);
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(500L);
            StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

            String sourceDdl =
                    String.format(
                            "CREATE TABLE doris_incremental_all_types_source (\n"
                                    + "  id INT,\n"
                                    + "  c1 BOOLEAN,\n"
                                    + "  c2 TINYINT,\n"
                                    + "  c3 SMALLINT,\n"
                                    + "  c4 INT,\n"
                                    + "  c5 BIGINT,\n"
                                    + "  c6 STRING,\n"
                                    + "  c7 FLOAT,\n"
                                    + "  c8 DOUBLE,\n"
                                    + "  c9 DECIMAL(12, 4),\n"
                                    + "  c10 DATE,\n"
                                    + "  c11 TIMESTAMP,\n"
                                    + "  c12 CHAR(1),\n"
                                    + "  c13 VARCHAR(256),\n"
                                    + "  c14 STRING,\n"
                                    + "  c15 ARRAY<STRING>,\n"
                                    + "  c16 MAP<STRING, STRING>,\n"
                                    + "  c17 ROW<name STRING, age INT>,\n"
                                    + "  c18 STRING,\n"
                                    + "  c19 STRING,\n"
                                    + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                    + ") WITH (\n"
                                    + "  'connector' = '%s',\n"
                                    + "  'fenodes' = '%s',\n"
                                    + "  'table.identifier' = '%s.%s',\n"
                                    + "  'username' = '%s',\n"
                                    + "  'password' = '%s',\n"
                                    + "  'source.scan.mode' = 'from-timestamp',\n"
                                    + "  'source.scan.timestamp' = '%s',\n"
                                    + "  'source.use-flight-sql' = 'true',\n"
                                    + "  'source.flight-sql-port' = '%d',\n"
                                    + "  'source.binlog.increment-type' = 'detail',\n"
                                    + "  'source.binlog.poll-interval' = '1s'\n"
                                    + ")",
                            DorisConfigOptions.IDENTIFIER,
                            escapeSqlLiteral(getFenodes()),
                            SOURCE_DATABASE,
                            TABLE,
                            escapeSqlLiteral(getDorisUsername()),
                            escapeSqlLiteral(getDorisPassword()),
                            escapeSqlLiteral(startTimestamp),
                            getFlightSqlPort());
            tableEnvironment.executeSql(sourceDdl);

            String sinkDdl =
                    String.format(
                            "CREATE TABLE doris_incremental_all_types_sink (\n"
                                    + "  id INT,\n"
                                    + "  c1 BOOLEAN,\n"
                                    + "  c2 TINYINT,\n"
                                    + "  c3 SMALLINT,\n"
                                    + "  c4 INT,\n"
                                    + "  c5 BIGINT,\n"
                                    + "  c6 STRING,\n"
                                    + "  c7 FLOAT,\n"
                                    + "  c8 DOUBLE,\n"
                                    + "  c9 DECIMAL(12, 4),\n"
                                    + "  c10 DATE,\n"
                                    + "  c11 TIMESTAMP,\n"
                                    + "  c12 CHAR(1),\n"
                                    + "  c13 VARCHAR(256),\n"
                                    + "  c14 STRING,\n"
                                    + "  c15 ARRAY<STRING>,\n"
                                    + "  c16 MAP<STRING, STRING>,\n"
                                    + "  c17 ROW<name STRING, age INT>,\n"
                                    + "  c18 STRING,\n"
                                    + "  c19 STRING,\n"
                                    + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                    + ") WITH (\n"
                                    + "  'connector' = '%s',\n"
                                    + "  'fenodes' = '%s',\n"
                                    + "  'table.identifier' = '%s.%s',\n"
                                    + "  'username' = '%s',\n"
                                    + "  'password' = '%s',\n"
                                    + "  'sink.label-prefix' = '%s',\n"
                                    + "  'sink.enable-2pc' = 'true',\n"
                                    + "  'sink.enable-delete' = 'true',\n"
                                    + "  'sink.ignore.update-before' = 'true',\n"
                                    + "  'sink.buffer-flush.interval' = '1s'\n"
                                    + ")",
                            DorisConfigOptions.IDENTIFIER,
                            escapeSqlLiteral(getFenodes()),
                            SINK_DATABASE,
                            TABLE,
                            escapeSqlLiteral(getDorisUsername()),
                            escapeSqlLiteral(getDorisPassword()),
                            UUID.randomUUID());
            tableEnvironment.executeSql(sinkDdl);

            TableResult result =
                    tableEnvironment.executeSql(
                            "INSERT INTO doris_incremental_all_types_sink "
                                    + "SELECT * FROM doris_incremental_all_types_source");
            jobClient =
                    result.getJobClient()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Incremental all-types job has no JobClient"));
            waitForJobStatus(
                    jobClient,
                    Collections.singletonList(JobStatus.RUNNING),
                    Deadline.fromNow(TIMEOUT));

            awaitSinkRows(INITIAL_ROWS);
            executeResource(MUTATIONS_RESOURCE);
            awaitSinkRows(FINAL_ROWS);
        } finally {
            cancelJob(jobClient);
        }
    }

    private void initializeDorisTables() {
        executeResource(SOURCE_TABLE_RESOURCE);
        executeResource(SINK_TABLE_RESOURCE);
    }

    private void executeResource(String resource) {
        String[] statements = ContainerUtils.parseFileContentSQL(resource);
        ContainerUtils.executeSQLStatement(getDorisQueryConnection(), LOG, statements);
    }

    private String resolveCurrentDorisTimestamp() {
        return RestService.resolveCurrentTimestamp(dorisOptions(), incrementalReadOptions(), LOG);
    }

    private void waitForDorisTimestampAfter(String timestamp) throws Exception {
        AtomicReference<String> currentTimestamp = new AtomicReference<>();
        try {
            waitUntilCondition(
                    () -> {
                        String current = resolveCurrentDorisTimestamp();
                        currentTimestamp.set(current);
                        return current.compareTo(timestamp) > 0;
                    },
                    Deadline.fromNow(TIMEOUT),
                    100L,
                    "Doris timestamp did not advance beyond " + timestamp);
        } catch (TimeoutException e) {
            TimeoutException timeout =
                    new TimeoutException(
                            "Doris timestamp did not advance beyond "
                                    + timestamp
                                    + "; current="
                                    + currentTimestamp.get());
            timeout.initCause(e);
            throw timeout;
        }
    }

    private DorisOptions dorisOptions() {
        return DorisOptions.builder()
                .setFenodes(getFenodes())
                .setTableIdentifier(SOURCE_DATABASE + "." + TABLE)
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

    private void awaitSinkRows(List<String> expected) throws Exception {
        AtomicReference<List<String>> actual = new AtomicReference<>(Collections.emptyList());
        try (Connection connection = getDorisQueryConnection()) {
            try {
                waitUntilCondition(
                        () -> {
                            List<String> rows =
                                    ContainerUtils.executeSQLStatement(
                                            connection,
                                            LOG,
                                            String.format(
                                                    "SELECT * FROM %s.%s ORDER BY id",
                                                    SINK_DATABASE, TABLE),
                                            COLUMN_COUNT);
                            actual.set(rows);
                            return rows.equals(expected);
                        },
                        Deadline.fromNow(TIMEOUT),
                        200L,
                        "Doris sink rows did not match " + expected);
            } catch (TimeoutException e) {
                TimeoutException timeout =
                        new TimeoutException(
                                "Doris sink rows did not match "
                                        + expected
                                        + "; actual="
                                        + actual.get());
                timeout.initCause(e);
                throw timeout;
            }
        }
    }

    private void cancelJob(JobClient jobClient) throws Exception {
        if (jobClient == null) {
            return;
        }
        JobStatus status = jobClient.getJobStatus().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (!status.isTerminalState()) {
            jobClient.cancel().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private int getFlightSqlPort() {
        return Integer.getInteger("doris_flight_sql_port", 9611);
    }

    private static String escapeSqlLiteral(String value) {
        return value == null ? "" : value.replace("'", "''");
    }
}
