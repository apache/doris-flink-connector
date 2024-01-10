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

package org.apache.doris.flink.tools.cdc;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.doris.flink.DorisTestBase;
import org.apache.doris.flink.tools.cdc.oracle.OracleDatabaseSync;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.api.common.JobStatus.RUNNING;

/**
 * OracleDorisE2ECase 1. Automatically create tables 2. Schema change event synchronization 3.
 * Synchronization of addition, deletion and modification events 4. CDC multi-table writing.
 */
public class OracleDorisE2ECase extends DorisTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(OracleDorisE2ECase.class);
    private static final String DATABASE = "test";
    private static final String ORACLE_USER = "system";
    private static final String ORACLE_PASSWD = "123456";
    private static final String ORACLE_TABLE_1 = "orc_tbl1";
    private static final String ORACLE_TABLE_2 = "orc_tbl2";
    private static final String ORACLE_TABLE_3 = "orc_tbl3";

    private static final OracleContainer ORACLE_CONTAINER =
            new OracleContainer("gvenzl/oracle-xe:21-slim-faststart")
                    .withDatabaseName(DATABASE)
                    .withPassword(ORACLE_PASSWD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startOracleContainers() {
        LOG.info("Starting Oracle containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Oracle Containers are started.");
    }

    @AfterClass
    public static void stopOracleContainers() {
        LOG.info("Stopping Oracle containers...");
        ORACLE_CONTAINER.stop();
        LOG.info("Oracle Containers are stopped.");
    }

    @Test
    public void testOracle2Doris() throws Exception {
        initializeOracleTable();
        JobClient jobClient = submitJob();
        // wait 2 times checkpoint
        Thread.sleep(20000);
        Set<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList("orc_tbl1", 1),
                                Arrays.asList("orc_tbl2", 2),
                                Arrays.asList("orc_tbl3", 3))
                        .collect(Collectors.toSet());
        String sql =
                "select * from %s union all select * from %s union all select * from %s order by 1;";
        String query1 = String.format(sql, ORACLE_TABLE_1, ORACLE_TABLE_2, ORACLE_TABLE_3);
        checkResult(expected, query1, 2);

        // add incremental data
        try (Connection connection =
                        DriverManager.getConnection(
                                ORACLE_CONTAINER.getJdbcUrl(), ORACLE_USER, ORACLE_PASSWD);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format("insert into %s  values ('doris_1_1',10);", ORACLE_TABLE_1));
            statement.execute(
                    String.format("insert into %s  values ('doris_2_1',11);", ORACLE_TABLE_2));
            statement.execute(
                    String.format("insert into %s  values ('doris_3_1',12);", ORACLE_TABLE_3));

            statement.execute(
                    String.format("update %s set age=18 where name='doris_1';", ORACLE_TABLE_1));
            statement.execute(
                    String.format("delete from %s where name='doris_2';", ORACLE_TABLE_2));
        }

        Thread.sleep(20000);
        Set<List<Object>> expected2 =
                Stream.<List<Object>>of(
                                Arrays.asList("doris_1", 18),
                                Arrays.asList("doris_1_1", 10),
                                Arrays.asList("doris_2_1", 11),
                                Arrays.asList("doris_3", 3),
                                Arrays.asList("doris_3_1", 12))
                        .collect(Collectors.toSet());
        sql = "select * from %s union all select * from %s union all select * from %s order by 1;";
        String query2 = String.format(sql, ORACLE_TABLE_1, ORACLE_TABLE_2, ORACLE_TABLE_3);
        checkResult(expected2, query2, 2);

        // mock schema change
        try (Connection connection =
                        DriverManager.getConnection(
                                ORACLE_CONTAINER.getJdbcUrl(), ORACLE_USER, ORACLE_PASSWD);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format("alter table %s add column c1 varchar(128)", ORACLE_TABLE_1));
            statement.execute(String.format("alter table %s drop column age", ORACLE_TABLE_1));
            Thread.sleep(20000);
            statement.execute(
                    String.format(
                            "insert into %s  values ('doris_1_1_1','c1_val')", ORACLE_TABLE_1));
        }
        Thread.sleep(20000);
        Set<List<Object>> expected3 =
                Stream.<List<Object>>of(
                                Arrays.asList("doris_1", null),
                                Arrays.asList("doris_1_1", null),
                                Arrays.asList("doris_1_1_1", "c1_val"))
                        .collect(Collectors.toSet());
        sql = "select * from %s order by 1";
        String query3 = String.format(sql, ORACLE_TABLE_1);
        checkResult(expected3, sql, 2);
        jobClient.cancel().get();
    }

    public void checkResult(Set<List<Object>> expected, String query, int columnSize)
            throws Exception {
        Set<List<Object>> actual = new HashSet<>();
        try (Statement sinkStatement = connection.createStatement()) {
            ResultSet sinkResultSet = sinkStatement.executeQuery(query);
            while (sinkResultSet.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnSize; i++) {
                    row.add(sinkResultSet.getObject(i));
                }
                actual.add(row);
            }
        }
        Assertions.assertIterableEquals(expected, actual);
    }

    public JobClient submitJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());
        Map<String, String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval", "10s");
        flinkMap.put("pipeline.operator-chaining", "false");
        flinkMap.put("parallelism.default", "1");

        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);

        String database = DATABASE;
        Map<String, String> oracleConfig = new HashMap<>();
        oracleConfig.put("hostname", ORACLE_CONTAINER.getHost());
        oracleConfig.put("port", ORACLE_CONTAINER.getMappedPort(1521) + "");
        oracleConfig.put("username", ORACLE_USER);
        oracleConfig.put("password", ORACLE_PASSWD);
        oracleConfig.put("url", ORACLE_CONTAINER.getJdbcUrl());
        oracleConfig.put("database-name", ORACLE_CONTAINER.getDatabaseName());
        oracleConfig.put("schema-name", ORACLE_USER.toLowerCase());
        Configuration config = Configuration.fromMap(oracleConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put("fenodes", getFenodes());
        sinkConfig.put("username", USERNAME);
        sinkConfig.put("password", PASSWORD);
        sinkConfig.put("jdbc-url", String.format(DorisTestBase.URL, DORIS_CONTAINER.getHost()));
        sinkConfig.put("sink.label-prefix", UUID.randomUUID().toString());
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("replication_num", "1");

        String includingTables = "tbl1|tbl2|tbl3";
        String excludingTables = "";
        DatabaseSync databaseSync = new OracleDatabaseSync();
        databaseSync
                .setEnv(env)
                .setDatabase(database)
                .setConfig(config)
                .setIncludingTables(includingTables)
                .setExcludingTables(excludingTables)
                .setIgnoreDefaultValue(false)
                .setSinkConfig(sinkConf)
                .setTableConfig(tableConfig)
                .setCreateTableOnly(false)
                .setNewSchemaChange(true)
                .create();
        databaseSync.build();
        JobClient jobClient = env.executeAsync();
        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(10)));
        return jobClient;
    }

    public void initializeOracleTable() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                ORACLE_CONTAINER.getJdbcUrl(), ORACLE_USER, ORACLE_PASSWD);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE TABLE %s ( \n"
                                    + "name varchar(256) primary key,\n"
                                    + "age int\n"
                                    + ")",
                            ORACLE_TABLE_1));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s ( \n"
                                    + "name varchar(256) primary key,\n"
                                    + "age int\n"
                                    + ")",
                            ORACLE_TABLE_2));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s ( \n"
                                    + "name varchar(256) primary key,\n"
                                    + "age int\n"
                                    + ")",
                            ORACLE_TABLE_3));
            // mock stock data
            statement.execute(
                    String.format("insert into %s  values ('doris_1',1)", ORACLE_TABLE_1));
            statement.execute(
                    String.format("insert into %s  values ('doris_2',2)", ORACLE_TABLE_2));
            statement.execute(
                    String.format("insert into %s  values ('doris_3',3)", ORACLE_TABLE_3));
        }
    }

    public static void waitForJobStatus(
            JobClient client, List<JobStatus> expectedStatus, Deadline deadline) throws Exception {
        waitUntilCondition(
                () -> {
                    JobStatus currentStatus = (JobStatus) client.getJobStatus().get();
                    if (expectedStatus.contains(currentStatus)) {
                        return true;
                    } else if (currentStatus.isTerminalState()) {
                        try {
                            client.getJobExecutionResult().get();
                        } catch (Exception var4) {
                            throw new IllegalStateException(
                                    String.format(
                                            "Job has entered %s state, but expecting %s",
                                            currentStatus, expectedStatus),
                                    var4);
                        }

                        throw new IllegalStateException(
                                String.format(
                                        "Job has entered a terminal state %s, but expecting %s",
                                        currentStatus, expectedStatus));
                    } else {
                        return false;
                    }
                },
                deadline,
                100L,
                "Condition was not met in given timeout.");
    }

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition,
            Deadline timeout,
            long retryIntervalMillis,
            String errorMsg)
            throws Exception {
        while (timeout.hasTimeLeft() && !(Boolean) condition.get()) {
            long timeLeft = Math.max(0L, timeout.timeLeft().toMillis());
            Thread.sleep(Math.min(retryIntervalMillis, timeLeft));
        }

        if (!timeout.hasTimeLeft()) {
            throw new TimeoutException(errorMsg);
        }
    }
}
