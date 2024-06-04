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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.DorisTestBase;
import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.flink.api.common.JobStatus.RUNNING;

/**
 * MySQLDorisE2ECase 1. Automatically create tables 2. Schema change event synchronization
 * 3.Synchronization of addition, deletion and modification events 4. CDC multi-table writing.
 */
public class MySQLDorisE2ECase extends DorisTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(MySQLDorisE2ECase.class);
    private static final String DATABASE = "test_e2e_mysql";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWD = "123456";
    private static final String TABLE_1 = "tbl1";
    private static final String TABLE_2 = "tbl2";
    private static final String TABLE_3 = "tbl3";
    private static final String TABLE_4 = "tbl4";

    private static final MySQLContainer MYSQL_CONTAINER =
            new MySQLContainer("mysql:8.0")
                    .withDatabaseName(DATABASE)
                    .withUsername(MYSQL_USER)
                    .withPassword(MYSQL_PASSWD);

    @BeforeClass
    public static void startMySQLContainers() {
        MYSQL_CONTAINER.setCommand("--default-time-zone=Asia/Shanghai");
        LOG.info("Starting MySQL containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("MySQL Containers are started.");
    }

    @AfterClass
    public static void stopMySQLContainers() {
        LOG.info("Stopping MySQL containers...");
        MYSQL_CONTAINER.stop();
        LOG.info("MySQL Containers are stopped.");
    }

    @Test
    public void testMySQL2Doris() throws Exception {
        printClusterStatus();
        initializeMySQLTable();
        initializeDorisTable();
        JobClient jobClient = submitJob();
        // wait 2 times checkpoint
        Thread.sleep(20000);
        List<String> expected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3");
        String sql =
                "select * from ( select * from %s.%s union all select * from %s.%s union all select * from %s.%s ) res order by 1";
        String query1 = String.format(sql, DATABASE, TABLE_1, DATABASE, TABLE_2, DATABASE, TABLE_3);
        checkResult(expected, query1, 2);

        // add incremental data
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), MYSQL_USER, MYSQL_PASSWD);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_1_1',10)", DATABASE, TABLE_1));
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_2_1',11)", DATABASE, TABLE_2));
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_3_1',12)", DATABASE, TABLE_3));

            statement.execute(
                    String.format(
                            "update %s.%s set age=18 where name='doris_1'", DATABASE, TABLE_1));
            statement.execute(
                    String.format("delete from %s.%s where name='doris_2'", DATABASE, TABLE_2));
        }

        Thread.sleep(20000);
        List<String> expected2 =
                Arrays.asList(
                        "doris_1,18", "doris_1_1,10", "doris_2_1,11", "doris_3,3", "doris_3_1,12");
        sql =
                "select * from ( select * from %s.%s union all select * from %s.%s union all select * from %s.%s ) res order by 1";
        String query2 = String.format(sql, DATABASE, TABLE_1, DATABASE, TABLE_2, DATABASE, TABLE_3);
        checkResult(expected2, query2, 2);

        // mock schema change
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), MYSQL_USER, MYSQL_PASSWD);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "alter table %s.%s add column c1 varchar(128)", DATABASE, TABLE_1));
            statement.execute(
                    String.format("alter table %s.%s drop column age", DATABASE, TABLE_1));
            Thread.sleep(20000);
            statement.execute(
                    String.format(
                            "insert into %s.%s  values ('doris_1_1_1','c1_val')",
                            DATABASE, TABLE_1));
        }
        Thread.sleep(20000);
        List<String> expected3 =
                Arrays.asList("doris_1,null", "doris_1_1,null", "doris_1_1_1,c1_val");
        sql = "select * from %s.%s order by 1";
        String query3 = String.format(sql, DATABASE, TABLE_1);
        checkResult(expected3, query3, 2);
        jobClient.cancel().get();
    }

    @Test
    public void testAutoAddTable() throws Exception {
        printClusterStatus();
        initializeMySQLTable();
        initializeDorisTable();
        JobClient jobClient = submitJob();
        // wait 2 times checkpoint
        Thread.sleep(20000);
        List<String> expected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3");
        String sql =
                "select * from ( select * from %s.%s union all select * from %s.%s union all select * from %s.%s ) res order by 1";
        String query1 = String.format(sql, DATABASE, TABLE_1, DATABASE, TABLE_2, DATABASE, TABLE_3);
        checkResult(expected, query1, 2);

        // auto create table4
        addTableTable_4();
        Thread.sleep(20000);
        List<String> expected2 = Arrays.asList("doris_4_1,4", "doris_4_2,4");
        sql = "select * from %s.%s order by 1";
        String query2 = String.format(sql, DATABASE, TABLE_4);
        checkResult(expected2, query2, 2);

        // add incremental data
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), MYSQL_USER, MYSQL_PASSWD);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_1_1',10)", DATABASE, TABLE_1));
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_2_1',11)", DATABASE, TABLE_2));
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_3_1',12)", DATABASE, TABLE_3));
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_4_3',43)", DATABASE, TABLE_4));

            statement.execute(
                    String.format(
                            "update %s.%s set age=18 where name='doris_1'", DATABASE, TABLE_1));
            statement.execute(
                    String.format("delete from %s.%s where name='doris_2'", DATABASE, TABLE_2));
            statement.execute(
                    String.format("delete from %s.%s where name='doris_4_2'", DATABASE, TABLE_4));
            statement.execute(
                    String.format(
                            "update %s.%s set age=41 where name='doris_4_1'", DATABASE, TABLE_4));
        }

        Thread.sleep(20000);
        List<String> expected3 =
                Arrays.asList(
                        "doris_1,18",
                        "doris_1_1,10",
                        "doris_2_1,11",
                        "doris_3,3",
                        "doris_3_1,12",
                        "doris_4_1,41",
                        "doris_4_3,43");
        sql =
                "select * from ( select * from %s.%s union all select * from %s.%s union all select * from %s.%s union all select * from %s.%s ) res order by 1";
        String query3 =
                String.format(
                        sql, DATABASE, TABLE_1, DATABASE, TABLE_2, DATABASE, TABLE_3, DATABASE,
                        TABLE_4);
        checkResult(expected3, query3, 2);

        // mock schema change
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), MYSQL_USER, MYSQL_PASSWD);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "alter table %s.%s add column c1 varchar(128)", DATABASE, TABLE_4));
            statement.execute(
                    String.format("alter table %s.%s drop column age", DATABASE, TABLE_4));
            Thread.sleep(20000);
            statement.execute(
                    String.format(
                            "insert into %s.%s  values ('doris_4_4','c1_val')", DATABASE, TABLE_4));
        }
        Thread.sleep(20000);
        List<String> expected4 =
                Arrays.asList("doris_4_1,null", "doris_4_3,null", "doris_4_4,c1_val");
        sql = "select * from %s.%s order by 1";
        String query4 = String.format(sql, DATABASE, TABLE_4);
        checkResult(expected4, query4, 2);
        jobClient.cancel().get();
    }

    @Test
    public void testMySQL2DorisByDefault() throws Exception {
        printClusterStatus();
        initializeMySQLTable();
        initializeDorisTable();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());
        Map<String, String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval", "10s");
        flinkMap.put("pipeline.operator-chaining", "false");
        flinkMap.put("parallelism.default", "1");

        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);

        String database = DATABASE;
        Map<String, String> mysqlConfig = new HashMap<>();
        mysqlConfig.put("database-name", DATABASE);
        mysqlConfig.put("hostname", MYSQL_CONTAINER.getHost());
        mysqlConfig.put("port", MYSQL_CONTAINER.getMappedPort(3306) + "");
        mysqlConfig.put("username", MYSQL_USER);
        mysqlConfig.put("password", MYSQL_PASSWD);
        mysqlConfig.put("server-time-zone", "Asia/Shanghai");
        Configuration config = Configuration.fromMap(mysqlConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put("fenodes", getFenodes());
        sinkConfig.put("username", USERNAME);
        sinkConfig.put("password", PASSWORD);
        sinkConfig.put("jdbc-url", String.format(DorisTestBase.URL, DORIS_CONTAINER.getHost()));
        sinkConfig.put("sink.label-prefix", UUID.randomUUID().toString());
        sinkConfig.put("sink.check-interval", "5000");
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("replication_num", "1");

        String includingTables = "tbl1|tbl2|tbl3";
        String excludingTables = "";
        DatabaseSync databaseSync = new MysqlDatabaseSync();
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
                // no single sink
                .setSingleSink(false)
                .create();
        databaseSync.build();
        JobClient jobClient = env.executeAsync();
        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(10)));

        // wait 2 times checkpoint
        Thread.sleep(20000);
        List<String> expected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3");
        String sql =
                "select * from ( select * from %s.%s union all select * from %s.%s union all select * from %s.%s ) res order by 1";
        String query1 = String.format(sql, DATABASE, TABLE_1, DATABASE, TABLE_2, DATABASE, TABLE_3);
        checkResult(expected, query1, 2);

        // add incremental data
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), MYSQL_USER, MYSQL_PASSWD);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_1_1',10)", DATABASE, TABLE_1));
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_2_1',11)", DATABASE, TABLE_2));
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_3_1',12)", DATABASE, TABLE_3));

            statement.execute(
                    String.format(
                            "update %s.%s set age=18 where name='doris_1'", DATABASE, TABLE_1));
            statement.execute(
                    String.format("delete from %s.%s where name='doris_2'", DATABASE, TABLE_2));
        }

        Thread.sleep(20000);
        List<String> expected2 =
                Arrays.asList(
                        "doris_1,18", "doris_1_1,10", "doris_2_1,11", "doris_3,3", "doris_3_1,12");
        sql =
                "select * from ( select * from %s.%s union all select * from %s.%s union all select * from %s.%s ) res order by 1";
        String query2 = String.format(sql, DATABASE, TABLE_1, DATABASE, TABLE_2, DATABASE, TABLE_3);
        checkResult(expected2, query2, 2);
        jobClient.cancel().get();
    }

    private void initializeDorisTable() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                String.format(URL, DORIS_CONTAINER.getHost()), USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_1));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_2));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_3));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_4));
        }
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
        Map<String, String> mysqlConfig = new HashMap<>();
        mysqlConfig.put("database-name", DATABASE);
        mysqlConfig.put("hostname", MYSQL_CONTAINER.getHost());
        mysqlConfig.put("port", MYSQL_CONTAINER.getMappedPort(3306) + "");
        mysqlConfig.put("username", MYSQL_USER);
        mysqlConfig.put("password", MYSQL_PASSWD);
        mysqlConfig.put("server-time-zone", "Asia/Shanghai");
        Configuration config = Configuration.fromMap(mysqlConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put("fenodes", getFenodes());
        sinkConfig.put("username", USERNAME);
        sinkConfig.put("password", PASSWORD);
        sinkConfig.put("jdbc-url", String.format(DorisTestBase.URL, DORIS_CONTAINER.getHost()));
        sinkConfig.put("sink.label-prefix", UUID.randomUUID().toString());
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("replication_num", "1");

        String includingTables = "tbl.*";
        String excludingTables = "";
        DatabaseSync databaseSync = new MysqlDatabaseSync();
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
                .setSingleSink(true)
                .setIgnoreDefaultValue(true)
                .create();
        databaseSync.build();
        JobClient jobClient = env.executeAsync();
        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(10)));
        return jobClient;
    }

    private void addTableTable_4() throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), MYSQL_USER, MYSQL_PASSWD);
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_4));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s ( \n"
                                    + "`name` varchar(256) primary key,\n"
                                    + "`age` int\n"
                                    + ")",
                            DATABASE, TABLE_4));

            // mock stock data
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_4_1',4)", DATABASE, TABLE_4));
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_4_2',4)", DATABASE, TABLE_4));
        }
    }

    public void initializeMySQLTable() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), MYSQL_USER, MYSQL_PASSWD);
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_1));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_2));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_3));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_4));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s ( \n"
                                    + "`name` varchar(256) primary key,\n"
                                    + "`age` int\n"
                                    + ")",
                            DATABASE, TABLE_1));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s ( \n"
                                    + "`name` varchar(256) primary key,\n"
                                    + "`age` int\n"
                                    + ")",
                            DATABASE, TABLE_2));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s ( \n"
                                    + "`name` varchar(256) primary key,\n"
                                    + "`age` int\n"
                                    + ")",
                            DATABASE, TABLE_3));
            // mock stock data
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_1',1)", DATABASE, TABLE_1));
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_2',2)", DATABASE, TABLE_2));
            statement.execute(
                    String.format("insert into %s.%s  values ('doris_3',3)", DATABASE, TABLE_3));
        }
    }
}
