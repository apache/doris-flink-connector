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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.doris.flink.DorisTestBase;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** DorisDorisE2ECase. */
public class DorisDorisE2ECase extends DorisTestBase {
    private static final String DATABASE_SOURCE = "test_e2e_source";
    private static final String DATABASE_SINK = "test_e2e_sink";
    private static final String TABLE = "test_tbl";

    @Test
    public void testDoris2Doris() throws Exception {
        initializeDorisTable(TABLE);
        printClusterStatus();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(), DATABASE_SOURCE + "." + TABLE, USERNAME, PASSWORD);
        tEnv.executeSql(sourceDDL);

        String sinkDDL =
                String.format(
                        "CREATE TABLE doris_sink ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(), DATABASE_SINK + "." + TABLE, USERNAME, PASSWORD);
        tEnv.executeSql(sinkDDL);

        tEnv.executeSql("INSERT INTO doris_sink SELECT * FROM doris_source").await();

        TableResult tableResult = tEnv.executeSql("SELECT * FROM doris_sink");
        List<Object> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        String[] expected = new String[] {"+I[doris, 18]", "+I[flink, 10]"};
        Assertions.assertIterableEquals(Arrays.asList(expected), actual);
    }

    private void initializeDorisTable(String table) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                String.format(URL, DORIS_CONTAINER.getHost()), USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE_SOURCE));
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE_SINK));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE_SOURCE, table));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE_SINK, table));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s ( \n"
                                    + "`name` varchar(256),\n"
                                    + "`age` int\n"
                                    + ") DISTRIBUTED BY HASH(`name`) BUCKETS 1\n"
                                    + "PROPERTIES (\n"
                                    + "\"replication_num\" = \"1\"\n"
                                    + ")\n",
                            DATABASE_SOURCE, table));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s ( \n"
                                    + "`name` varchar(256),\n"
                                    + "`age` int\n"
                                    + ") DISTRIBUTED BY HASH(`name`) BUCKETS 1\n"
                                    + "PROPERTIES (\n"
                                    + "\"replication_num\" = \"1\"\n"
                                    + ")\n",
                            DATABASE_SINK, table));
            statement.execute(
                    String.format(
                            "insert into %s.%s  values ('doris',18)", DATABASE_SOURCE, table));
            statement.execute(
                    String.format(
                            "insert into %s.%s  values ('flink',10)", DATABASE_SOURCE, table));
        }
    }
}
