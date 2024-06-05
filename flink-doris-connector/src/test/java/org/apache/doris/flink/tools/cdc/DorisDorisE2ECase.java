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
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
                                + "id  int,\n"
                                + "c1  boolean,\n"
                                + "c2  tinyint,\n"
                                + "c3  smallint,\n"
                                + "c4  int, \n"
                                + "c5  bigint, \n"
                                + "c6  string, \n"
                                + "c7  float, \n"
                                + "c8  double, \n"
                                + "c9  decimal(12,4), \n"
                                + "c10  date, \n"
                                + "c11  TIMESTAMP, \n"
                                + "c12  char(1), \n"
                                + "c13  varchar(256), \n"
                                + "c14  Array<String>, \n"
                                + "c15  Map<String, String>, \n"
                                + "c16  ROW<name String, age int>, \n"
                                + "c17  STRING \n"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'sink.label-prefix' = '"
                                + UUID.randomUUID()
                                + "',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(),
                        DATABASE_SOURCE + "." + TABLE,
                        USERNAME,
                        PASSWORD);
        tEnv.executeSql(sourceDDL);

        String sinkDDL =
                String.format(
                        "CREATE TABLE doris_sink ("
                                + "id  int,\n"
                                + "c1  boolean,\n"
                                + "c2  tinyint,\n"
                                + "c3  smallint,\n"
                                + "c4  int, \n"
                                + "c5  bigint, \n"
                                + "c6  string, \n"
                                + "c7  float, \n"
                                + "c8  double, \n"
                                + "c9  decimal(12,4), \n"
                                + "c10  date, \n"
                                + "c11  TIMESTAMP, \n"
                                + "c12  char(1), \n"
                                + "c13  varchar(256), \n"
                                + "c14  Array<String>, \n"
                                + "c15  Map<String, String>, \n"
                                + "c16  ROW<name String, age int>, \n"
                                + "c17  STRING \n"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'sink.label-prefix' = '"
                                + UUID.randomUUID()
                                + "',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(),
                        DATABASE_SINK + "." + TABLE,
                        USERNAME,
                        PASSWORD);
        tEnv.executeSql(sinkDDL);

        tEnv.executeSql("INSERT INTO doris_sink SELECT * FROM doris_source").await();

        TableResult tableResult = tEnv.executeSql("SELECT * FROM doris_sink");
        List<Object> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        System.out.println(actual);
        String[] expected =
                new String[] {
                    "+I[1, true, 127, 32767, 2147483647, 9223372036854775807, 123456789012345678901234567890, 3.14, 2.7182818284, 12345.6789, 2023-05-22, 2023-05-22T12:34:56, A, Example text, [item1, item2, item3], {key1=value1, key2=value2}, +I[John Doe, 30], {\"key\":\"value\"}]",
                    "+I[2, false, -128, -32768, -2147483648, -9223372036854775808, -123456789012345678901234567890, -3.14, -2.7182818284, -12345.6789, 2024-01-01, 2024-01-01T00:00, B, Another example, [item4, item5, item6], {key3=value3, key4=value4}, +I[Jane Doe, 25], {\"another_key\":\"another_value\"}]"
                };
        Assert.assertArrayEquals(expected, actual.toArray(new String[0]));
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
                            "CREATE TABLE %s.%s (\n"
                                    + "            `id` int,\n"
                                    + "            `c1` boolean,\n"
                                    + "            `c2` tinyint,\n"
                                    + "            `c3` smallint,\n"
                                    + "            `c4` int,\n"
                                    + "            `c5` bigint,\n"
                                    + "            `c6` largeint,\n"
                                    + "            `c7` float,\n"
                                    + "            `c8` double,\n"
                                    + "            `c9` decimal(12,4),\n"
                                    + "            `c10` date,\n"
                                    + "            `c11` datetime,\n"
                                    + "            `c12` char(1),\n"
                                    + "            `c13` varchar(256),\n"
                                    + "            `c14` Array<String>,\n"
                                    + "            `c15` Map<String, String>,\n"
                                    + "            `c16` Struct<name: String, age: int>,\n"
                                    + "            `c17` JSON\n"
                                    + "        )\n"
                                    + "DUPLICATE KEY(`id`)\n"
                                    + "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n"
                                    + "PROPERTIES (\n"
                                    + "\"replication_num\" = \"1\",\n"
                                    + "\"light_schema_change\" = \"true\"\n"
                                    + ");",
                            DATABASE_SOURCE, table));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s like %s.%s",
                            DATABASE_SINK, table, DATABASE_SOURCE, table));
            statement.execute(
                    String.format(
                            "INSERT INTO %s.%s \n"
                                    + "VALUES \n"
                                    + "(\n"
                                    + "    1,  \n"
                                    + "    TRUE, \n"
                                    + "    127,  \n"
                                    + "    32767, \n"
                                    + "    2147483647, \n"
                                    + "    9223372036854775807, \n"
                                    + "    123456789012345678901234567890, \n"
                                    + "    3.14,  \n"
                                    + "    2.7182818284, \n"
                                    + "    12345.6789, \n"
                                    + "    '2023-05-22',  \n"
                                    + "    '2023-05-22 12:34:56', \n"
                                    + "    'A', \n"
                                    + "    'Example text', \n"
                                    + "    ['item1', 'item2', 'item3'], \n"
                                    + "    {'key1': 'value1', 'key2': 'value2'}, \n"
                                    + "    STRUCT('John Doe', 30),  \n"
                                    + "    '{\"key\": \"value\"}'  \n"
                                    + "),\n"
                                    + "(\n"
                                    + "    2,\n"
                                    + "    FALSE,\n"
                                    + "    -128,\n"
                                    + "    -32768,\n"
                                    + "    -2147483648,\n"
                                    + "    -9223372036854775808,\n"
                                    + "    -123456789012345678901234567890,\n"
                                    + "    -3.14,\n"
                                    + "    -2.7182818284,\n"
                                    + "    -12345.6789,\n"
                                    + "    '2024-01-01',\n"
                                    + "    '2024-01-01 00:00:00',\n"
                                    + "    'B',\n"
                                    + "    'Another example',\n"
                                    + "    ['item4', 'item5', 'item6'],\n"
                                    + "    {'key3': 'value3', 'key4': 'value4'},\n"
                                    + "    STRUCT('Jane Doe', 25),\n"
                                    + "    '{\"another_key\": \"another_value\"}'\n"
                                    + ")",
                            DATABASE_SOURCE, table));
        }
    }
}
