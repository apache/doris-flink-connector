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

package org.apache.doris.flink.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.DorisTestBase;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** DorisSink ITCase with csv and arrow format. */
public class DorisSinkITCase extends DorisTestBase {
    static final String DATABASE = "test";
    static final String TABLE_CSV = "tbl_csv";
    static final String TABLE_JSON = "tbl_json";
    static final String TABLE_JSON_TBL = "tbl_json_tbl";

    @Test
    public void testSinkCsvFormat() throws Exception {
        initializeTable(TABLE_CSV);
        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");
        submitJob(TABLE_CSV, properties, new String[] {"doris,1"});

        Thread.sleep(10000);
        Set<List<Object>> actual = new HashSet<>();
        try (Statement sinkStatement = connection.createStatement()) {
            ResultSet sinkResultSet =
                    sinkStatement.executeQuery(
                            String.format(
                                    "select name,age from %s.%s order by 1", DATABASE, TABLE_CSV));
            while (sinkResultSet.next()) {
                List<Object> row =
                        Arrays.asList(sinkResultSet.getString("name"), sinkResultSet.getInt("age"));
                actual.add(row);
            }
        }
        Set<List<Object>> expected =
                Stream.<List<Object>>of(Arrays.asList("doris", 1)).collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);
    }

    @Test
    public void testSinkJsonFormat() throws Exception {
        initializeTable(TABLE_JSON);
        Properties properties = new Properties();
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("format", "json");

        // mock data
        Map<String, Object> row1 = new HashMap<>();
        row1.put("name", "doris1");
        row1.put("age", 1);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("name", "doris2");
        row2.put("age", 2);

        submitJob(
                TABLE_JSON,
                properties,
                new String[] {
                    new ObjectMapper().writeValueAsString(row1),
                    new ObjectMapper().writeValueAsString(row2)
                });

        Thread.sleep(10000);
        Set<List<Object>> actual = new HashSet<>();
        try (Statement sinkStatement = connection.createStatement()) {
            ResultSet sinkResultSet =
                    sinkStatement.executeQuery(
                            String.format(
                                    "select name,age from %s.%s order by 1", DATABASE, TABLE_JSON));
            while (sinkResultSet.next()) {
                List<Object> row =
                        Arrays.asList(sinkResultSet.getString("name"), sinkResultSet.getInt("age"));
                actual.add(row);
            }
        }
        Set<List<Object>> expected =
                Stream.<List<Object>>of(Arrays.asList("doris1", 1), Arrays.asList("doris2", 2))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);
    }

    public void submitJob(String table, Properties properties, String[] records) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DorisSink.Builder<String> builder = DorisSink.builder();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();

        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier(DATABASE + "." + table)
                .setUsername(USERNAME)
                .setPassword(PASSWORD);
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix(UUID.randomUUID().toString()).setStreamLoadProp(properties);

        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        env.fromElements(records).sinkTo(builder.build());
        env.execute();
    }

    @Test
    public void testTableSinkJsonFormat() throws Exception {
        initializeTable(TABLE_JSON_TBL);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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
                                + " 'password' = '%s',"
                                + " 'sink.properties.format' = 'json',"
                                + " 'sink.properties.read_json_by_line' = 'true',"
                                + " 'sink.label-prefix' = 'doris_sink'"
                                + ")",
                        getFenodes(), DATABASE + "." + TABLE_JSON_TBL, USERNAME, PASSWORD);
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql("INSERT INTO doris_sink SELECT 'doris',1 union all SELECT 'flink',2");

        Thread.sleep(10000);
        Set<List<Object>> actual = new HashSet<>();
        try (Statement sinkStatement = connection.createStatement()) {
            ResultSet sinkResultSet =
                    sinkStatement.executeQuery(
                            String.format(
                                    "select name,age from %s.%s order by 1",
                                    DATABASE, TABLE_JSON_TBL));
            while (sinkResultSet.next()) {
                List<Object> row =
                        Arrays.asList(sinkResultSet.getString("name"), sinkResultSet.getInt("age"));
                actual.add(row);
            }
        }
        Set<List<Object>> expected =
                Stream.<List<Object>>of(Arrays.asList("doris", 1), Arrays.asList("flink", 2))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);
    }

    private void initializeTable(String table) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s ( \n"
                                    + "`name` varchar(256),\n"
                                    + "`age` int\n"
                                    + ") DISTRIBUTED BY HASH(`name`) BUCKETS 1\n"
                                    + "PROPERTIES (\n"
                                    + "\"replication_num\" = \"1\"\n"
                                    + ")\n",
                            DATABASE, table));
        }
    }
}
