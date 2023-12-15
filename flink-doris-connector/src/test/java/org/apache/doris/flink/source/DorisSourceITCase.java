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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.doris.flink.DorisTestBase;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** DorisSource ITCase. */
public class DorisSourceITCase extends DorisTestBase {
    static final String DATABASE = "test";
    static final String TABLE_READ = "tbl_read";
    static final String TABLE_READ_TBL = "tbl_read_tbl";

    @Test
    public void testSource() throws Exception {
        initializeTable(TABLE_READ);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();

        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier(DATABASE + "." + TABLE_READ)
                .setUsername(USERNAME)
                .setPassword(PASSWORD);

        DorisSource<List<?>> source =
                DorisSource.<List<?>>builder()
                        .setDorisReadOptions(readOptionBuilder.build())
                        .setDorisOptions(dorisBuilder.build())
                        .setDeserializer(new SimpleListDeserializationSchema())
                        .build();
        List<Object> actual = new ArrayList<>();
        try (CloseableIterator<List<?>> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Doris Source")
                        .executeAndCollect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
        }
        List<Object> expected =
                Arrays.asList(Arrays.asList("doris", 18), Arrays.asList("flink", 10));
        Assertions.assertIterableEquals(expected, actual);
    }

    @Test
    public void testTableSource() throws Exception {
        initializeTable(TABLE_READ_TBL);
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
                        getFenodes(), DATABASE + "." + TABLE_READ_TBL, USERNAME, PASSWORD);
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("SELECT * FROM doris_source");

        List<Object> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        String[] expected = new String[] {"+I[doris, 18]", "+I[flink, 10]"};
        Assertions.assertIterableEquals(Arrays.asList(expected), actual);
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
            statement.execute(
                    String.format("insert into %s.%s  values ('doris',18)", DATABASE, table));
            statement.execute(
                    String.format("insert into %s.%s  values ('flink',10)", DATABASE, table));
        }
    }
}
