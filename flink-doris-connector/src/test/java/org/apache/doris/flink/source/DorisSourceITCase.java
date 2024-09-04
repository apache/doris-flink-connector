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

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/** DorisSource ITCase. */
public class DorisSourceITCase extends AbstractITCaseService {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceITCase.class);
    private static final String DATABASE = "test_source";
    private static final String TABLE_READ = "tbl_read";
    private static final String TABLE_READ_OLD_API = "tbl_read_old_api";
    private static final String TABLE_READ_TBL = "tbl_read_tbl";
    private static final String TABLE_READ_TBL_OLD_API = "tbl_read_tbl_old_api";
    private static final String TABLE_READ_TBL_ALL_OPTIONS = "tbl_read_tbl_all_options";
    private static final String TABLE_READ_TBL_PUSH_DOWN = "tbl_read_tbl_push_down";
    private static final String TABLE_READ_TBL_PUSH_DOWN_WITH_UNION_ALL =
            "tbl_read_tbl_push_down_with_union_all";

    @Test
    public void testSource() throws Exception {
        initializeTable(TABLE_READ);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier(DATABASE + "." + TABLE_READ)
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword());

        DorisSource<List<?>> source =
                DorisSource.<List<?>>builder()
                        .setDorisOptions(dorisBuilder.build())
                        .setDeserializer(new SimpleListDeserializationSchema())
                        .build();
        List<String> actual = new ArrayList<>();
        try (CloseableIterator<List<?>> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Doris Source")
                        .executeAndCollect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        List<String> expected = Arrays.asList("[doris, 18]", "[flink, 10]", "[apache, 12]");
        checkResult("testSource", expected.toArray(), actual.toArray());
    }

    @Test
    public void testOldSourceApi() throws Exception {
        initializeTable(TABLE_READ_OLD_API);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put("fenodes", getFenodes());
        properties.put("username", getDorisUsername());
        properties.put("password", getDorisPassword());
        properties.put("table.identifier", DATABASE + "." + TABLE_READ_OLD_API);
        DorisStreamOptions options = new DorisStreamOptions(properties);

        List<String> actual = new ArrayList<>();
        try (CloseableIterator<List<?>> iterator =
                env.addSource(
                                new DorisSourceFunction(
                                        options, new SimpleListDeserializationSchema()))
                        .executeAndCollect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        List<String> expected = Arrays.asList("[doris, 18]", "[flink, 10]", "[apache, 12]");
        checkResult("testOldSourceApi", expected.toArray(), actual.toArray());
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
                        getFenodes(),
                        DATABASE + "." + TABLE_READ_TBL,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("SELECT * FROM doris_source");

        List<String> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        String[] expected = new String[] {"+I[doris, 18]", "+I[flink, 10]", "+I[apache, 12]"};
        Assert.assertArrayEquals(expected, actual.toArray());

        // fitler query
        List<String> actualFilter = new ArrayList<>();
        TableResult tableResultFilter =
                tEnv.executeSql("SELECT * FROM doris_source where name='doris'");
        try (CloseableIterator<Row> iterator = tableResultFilter.collect()) {
            while (iterator.hasNext()) {
                actualFilter.add(iterator.next().toString());
            }
        }
        String[] expectedFilter = new String[] {"+I[doris, 18]"};
        checkResult("testTableSource", expectedFilter, actualFilter.toArray());
    }

    @Test
    public void testTableSourceOldApi() throws Exception {
        initializeTable(TABLE_READ_TBL_OLD_API);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_old_api ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'source.use-old-api' = 'true',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_READ_TBL_OLD_API,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("SELECT * FROM doris_source_old_api");

        List<String> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        String[] expected = new String[] {"+I[doris, 18]", "+I[flink, 10]", "+I[apache, 12]"};
        checkResult("testTableSourceOldApi", expected, actual.toArray());
    }

    @Test
    public void testTableSourceAllOptions() throws Exception {
        initializeTable(TABLE_READ_TBL_ALL_OPTIONS);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_all_options ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'source.use-old-api' = 'true',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'doris.request.retries' = '3',"
                                + " 'doris.request.connect.timeout' = '60s',"
                                + " 'doris.request.read.timeout' = '60s',"
                                + " 'doris.request.query.timeout' = '3600s',"
                                + " 'doris.request.tablet.size' = '2',"
                                + " 'doris.batch.size' = '1024',"
                                + " 'doris.exec.mem.limit' = '2048mb',"
                                + " 'doris.deserialize.arrow.async' = 'true',"
                                + " 'doris.deserialize.queue.size' = '32'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_READ_TBL_ALL_OPTIONS,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("SELECT * FROM doris_source_all_options");

        List<String> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        String[] expected = new String[] {"+I[doris, 18]", "+I[flink, 10]", "+I[apache, 12]"};
        checkResult("testTableSourceAllOptions", expected, actual.toArray());
    }

    @Test
    public void testTableSourceFilterAndProjectionPushDown() throws Exception {
        initializeTable(TABLE_READ_TBL_PUSH_DOWN);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_filter_and_projection_push_down ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_READ_TBL_PUSH_DOWN,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sourceDDL);
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT age FROM doris_source_filter_and_projection_push_down where age = '18'");

        List<String> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        String[] expected = new String[] {"+I[18]"};
        checkResult("testTableSourceFilterAndProjectionPushDown", expected, actual.toArray());
    }

    @Test
    public void testTableSourceFilterWithUnionAll() {
        LOG.info("starting to execute testTableSourceFilterWithUnionAll case.");
        initializeTable(TABLE_READ_TBL_PUSH_DOWN_WITH_UNION_ALL);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_filter_with_union_all ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_READ_TBL_PUSH_DOWN_WITH_UNION_ALL,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sourceDDL);
        String querySql =
                "  SELECT * FROM doris_source_filter_with_union_all where age = '18'"
                        + " UNION ALL "
                        + "SELECT * FROM doris_source_filter_with_union_all where age = '10'";
        TableResult tableResult = tEnv.executeSql(querySql);

        List<String> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        } catch (Exception e) {
            LOG.error("Failed to execute sql. sql={}", querySql, e);
            throw new DorisRuntimeException(e);
        }
        Set<String> expected = new HashSet<>(Arrays.asList("+I[flink, 10]", "+I[doris, 18]"));
        for (String a : actual) {
            Assert.assertTrue(expected.contains(a));
        }
    }

    private void checkResult(String testName, Object[] expected, Object[] actual) {
        LOG.info(
                "Checking DorisSourceITCase result. testName={}, actual={}, expected={}",
                testName,
                actual,
                expected);
        Assert.assertArrayEquals(expected, actual);
    }

    private void initializeTable(String table) {
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(),
                LOG,
                String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
                String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
                String.format(
                        "CREATE TABLE %s.%s ( \n"
                                + "`name` varchar(256),\n"
                                + "`age` int\n"
                                + ") DISTRIBUTED BY HASH(`name`) BUCKETS 1\n"
                                + "PROPERTIES (\n"
                                + "\"replication_num\" = \"1\"\n"
                                + ")\n",
                        DATABASE, table),
                String.format("insert into %s.%s  values ('doris',18)", DATABASE, table),
                String.format("insert into %s.%s  values ('flink',10)", DATABASE, table),
                String.format("insert into %s.%s  values ('apache',12)", DATABASE, table));
    }
}
