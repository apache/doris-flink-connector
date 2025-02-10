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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

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
    private static final String TABLE_READ_TBL_TIMESTAMP_PUSH_DOWN =
            "tbl_read_tbl_timestamp_push_down";
    private static final String TABLE_READ_TBL_PUSH_DOWN_WITH_UNION_ALL =
            "tbl_read_tbl_push_down_with_union_all";
    static final String TABLE_CSV_JM = "tbl_csv_jm_source";
    static final String TABLE_CSV_TM = "tbl_csv_tm_source";
    private static final String TABLE_READ_TBL_PUSH_DOWN_WITH_UNION_ALL_NOT_EQ_FILTER =
            "tbl_read_tbl_push_down_with_union_all_not_eq_filter";
    private static final String TABLE_READ_TBL_PUSH_DOWN_WITH_FILTER_QUERY =
            "tbl_read_tbl_push_down_with_filter_query";

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @Test
    public void testSource() throws Exception {
        initializeTable(TABLE_READ);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(DEFAULT_PARALLELISM);
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
        checkResultInAnyOrder("testSource", expected.toArray(), actual.toArray());
    }

    @Test
    public void testOldSourceApi() throws Exception {
        initializeTable(TABLE_READ_OLD_API);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
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
        checkResultInAnyOrder("testOldSourceApi", expected.toArray(), actual.toArray());
    }

    @Test
    public void testTableSource() throws Exception {
        initializeTable(TABLE_READ_TBL);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
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
        assertEqualsInAnyOrder(Arrays.asList(expected), Arrays.asList(actual.toArray()));

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
        checkResultInAnyOrder("testTableSource", expectedFilter, actualFilter.toArray());
    }

    @Test
    public void testTableSourceOldApi() throws Exception {
        initializeTable(TABLE_READ_TBL_OLD_API);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_old_api ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
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
        checkResultInAnyOrder("testTableSourceOldApi", expected, actual.toArray());
    }

    @Test
    public void testTableSourceAllOptions() throws Exception {
        initializeTable(TABLE_READ_TBL_ALL_OPTIONS);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_all_options ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
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
        checkResultInAnyOrder("testTableSourceAllOptions", expected, actual.toArray());
    }

    @Test
    public void testTableSourceFilterAndProjectionPushDown() throws Exception {
        initializeTable(TABLE_READ_TBL_PUSH_DOWN);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_filter_and_projection_push_down ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
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
        checkResultInAnyOrder(
                "testTableSourceFilterAndProjectionPushDown", expected, actual.toArray());
    }

    @Test
    public void testTableSourceTimestampFilterAndProjectionPushDown() throws Exception {
        initializeTimestampTable(TABLE_READ_TBL_TIMESTAMP_PUSH_DOWN);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_datetime_filter_and_projection_push_down ("
                                + "`id` int ,\n"
                                + "`name` timestamp,\n"
                                + "`age` int,\n"
                                + "`birthday` timestamp,\n"
                                + "`brilliant_time` timestamp(6)\n"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_READ_TBL_TIMESTAMP_PUSH_DOWN,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sourceDDL);

        List<String> actualProjectionResult =
                generateExecuteSQLResult(
                        tEnv,
                        "SELECT id,birthday,brilliant_time FROM doris_source_datetime_filter_and_projection_push_down order by id");

        List<String> actualPushDownDatetimeResult =
                generateExecuteSQLResult(
                        tEnv,
                        "SELECT id,birthday FROM doris_source_datetime_filter_and_projection_push_down where birthday >= '2023-01-01 00:00:00' order by id");
        List<String> actualPushDownMicrosecondResult =
                generateExecuteSQLResult(
                        tEnv,
                        "SELECT id,brilliant_time FROM doris_source_datetime_filter_and_projection_push_down where brilliant_time > '2023-01-01 00:00:00.000001' order by id");
        List<String> actualPushDownNanosecondResult =
                generateExecuteSQLResult(
                        tEnv,
                        "SELECT id,brilliant_time FROM doris_source_datetime_filter_and_projection_push_down where brilliant_time > '2023-01-01 00:00:00.000009001' order by id");

        List<String> actualPushDownNanosecondRoundDownResult =
                generateExecuteSQLResult(
                        tEnv,
                        "SELECT id,brilliant_time FROM doris_source_datetime_filter_and_projection_push_down where brilliant_time >= '2023-01-01 00:00:00.999999001' order by id");
        List<String> actualPushDownNanosecondRoundUpResult =
                generateExecuteSQLResult(
                        tEnv,
                        "SELECT id,brilliant_time FROM doris_source_datetime_filter_and_projection_push_down where brilliant_time >= '2023-01-01 00:00:00.999999999' order by id");

        String[] expectedProjectionResult =
                new String[] {
                    "+I[1, 2023-01-01T00:00, 2023-01-01T00:00:00.000001]",
                    "+I[2, 2023-01-01T00:00:01, 2023-01-01T00:00:00.005]",
                    "+I[3, 2023-01-01T00:00:02, 2023-01-01T00:00:00.000009]",
                    "+I[4, 2023-01-01T00:00:02, 2023-01-01T00:00:00.999999]",
                    "+I[5, 2023-01-01T00:00:02, 2023-01-01T00:00:00.999999]",
                    "+I[6, 2023-01-01T00:00:02, 2023-01-01T00:00:01]"
                };
        String[] expectedPushDownDatetimeResult =
                new String[] {
                    "+I[1, 2023-01-01T00:00]",
                    "+I[2, 2023-01-01T00:00:01]",
                    "+I[3, 2023-01-01T00:00:02]",
                    "+I[4, 2023-01-01T00:00:02]",
                    "+I[5, 2023-01-01T00:00:02]",
                    "+I[6, 2023-01-01T00:00:02]"
                };
        String[] expectedPushDownWithMicrosecondResult =
                new String[] {
                    "+I[2, 2023-01-01T00:00:00.005]",
                    "+I[3, 2023-01-01T00:00:00.000009]",
                    "+I[4, 2023-01-01T00:00:00.999999]",
                    "+I[5, 2023-01-01T00:00:00.999999]",
                    "+I[6, 2023-01-01T00:00:01]"
                };

        String[] expectedPushDownWithNanosecondResult =
                new String[] {
                    "+I[2, 2023-01-01T00:00:00.005]",
                    "+I[4, 2023-01-01T00:00:00.999999]",
                    "+I[5, 2023-01-01T00:00:00.999999]",
                    "+I[6, 2023-01-01T00:00:01]"
                };

        String[] expectedPushDownWithNanosecondRoundDownResult =
                new String[] {
                    "+I[4, 2023-01-01T00:00:00.999999]",
                    "+I[5, 2023-01-01T00:00:00.999999]",
                    "+I[6, 2023-01-01T00:00:01]"
                };

        String[] expectedPushDownWithNanosecondRoundUpResult =
                new String[] {
                    "+I[4, 2023-01-01T00:00:00.999999]",
                    "+I[5, 2023-01-01T00:00:00.999999]",
                    "+I[6, 2023-01-01T00:00:01]"
                };
        checkResultInAnyOrder(
                "testTableSourceTimestampFilterAndProjectionPushDown",
                expectedProjectionResult,
                actualProjectionResult.toArray());
        checkResultInAnyOrder(
                "testTableSourceTimestampFilterAndProjectionPushDown",
                expectedPushDownDatetimeResult,
                actualPushDownDatetimeResult.toArray());
        checkResultInAnyOrder(
                "testTableSourceTimestampFilterAndProjectionPushDown",
                expectedPushDownWithMicrosecondResult,
                actualPushDownMicrosecondResult.toArray());
        checkResultInAnyOrder(
                "testTableSourceTimestampFilterAndProjectionPushDown",
                expectedPushDownWithNanosecondResult,
                actualPushDownNanosecondResult.toArray());
        checkResultInAnyOrder(
                "testTableSourceTimestampFilterAndProjectionPushDown",
                expectedPushDownWithNanosecondRoundDownResult,
                actualPushDownNanosecondRoundDownResult.toArray());
        checkResultInAnyOrder(
                "testTableSourceTimestampFilterAndProjectionPushDown",
                expectedPushDownWithNanosecondRoundUpResult,
                actualPushDownNanosecondRoundUpResult.toArray());
    }

    @Test
    public void testTableSourceFilterWithUnionAll() throws Exception {
        LOG.info("starting to execute testTableSourceFilterWithUnionAll case.");
        initializeTable(TABLE_READ_TBL_PUSH_DOWN_WITH_UNION_ALL);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_filter_with_union_all ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
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
        }

        String[] expected = new String[] {"+I[flink, 10]", "+I[doris, 18]"};
        checkResultInAnyOrder("testTableSourceFilterWithUnionAll", expected, actual.toArray());
    }

    @Test
    public void testTableSourceFilterWithFilterQuery() throws Exception {
        LOG.info("starting to execute testTableSourceFilterWithFilterQuery case.");
        // init doris table
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(),
                LOG,
                String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
                String.format(
                        "DROP TABLE IF EXISTS %s.%s",
                        DATABASE, TABLE_READ_TBL_PUSH_DOWN_WITH_FILTER_QUERY),
                String.format(
                        "CREATE TABLE %s.%s ( \n"
                                + "`name` varchar(256),\n"
                                + "`dt` date,\n"
                                + "`age` int\n"
                                + ") DISTRIBUTED BY HASH(`name`) BUCKETS 10\n"
                                + "PROPERTIES (\n"
                                + "\"replication_num\" = \"1\"\n"
                                + ")\n",
                        DATABASE, TABLE_READ_TBL_PUSH_DOWN_WITH_FILTER_QUERY),
                String.format(
                        "insert into %s.%s  values ('doris',date_sub(now(),INTERVAL 7 DAY), 18)",
                        DATABASE, TABLE_READ_TBL_PUSH_DOWN_WITH_FILTER_QUERY),
                String.format(
                        "insert into %s.%s  values ('flink','2025-02-10', 10)",
                        DATABASE, TABLE_READ_TBL_PUSH_DOWN_WITH_FILTER_QUERY),
                String.format(
                        "insert into %s.%s  values ('apache',now(), 12)",
                        DATABASE, TABLE_READ_TBL_PUSH_DOWN_WITH_FILTER_QUERY));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_filter_with_filter_query ("
                                + " name STRING,"
                                + " dt DATE,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'doris.filter.query' = ' (dt = DATE_FORMAT(TIMESTAMPADD(DAY , -7, NOW()), ''yyyy-MM-dd'')) '"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_READ_TBL_PUSH_DOWN_WITH_FILTER_QUERY,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sourceDDL);
        String querySql =
                "  SELECT * FROM doris_source_filter_with_filter_query where name = 'doris' and age > 2";
        TableResult tableResult = tEnv.executeSql(querySql);

        List<String> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }

        String nowDate =
                LocalDate.now().minusDays(7).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        String[] expected = new String[] {"+I[doris, " + nowDate + ", 18]"};
        checkResultInAnyOrder("testTableSourceFilterWithFilterQuery", expected, actual.toArray());
    }

    @Test
    public void testTableSourceFilterWithUnionAllNotEqualFilter() throws Exception {
        LOG.info("starting to execute testTableSourceFilterWithUnionAllNotEqualFilter case.");
        initializeTable(TABLE_READ_TBL_PUSH_DOWN_WITH_UNION_ALL_NOT_EQ_FILTER);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_filter_with_union_all ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_READ_TBL_PUSH_DOWN_WITH_UNION_ALL_NOT_EQ_FILTER,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sourceDDL);
        String querySql =
                "  SELECT * FROM doris_source_filter_with_union_all where name = 'doris'"
                        + " UNION ALL "
                        + "SELECT * FROM doris_source_filter_with_union_all where name in ('error','flink')";
        TableResult tableResult = tEnv.executeSql(querySql);

        List<String> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }

        String[] expected = new String[] {"+I[flink, 10]", "+I[doris, 18]"};
        checkResultInAnyOrder(
                "testTableSourceFilterWithUnionAllNotEqualFilter", expected, actual.toArray());
    }

    @Test
    public void testJobManagerFailoverSource() throws Exception {
        LOG.info("start to test JobManagerFailoverSource.");
        initializeTableWithData(TABLE_CSV_JM);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_jm ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_CSV_JM,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from doris_source_jm");
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();

        List<String> expectedData = getExpectedData();
        if (iterator.hasNext()) {
            LOG.info("trigger jobmanager failover...");
            triggerFailover(
                    FailoverType.JM,
                    jobId,
                    miniClusterResource.getMiniCluster(),
                    () -> sleepMs(100));
        }
        List<String> actual = fetchRows(iterator);
        LOG.info("actual data: {}, expected: {}", actual, expectedData);
        Assert.assertTrue(actual.size() >= expectedData.size());
        Assert.assertTrue(actual.containsAll(expectedData));
    }

    private static List<String> getExpectedData() {
        String[] expected =
                new String[] {
                    "+I[101, 1]",
                    "+I[102, 1]",
                    "+I[103, 1]",
                    "+I[201, 2]",
                    "+I[202, 2]",
                    "+I[203, 2]",
                    "+I[301, 3]",
                    "+I[302, 3]",
                    "+I[303, 3]",
                    "+I[401, 4]",
                    "+I[402, 4]",
                    "+I[403, 4]",
                    "+I[501, 5]",
                    "+I[502, 5]",
                    "+I[503, 5]",
                    "+I[601, 6]",
                    "+I[602, 6]",
                    "+I[603, 6]",
                    "+I[701, 7]",
                    "+I[702, 7]",
                    "+I[703, 7]",
                    "+I[801, 8]",
                    "+I[802, 8]",
                    "+I[803, 8]",
                    "+I[901, 9]",
                    "+I[902, 9]",
                    "+I[903, 9]"
                };
        return Arrays.asList(expected);
    }

    @Test
    public void testTaskManagerFailoverSource() throws Exception {
        LOG.info("start to test TaskManagerFailoverSource.");
        initializeTableWithData(TABLE_CSV_TM);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_source_tm ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_CSV_TM,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from doris_source_tm");
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();
        List<String> expectedData = getExpectedData();
        if (iterator.hasNext()) {
            LOG.info("trigger taskmanager failover...");
            triggerFailover(
                    FailoverType.TM,
                    jobId,
                    miniClusterResource.getMiniCluster(),
                    () -> sleepMs(100));
        }

        List<String> actual = fetchRows(iterator);
        LOG.info("actual data: {}, expected: {}", actual, expectedData);
        Assert.assertTrue(actual.size() >= expectedData.size());
        Assert.assertTrue(actual.containsAll(expectedData));
    }

    private void checkResult(String testName, Object[] expected, Object[] actual) {
        LOG.info(
                "Checking DorisSourceITCase result. testName={}, actual={}, expected={}",
                testName,
                actual,
                expected);
        Assert.assertArrayEquals(expected, actual);
    }

    private void checkResultInAnyOrder(String testName, Object[] expected, Object[] actual) {
        LOG.info(
                "Checking DorisSourceITCase result. testName={}, actual={}, expected={}",
                testName,
                actual,
                expected);
        assertEqualsInAnyOrder(Arrays.asList(expected), Arrays.asList(actual));
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
                                + ") DISTRIBUTED BY HASH(`name`) BUCKETS 10\n"
                                + "PROPERTIES (\n"
                                + "\"replication_num\" = \"1\"\n"
                                + ")\n",
                        DATABASE, table),
                String.format("insert into %s.%s  values ('doris',18)", DATABASE, table),
                String.format("insert into %s.%s  values ('flink',10)", DATABASE, table),
                String.format("insert into %s.%s  values ('apache',12)", DATABASE, table));
    }

    private void initializeTimestampTable(String table) {
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(),
                LOG,
                String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
                String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
                String.format(
                        "CREATE TABLE %s.%s ( \n"
                                + "`id` int,\n"
                                + "`name` varchar(256),\n"
                                + "`age` int,\n"
                                + "`birthday` datetime,\n"
                                + "`brilliant_time` datetime(6),\n"
                                + ") DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                                + "PROPERTIES (\n"
                                + "\"replication_num\" = \"1\"\n"
                                + ")\n",
                        DATABASE, table),
                String.format(
                        "insert into %s.%s  values (1,'Kevin',54,'2023-01-01T00:00:00','2023-01-01T00:00:00.000001')",
                        DATABASE, table),
                String.format(
                        "insert into %s.%s  values (2,'Dylan',25,'2023-01-01T00:00:01','2023-01-01T00:00:00.005000')",
                        DATABASE, table),
                String.format(
                        "insert into %s.%s  values (3,'Darren',65,'2023-01-01T00:00:02','2023-01-01T00:00:00.000009')",
                        DATABASE, table),
                String.format(
                        "insert into %s.%s  values (4,'Warren',75,'2023-01-01T00:00:02','2023-01-01T00:00:00.999999')",
                        DATABASE, table),
                String.format(
                        "insert into %s.%s  values (5,'Simba',75,'2023-01-01T00:00:02','2023-01-01T00:00:00.999999001')",
                        DATABASE, table),
                String.format(
                        "insert into %s.%s  values (6,'Jimmy',75,'2023-01-01T00:00:02','2023-01-01T00:00:00.999999999')",
                        DATABASE, table));
    }

    private void initializeTableWithData(String table) {
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(),
                LOG,
                String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
                String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
                String.format(
                        "CREATE TABLE %s.%s ( \n"
                                + "`name` varchar(256),\n"
                                + "`age` int\n"
                                + ") DISTRIBUTED BY HASH(`name`) BUCKETS 10\n"
                                + "PROPERTIES (\n"
                                + "\"replication_num\" = \"1\"\n"
                                + ")\n",
                        DATABASE, table),
                String.format(
                        "insert into %s.%s  values ('101',1),('102',1),('103',1)", DATABASE, table),
                String.format(
                        "insert into %s.%s  values ('201',2),('202',2),('203',2)", DATABASE, table),
                String.format(
                        "insert into %s.%s  values ('301',3),('302',3),('303',3)", DATABASE, table),
                String.format(
                        "insert into %s.%s  values ('401',4),('402',4),('403',4)", DATABASE, table),
                String.format(
                        "insert into %s.%s  values ('501',5),('502',5),('503',5)", DATABASE, table),
                String.format(
                        "insert into %s.%s  values ('601',6),('602',6),('603',6)", DATABASE, table),
                String.format(
                        "insert into %s.%s  values ('701',7),('702',7),('703',7)", DATABASE, table),
                String.format(
                        "insert into %s.%s  values ('801',8),('802',8),('803',8)", DATABASE, table),
                String.format(
                        "insert into %s.%s  values ('901',9),('902',9),('903',9)",
                        DATABASE, table));
    }

    private static List<String> fetchRows(Iterator<Row> iter) {
        List<String> rows = new ArrayList<>();
        while (iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
        }
        return rows;
    }

    private List<String> generateExecuteSQLResult(StreamTableEnvironment tEnv, String executeSql)
            throws Exception {
        List<String> actualResultList = new ArrayList<>();
        TableResult tableResult = tEnv.executeSql(executeSql);
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {

                actualResultList.add(iterator.next().toString());
            }
        }
        return actualResultList;
    }
}
