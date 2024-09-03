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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.sink.DorisSink.Builder;
import org.apache.doris.flink.sink.batch.DorisBatchSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.doris.flink.utils.MockSource;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.flink.api.common.JobStatus.RUNNING;

/** DorisSink ITCase with csv and arrow format. */
public class DorisSinkITCase extends AbstractITCaseService {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkITCase.class);
    static final String DATABASE = "test_sink";
    static final String TABLE_CSV = "tbl_csv";
    static final String TABLE_JSON = "tbl_json";
    static final String TABLE_JSON_TBL = "tbl_json_tbl";
    static final String TABLE_CSV_BATCH_TBL = "tbl_csv_batch_tbl";
    static final String TABLE_CSV_BATCH_DS = "tbl_csv_batch_DS";
    static final String TABLE_GROUP_COMMIT = "tbl_group_commit";
    static final String TABLE_GZ_FORMAT = "tbl_gz_format";
    static final String TABLE_CSV_JM = "tbl_csv_jm";
    static final String TABLE_CSV_TM = "tbl_csv_tm";

    @Test
    public void testSinkCsvFormat() throws Exception {
        initializeTable(TABLE_CSV);
        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix(UUID.randomUUID().toString()).setStreamLoadProp(properties);
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier(DATABASE + "." + TABLE_CSV)
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword());
        submitJob(dorisBuilder.build(), executionBuilder.build(), new String[] {"doris,1"});

        Thread.sleep(10000);
        List<String> expected = Arrays.asList("doris,1");
        String query = String.format("select name,age from %s.%s order by 1", DATABASE, TABLE_CSV);
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, query, 2);
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

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix(UUID.randomUUID().toString()).setStreamLoadProp(properties);
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier(DATABASE + "." + TABLE_JSON)
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword());

        submitJob(
                dorisBuilder.build(),
                executionBuilder.build(),
                new String[] {
                    new ObjectMapper().writeValueAsString(row1),
                    new ObjectMapper().writeValueAsString(row2)
                });

        Thread.sleep(10000);
        List<String> expected = Arrays.asList("doris1,1", "doris2,2");
        String query = String.format("select name,age from %s.%s order by 1", DATABASE, TABLE_JSON);
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, query, 2);
    }

    private void submitJob(
            DorisOptions dorisOptions, DorisExecutionOptions executionOptions, String[] records)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        Builder<String> builder = DorisSink.builder();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();

        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisOptions);

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
                                + " 'benodes' = '%s',"
                                + " 'auto-redirect' = 'false',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'sink.buffer-size' = '1MB',"
                                + " 'sink.buffer-count' = '3',"
                                + " 'sink.max-retries' = '1',"
                                + " 'sink.enable-2pc' = 'true',"
                                + " 'sink.use-cache' = 'true',"
                                + " 'sink.enable-delete' = 'false',"
                                + " 'sink.ignore.update-before' = 'true',"
                                + " 'sink.properties.format' = 'json',"
                                + " 'sink.properties.read_json_by_line' = 'true',"
                                + " 'sink.label-prefix' = 'doris_sink"
                                + UUID.randomUUID()
                                + "'"
                                + ")",
                        getFenodes(),
                        getBenodes(),
                        DATABASE + "." + TABLE_JSON_TBL,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql("INSERT INTO doris_sink SELECT 'doris',1 union all SELECT 'flink',2");

        Thread.sleep(10000);
        List<String> expected = Arrays.asList("doris,1", "flink,2");
        String query =
                String.format("select name,age from %s.%s order by 1", DATABASE, TABLE_JSON_TBL);
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, query, 2);
    }

    @Test
    public void testTableBatch() throws Exception {
        initializeTable(TABLE_CSV_BATCH_TBL);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sinkDDL =
                String.format(
                        "CREATE TABLE doris_sink_batch ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'sink.label-prefix' = '"
                                + UUID.randomUUID()
                                + "',"
                                + " 'sink.properties.column_separator' = '\\x01',"
                                + " 'sink.properties.line_delimiter' = '\\x02',"
                                + " 'sink.ignore.update-before' = 'false',"
                                + " 'sink.enable.batch-mode' = 'true',"
                                + " 'sink.enable-delete' = 'true',"
                                + " 'sink.flush.queue-size' = '2',"
                                + " 'sink.buffer-flush.max-rows' = '10000',"
                                + " 'sink.buffer-flush.max-bytes' = '10MB',"
                                + " 'sink.buffer-flush.interval' = '1s'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_CSV_BATCH_TBL,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql("INSERT INTO doris_sink_batch SELECT 'doris',1 union all SELECT 'flink',2");

        Thread.sleep(20000);
        List<String> expected = Arrays.asList("doris,1", "flink,2");
        String query =
                String.format(
                        "select name,age from %s.%s order by 1", DATABASE, TABLE_CSV_BATCH_TBL);
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, query, 2);
    }

    @Test
    public void testDataStreamBatch() throws Exception {
        initializeTable(TABLE_CSV_BATCH_DS);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DorisBatchSink.Builder<String> builder = DorisBatchSink.builder();

        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier(DATABASE + "." + TABLE_CSV_BATCH_DS)
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword());
        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setLabelPrefix(UUID.randomUUID().toString())
                .setStreamLoadProp(properties)
                .setBufferFlushMaxBytes(10485760)
                .setBufferFlushMaxRows(10000)
                .setBufferFlushIntervalMs(1000);

        builder.setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        env.fromElements("doris,1", "flink,2").sinkTo(builder.build());
        env.execute();

        Thread.sleep(20000);
        List<String> expected = Arrays.asList("doris,1", "flink,2");
        String query =
                String.format(
                        "select name,age from %s.%s order by 1", DATABASE, TABLE_CSV_BATCH_DS);
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, query, 2);
    }

    @Test
    public void testTableGroupCommit() throws Exception {
        initializeTable(TABLE_GROUP_COMMIT);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sinkDDL =
                String.format(
                        "CREATE TABLE doris_group_commit_sink ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'sink.label-prefix' = '"
                                + UUID.randomUUID()
                                + "',"
                                + " 'sink.properties.column_separator' = '\\x01',"
                                + " 'sink.properties.line_delimiter' = '\\x02',"
                                + " 'sink.properties.group_commit' = 'sync_mode',"
                                + " 'sink.ignore.update-before' = 'false',"
                                + " 'sink.enable.batch-mode' = 'true',"
                                + " 'sink.enable-delete' = 'true',"
                                + " 'sink.flush.queue-size' = '2',"
                                + " 'sink.buffer-flush.max-rows' = '10000',"
                                + " 'sink.buffer-flush.max-bytes' = '10MB',"
                                + " 'sink.buffer-flush.interval' = '1s'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_GROUP_COMMIT,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(
                "INSERT INTO doris_group_commit_sink SELECT 'doris',1 union all  SELECT 'group_commit',2 union all  SELECT 'flink',3");

        Thread.sleep(25000);
        List<String> expected = Arrays.asList("doris,1", "flink,3", "group_commit,2");
        String query =
                String.format(
                        "select name,age from %s.%s order by 1", DATABASE, TABLE_GROUP_COMMIT);
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, query, 2);
    }

    @Test
    public void testTableGzFormat() throws Exception {
        initializeTable(TABLE_GZ_FORMAT);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sinkDDL =
                String.format(
                        "CREATE TABLE doris_gz_format_sink ("
                                + " name STRING,"
                                + " age INT"
                                + ") WITH ("
                                + " 'connector' = 'doris',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'sink.label-prefix' = '"
                                + UUID.randomUUID()
                                + "',"
                                + " 'sink.properties.column_separator' = '\\x01',"
                                + " 'sink.properties.line_delimiter' = '\\x02',"
                                + " 'sink.properties.compress_type' = 'gz'"
                                + ")",
                        getFenodes(),
                        DATABASE + "." + TABLE_GZ_FORMAT,
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(
                "INSERT INTO doris_gz_format_sink SELECT 'doris',1 union all  SELECT 'flink',2");

        Thread.sleep(25000);
        List<String> expected = Arrays.asList("doris,1", "flink,2");
        String query =
                String.format("select name,age from %s.%s order by 1", DATABASE, TABLE_GZ_FORMAT);
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, query, 2);
    }

    @Test
    public void testJobManagerFailoverSink() throws Exception {
        LOG.info("start to test JobManagerFailoverSink.");
        initializeFailoverTable(TABLE_CSV_JM);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));

        DorisSink.Builder<String> builder = DorisSink.builder();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();

        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier(DATABASE + "." + TABLE_CSV_JM)
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword());
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");
        executionBuilder
                .setLabelPrefix(UUID.randomUUID().toString())
                .setStreamLoadProp(properties)
                .setUseCache(true);

        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        env.addSource(new MockSource(5)).sinkTo(builder.build());
        JobClient jobClient = env.executeAsync();
        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(10)));
        JobID jobID = jobClient.getJobID();
        // wait checkpoint 2 times
        Thread.sleep(20000);
        LOG.info("trigger jobmanager failover...");
        triggerFailover(
                FailoverType.JM, jobID, miniClusterResource.getMiniCluster(), () -> sleepMs(100));

        LOG.info("Waiting the JobManagerFailoverSink job to be finished. jobId={}", jobID);
        waitForJobStatus(
                jobClient,
                Collections.singletonList(FINISHED),
                Deadline.fromNow(Duration.ofSeconds(120)));

        LOG.info("Will check job manager failover sink result.");
        List<String> expected =
                Arrays.asList("1,0", "1,1", "2,0", "2,1", "3,0", "3,1", "4,0", "4,1", "5,0", "5,1");
        String query =
                String.format("select id,task_id from %s.%s order by 1,2", DATABASE, TABLE_CSV_JM);
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, query, 2);
    }

    @Test
    public void testTaskManagerFailoverSink() throws Exception {
        LOG.info("start to test TaskManagerFailoverSink.");
        initializeFailoverTable(TABLE_CSV_TM);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));

        DorisSink.Builder<String> builder = DorisSink.builder();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();

        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier(DATABASE + "." + TABLE_CSV_TM)
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword());
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");
        executionBuilder.setLabelPrefix(UUID.randomUUID().toString()).setStreamLoadProp(properties);

        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        env.addSource(new MockSource(5)).sinkTo(builder.build());
        JobClient jobClient = env.executeAsync();
        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(10)));
        JobID jobID = jobClient.getJobID();
        // wait checkpoint 2 times
        Thread.sleep(20000);
        LOG.info("trigger taskmanager failover...");
        triggerFailover(
                FailoverType.TM, jobID, miniClusterResource.getMiniCluster(), () -> sleepMs(100));

        LOG.info("Waiting the TaskManagerFailoverSink job to be finished. jobId={}", jobID);
        waitForJobStatus(
                jobClient,
                Collections.singletonList(FINISHED),
                Deadline.fromNow(Duration.ofSeconds(120)));

        LOG.info("Will check task manager failover sink result.");
        List<String> expected =
                Arrays.asList("1,0", "1,1", "2,0", "2,1", "3,0", "3,1", "4,0", "4,1", "5,0", "5,1");
        String query =
                String.format("select id,task_id from %s.%s order by 1,2", DATABASE, TABLE_CSV_TM);
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, query, 2);
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
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
                        DATABASE, table));
    }

    private void initializeFailoverTable(String table) {
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(),
                LOG,
                String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
                String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
                String.format(
                        "CREATE TABLE %s.%s ( \n"
                                + "`id` int,\n"
                                + "`task_id` int\n"
                                + ") DISTRIBUTED BY HASH(`id`) BUCKETS 1\n"
                                + "PROPERTIES (\n"
                                + "\"replication_num\" = \"1\"\n"
                                + ")\n",
                        DATABASE, table));
    }
}
