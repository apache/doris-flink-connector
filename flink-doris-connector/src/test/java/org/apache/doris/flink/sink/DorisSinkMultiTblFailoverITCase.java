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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.sink.batch.RecordWithMeta;
import org.apache.doris.flink.sink.writer.serializer.RecordWithMetaSerializer;
import org.apache.doris.flink.utils.MockMultiTableSource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.flink.api.common.JobStatus.RUNNING;

/** DorisSink abnormal case of multi-table writing */
@RunWith(Parameterized.class)
public class DorisSinkMultiTblFailoverITCase extends AbstractITCaseService {
    private static final Logger LOG =
            LoggerFactory.getLogger(DorisSinkMultiTblFailoverITCase.class);
    static final String DATABASE = "test_multi_failover_sink";
    static final String TABLE_MULTI_CSV = "tbl_multi_csv";
    static final String TABLE_MULTI_CSV_NO_EXIST_TBL = "tbl_multi_csv_no_exist";
    private final boolean batchMode;

    public DorisSinkMultiTblFailoverITCase(boolean batchMode) {
        this.batchMode = batchMode;
    }

    @Parameterized.Parameters(name = "batchMode: {0}")
    public static Object[] parameters() {
        return new Object[][] {new Object[] {false}, new Object[] {true}};
    }

    /**
     * In an extreme case, during a checkpoint, a piece of data written is bufferCount*bufferSize
     */
    @Test
    public void testTableNotExistCornerCase() throws Exception {
        LOG.info("Start to testTableNotExistCornerCase");
        dropDatabase();
        dropTable(TABLE_MULTI_CSV_NO_EXIST_TBL);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointTimeout(300 * 1000);
        env.setParallelism(1);
        int checkpointIntervalMs = 10000;
        env.enableCheckpointing(checkpointIntervalMs);

        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("format", "csv");
        DorisSink.Builder<RecordWithMeta> builder = DorisSink.builder();
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setLabelPrefix(UUID.randomUUID().toString())
                .enable2PC()
                .setBatchMode(batchMode)
                .setFlushQueueSize(1)
                .setBufferSize(1)
                .setBufferCount(3)
                .setCheckInterval(0)
                .setStreamLoadProp(properties);
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier("")
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword());

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new RecordWithMetaSerializer())
                .setDorisOptions(dorisBuilder.build());

        DataStreamSource<RecordWithMeta> mockSource =
                env.addSource(
                        new SourceFunction<RecordWithMeta>() {
                            @Override
                            public void run(SourceContext<RecordWithMeta> ctx) throws Exception {
                                RecordWithMeta record3 =
                                        new RecordWithMeta(
                                                DATABASE, TABLE_MULTI_CSV_NO_EXIST_TBL, "1,3");
                                ctx.collect(record3);
                            }

                            @Override
                            public void cancel() {}
                        });

        mockSource.sinkTo(builder.build());
        JobClient jobClient = env.executeAsync();
        CompletableFuture<JobStatus> jobStatus = jobClient.getJobStatus();
        LOG.info("Job status: {}", jobStatus);

        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(60)));
        // wait checkpoint failure
        List<JobStatus> errorStatus =
                Arrays.asList(
                        JobStatus.FAILING,
                        JobStatus.CANCELLING,
                        JobStatus.CANCELED,
                        JobStatus.FAILED,
                        JobStatus.RESTARTING);

        waitForJobStatus(jobClient, errorStatus, Deadline.fromNow(Duration.ofSeconds(30)));

        LOG.info("start to create add table");
        initializeTable(TABLE_MULTI_CSV_NO_EXIST_TBL);

        LOG.info("wait job restart success");
        // wait table restart success
        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(60)));

        LOG.info("wait job running finished");
        waitForJobStatus(
                jobClient,
                Collections.singletonList(FINISHED),
                Deadline.fromNow(Duration.ofSeconds(60)));

        String queryRes =
                String.format(
                        "select id,task_id from %s.%s ", DATABASE, TABLE_MULTI_CSV_NO_EXIST_TBL);
        List<String> expected = Arrays.asList("1,3");

        if (!batchMode) {
            ContainerUtils.checkResult(
                    getDorisQueryConnection(), LOG, expected, queryRes, 2, false);
        } else {
            List<String> actualResult =
                    ContainerUtils.getResult(getDorisQueryConnection(), LOG, expected, queryRes, 2);
            LOG.info("actual size: {}, expected size: {}", actualResult.size(), expected.size());
            Assert.assertTrue(
                    actualResult.size() >= expected.size() && actualResult.containsAll(expected));
        }
    }

    /**
     * Four exceptions are simulated in one job 1. Add a table that does not exist 2. flink
     * checkpoint failed 3. doris cluster restart 4. stream load fail
     */
    @Test
    public void testMultiTblFailoverSink() throws Exception {
        LOG.info("Start to testMultiTblFailoverSink");
        int totalTblNum = 3;
        for (int i = 1; i <= totalTblNum; i++) {
            String tableName = TABLE_MULTI_CSV + i;
            initializeTable(tableName);
        }
        int newTableIndex = totalTblNum + 1;
        dropTable(TABLE_MULTI_CSV + newTableIndex);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        int checkpointIntervalMs = 5000;
        env.enableCheckpointing(checkpointIntervalMs);

        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("format", "csv");
        DorisSink.Builder<RecordWithMeta> builder = DorisSink.builder();
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setLabelPrefix(UUID.randomUUID().toString())
                .enable2PC()
                .setBatchMode(batchMode)
                .setFlushQueueSize(4)
                .setBufferSize(1024)
                .setCheckInterval(1000)
                .setStreamLoadProp(properties);
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier("")
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword());

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new RecordWithMetaSerializer())
                .setDorisOptions(dorisBuilder.build());

        int triggerCkptError = 7;
        int totalRecords = 20;
        int addTblCheckpointId = 4;
        DataStreamSource<RecordWithMeta> mockSource =
                env.addSource(
                        new MockMultiTableSource(
                                totalRecords,
                                triggerCkptError,
                                DATABASE,
                                TABLE_MULTI_CSV,
                                totalTblNum,
                                addTblCheckpointId));

        mockSource.sinkTo(builder.build());
        JobClient jobClient = env.executeAsync();
        CompletableFuture<JobStatus> jobStatus = jobClient.getJobStatus();
        LOG.info("Job status: {}", jobStatus);

        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(120)));

        // wait checkpoint failure
        List<JobStatus> errorStatus =
                Arrays.asList(
                        JobStatus.FAILING,
                        JobStatus.CANCELLING,
                        JobStatus.CANCELED,
                        JobStatus.FAILED,
                        JobStatus.RESTARTING);

        waitForJobStatus(jobClient, errorStatus, Deadline.fromNow(Duration.ofSeconds(300)));

        Random random = new Random();
        LOG.info("start to create add table");
        // random sleep to create table
        Thread.sleep((checkpointIntervalMs / 1000 * random.nextInt(3)) * 1000);
        // create new add table
        initializeTable(TABLE_MULTI_CSV + newTableIndex);

        LOG.info("wait job restart success");
        // wait table restart success
        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(300)));

        LOG.info("wait job running finished");

        List<String> result = new ArrayList<>();
        boolean restart = false;
        boolean faultInjection = false;
        while (true) {
            try {
                // restart may be make query failed
                String query =
                        String.format(
                                "select * from %s",
                                DATABASE + "." + TABLE_MULTI_CSV + newTableIndex);
                result =
                        ContainerUtils.executeSQLStatement(
                                getDorisQueryConnection(), LOG, query, 2);
            } catch (Exception ex) {
                LOG.error("Failed to query result, cause " + ex.getMessage());
                continue;
            }

            if (getFlinkJobStatus(jobClient).equals(JobStatus.FINISHED)) {
                // Batch mode can only achieve at least once
                break;
            }

            int randomSleepSec = random.nextInt(10);
            if (result.size() > 2 && !faultInjection) {
                faultInjectionOpen();
                randomSleepSec = randomSleepSec + checkpointIntervalMs / 1000;
                LOG.info("Injecting fault, sleep {}s before recover", randomSleepSec);
                Thread.sleep(randomSleepSec * 1000);
                faultInjectionClear();
                LOG.info("Injecting fault recover");
                faultInjection = true;
            }

            if (result.size() > 6 && !restart) {
                LOG.info(
                        "Restarting doris cluster, sleep {}s before next restart",
                        randomSleepSec + 30);
                dorisContainerService.restartContainer();
                Thread.sleep((randomSleepSec + 30) * 1000);
                restart = true;
            }
            Thread.sleep(500);
        }

        // concat expect value
        List<String> expected = new ArrayList<>();
        for (int tb = 1; tb <= newTableIndex; tb++) {
            for (int i = 1; i <= totalRecords; i++) {
                if (tb == newTableIndex && i <= addTblCheckpointId) {
                    continue;
                }
                for (int j = 0; j < DEFAULT_PARALLELISM; j++) {
                    expected.add(TABLE_MULTI_CSV + tb + "," + i + "," + j);
                }
            }
        }

        String queryRes =
                String.format(
                        "select * from (select \"%s\" as tbl,id,task_id from %s.%s "
                                + "union all select \"%s\" as tbl,id,task_id from %s.%s "
                                + "union all select \"%s\" as tbl,id,task_id from %s.%s "
                                + "union all select \"%s\" as tbl,id,task_id from %s.%s ) a"
                                + " order by tbl,id,task_id",
                        TABLE_MULTI_CSV + 1,
                        DATABASE,
                        TABLE_MULTI_CSV + 1,
                        TABLE_MULTI_CSV + 2,
                        DATABASE,
                        TABLE_MULTI_CSV + 2,
                        TABLE_MULTI_CSV + 3,
                        DATABASE,
                        TABLE_MULTI_CSV + 3,
                        TABLE_MULTI_CSV + 4,
                        DATABASE,
                        TABLE_MULTI_CSV + 4);

        if (!batchMode) {
            ContainerUtils.checkResult(
                    getDorisQueryConnection(), LOG, expected, queryRes, 3, false);
        } else {
            List<String> actualResult =
                    ContainerUtils.getResult(getDorisQueryConnection(), LOG, expected, queryRes, 3);
            LOG.info("actual size: {}, expected size: {}", actualResult.size(), expected.size());
            Assert.assertTrue(
                    actualResult.size() >= expected.size() && actualResult.containsAll(expected));
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
                                + "`id` int,\n"
                                + "`task_id` int\n"
                                + ") DISTRIBUTED BY HASH(`id`) BUCKETS 1\n"
                                + "PROPERTIES (\n"
                                + "\"replication_num\" = \"1\"\n"
                                + ")\n",
                        DATABASE, table));
    }

    private void dropTable(String table) {
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(),
                LOG,
                String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table));
    }

    private void dropDatabase() {
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(),
                LOG,
                String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE));
    }
}
