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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.doris.flink.utils.MockSource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** DorisSink ITCase failover case */
@RunWith(Parameterized.class)
public class DorisSinkFailoverITCase extends AbstractITCaseService {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkFailoverITCase.class);
    static final String DATABASE = "test_failover_sink";
    static final String TABLE_JSON_TBL_RESTART_DORIS = "tbl_json_tbl_restart_doris";
    static final String TABLE_JSON_TBL_LOAD_FAILURE = "tbl_json_tbl_load_failure";
    static final String TABLE_JSON_TBL_CKPT_FAILURE = "tbl_json_tbl_ckpt_failure";

    private final boolean batchMode;

    public DorisSinkFailoverITCase(boolean batchMode) {
        this.batchMode = batchMode;
    }

    @Parameterized.Parameters(name = "batchMode: {0}")
    public static Object[] parameters() {
        return new Object[][] {new Object[] {false}, new Object[] {true}};
    }

    /** test doris cluster failover */
    @Test
    public void testDorisClusterFailoverSink() throws Exception {
        LOG.info("start to test testDorisClusterFailoverSink.");
        makeFailoverTest(TABLE_JSON_TBL_RESTART_DORIS, FaultType.RESTART_FAILURE, 40);
    }

    /** mock precommit failure */
    @Test
    public void testStreamLoadFailoverSink() throws Exception {
        LOG.info("start to test testStreamLoadFailoverSink.");
        makeFailoverTest(TABLE_JSON_TBL_LOAD_FAILURE, FaultType.STREAM_LOAD_FAILURE, 20);
    }

    /** mock checkpoint failure when precommit or streamload successful */
    @Test
    public void testCheckpointFailoverSink() throws Exception {
        LOG.info("start to test testCheckpointFailoverSink.");
        makeFailoverTest(TABLE_JSON_TBL_CKPT_FAILURE, FaultType.CHECKPOINT_FAILURE, 20);
    }

    public void makeFailoverTest(String tableName, FaultType faultType, int totalRecords)
            throws Exception {
        initializeTable(tableName);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        int checkpointInterval = 5000;
        env.enableCheckpointing(checkpointInterval);

        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("format", "csv");
        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setLabelPrefix(UUID.randomUUID().toString())
                .enable2PC()
                .setCheckInterval(1000)
                .setBatchMode(batchMode)
                .setFlushQueueSize(4)
                .setStreamLoadProp(properties);
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes(getFenodes())
                .setTableIdentifier(DATABASE + "." + tableName)
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword());

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        int triggerCkptError = -1;
        if (FaultType.CHECKPOINT_FAILURE.equals(faultType)) {
            triggerCkptError = 7;
        }
        DataStreamSource<String> mockSource =
                env.addSource(new MockSource(totalRecords, triggerCkptError));
        mockSource.sinkTo(builder.build());
        JobClient jobClient = env.executeAsync();
        CompletableFuture<JobStatus> jobStatus = jobClient.getJobStatus();
        LOG.info("Job status: {}", jobStatus);

        String query = String.format("select * from %s", DATABASE + "." + tableName);
        List<String> result;
        int maxRestart = 5;
        Random random = new Random();
        while (true) {
            try {
                // restart may be make query failed
                result =
                        ContainerUtils.executeSQLStatement(
                                getDorisQueryConnection(), LOG, query, 2);
            } catch (Exception ex) {
                LOG.error("Failed to query result, cause " + ex.getMessage());
                continue;
            }

            if (result.size() >= totalRecords * DEFAULT_PARALLELISM
                    && getFlinkJobStatus(jobClient).equals(JobStatus.FINISHED)) {
                // Batch mode can only achieve at least once
                break;
            }

            // Wait until write is successful, then trigger error
            if (result.size() > 1 && maxRestart-- >= 0) {
                // trigger error random
                int randomSleepSec = random.nextInt(30);
                if (FaultType.STREAM_LOAD_FAILURE.equals(faultType)) {
                    faultInjectionOpen();
                    randomSleepSec = randomSleepSec + 20;
                    LOG.info("Injecting fault, sleep {}s before recover", randomSleepSec);
                    Thread.sleep(randomSleepSec * 1000);
                    faultInjectionClear();
                } else if (FaultType.RESTART_FAILURE.equals(faultType)) {
                    // docker image restart time is about 60s
                    randomSleepSec = randomSleepSec + 60;
                    dorisContainerService.restartContainer();
                    LOG.info(
                            "Restarting doris cluster, sleep {}s before next restart",
                            randomSleepSec);
                    Thread.sleep(randomSleepSec * 1000);
                }
            } else {
                // Avoid frequent queries
                Thread.sleep(checkpointInterval);
            }
        }

        // concat expect value
        List<String> expected = new ArrayList<>();
        for (int i = 1; i <= totalRecords; i++) {
            for (int j = 0; j < DEFAULT_PARALLELISM; j++) {
                expected.add(i + "," + j);
            }
        }
        if (!batchMode) {
            ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, query, 2, false);
        } else {
            List<String> actualResult =
                    ContainerUtils.getResult(getDorisQueryConnection(), LOG, expected, query, 2);
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
}
