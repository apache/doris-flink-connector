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

package org.apache.doris.flink.container.e2e;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.doris.flink.container.AbstractE2EService;
import org.apache.doris.flink.container.ContainerUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Doris2DorisE2ECase extends AbstractE2EService {
    private static final Logger LOG = LoggerFactory.getLogger(Doris2DorisE2ECase.class);
    private static final String DATABASE_SOURCE = "test_doris2doris_source";
    private static final String DATABASE_SINK = "test_doris2doris_sink";
    private static final String TABLE = "test_tbl";

    @Before
    public void setUp() throws InterruptedException {
        LOG.info("Doris2DorisE2ECase attempting to acquire semaphore.");
        SEMAPHORE.acquire();
        LOG.info("Doris2DorisE2ECase semaphore acquired.");
    }

    @Test
    public void testDoris2Doris() throws Exception {
        LOG.info("Start executing the test case of doris to doris.");
        initializeDorisTable();
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
                        getDorisUsername(),
                        getDorisPassword());
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
                        getDorisUsername(),
                        getDorisPassword());
        tEnv.executeSql(sinkDDL);

        tEnv.executeSql("INSERT INTO doris_sink SELECT * FROM doris_source").await();

        TableResult tableResult = tEnv.executeSql("SELECT * FROM doris_sink");
        List<Object> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        LOG.info("The actual data in the doris sink table is, actual={}", actual);

        String[] expected =
                new String[] {
                    "+I[1, true, 127, 32767, 2147483647, 9223372036854775807, 123456789012345678901234567890, 3.14, 2.7182818284, 12345.6789, 2023-05-22, 2023-05-22T12:34:56, A, Example text, [item1, item2, item3], {key1=value1, key2=value2}, +I[John Doe, 30], {\"key\":\"value\"}]",
                    "+I[2, false, -128, -32768, -2147483648, -9223372036854775808, -123456789012345678901234567890, -3.14, -2.7182818284, -12345.6789, 2024-01-01, 2024-01-01T00:00, B, Another example, [item4, item5, item6], {key3=value3, key4=value4}, +I[Jane Doe, 25], {\"another_key\":\"another_value\"}]"
                };
        Assert.assertArrayEquals(expected, actual.toArray(new String[0]));
    }

    private void initializeDorisTable() {
        String[] sourceInitSql =
                ContainerUtils.parseFileContentSQL(
                        "container/e2e/doris2doris/test_doris2doris_source_test_tbl.sql");
        ContainerUtils.executeSQLStatement(getDorisQueryConnection(), LOG, sourceInitSql);
        String[] sinkInitSql =
                ContainerUtils.parseFileContentSQL(
                        "container/e2e/doris2doris/test_doris2doris_sink_test_tbl.sql");
        ContainerUtils.executeSQLStatement(getDorisQueryConnection(), LOG, sinkInitSql);
        LOG.info("Initialization of doris table successful.");
    }

    @After
    public void close() {
        try {
            // Ensure that semaphore is always released
        } finally {
            LOG.info("Doris2DorisE2ECase releasing semaphore.");
            SEMAPHORE.release();
        }
    }
}
