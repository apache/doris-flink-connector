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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.doris.flink.container.AbstractContainerTestBase;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@RunWith(Parameterized.class)
public class Doris2DorisE2ECase extends AbstractContainerTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(Doris2DorisE2ECase.class);
    private static final String DATABASE_SOURCE = "test_doris2doris_source";
    private static final String DATABASE_SINK = "test_doris2doris_sink";
    private static final String TABLE = "test_tbl";

    private final boolean useFlightRead;

    public Doris2DorisE2ECase(boolean useFlightRead) {
        this.useFlightRead = useFlightRead;
    }

    @Parameterized.Parameters(name = "useFlightRead: {0}")
    public static Object[] parameters() {
        return new Object[][] {new Object[] {false}, new Object[] {true}};
    }

    @Test
    public void testDoris2Doris() throws Exception {
        LOG.info("Start executing the test case of doris to doris.");
        initializeDorisTable();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
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
                                + "c14  STRING, \n"
                                + "c15  Array<String>, \n"
                                + "c16  Map<String, String>, \n"
                                + "c17  ROW<name String, age int>, \n"
                                + "c18  STRING, \n"
                                + "c19  STRING\n"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
                                + " 'fenodes' = '%s',"
                                + " 'table.identifier' = '%s',"
                                + " 'sink.label-prefix' = '"
                                + UUID.randomUUID()
                                + "',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'source.use-flight-sql' = '%s',\n"
                                + " 'source.flight-sql-port' = '9611'"
                                + ")",
                        getFenodes(),
                        DATABASE_SOURCE + "." + TABLE,
                        getDorisUsername(),
                        getDorisPassword(),
                        useFlightRead);
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
                                + "c14  STRING, \n"
                                + "c15  Array<String>, \n"
                                + "c16  Map<String, String>, \n"
                                + "c17  ROW<name String, age int>, \n"
                                + "c18  STRING, \n"
                                + "c19  STRING\n"
                                + ") WITH ("
                                + " 'connector' = '"
                                + DorisConfigOptions.IDENTIFIER
                                + "',"
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

        List<String> excepted =
                Arrays.asList(
                        "1,true,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,3.14,2.71828,12345.6789,2025-03-11,2025-03-11T12:34:56,A,Hello, Doris!,This is a string,[\"Alice\", \"Bob\"],{\"key1\":\"value1\", \"key2\":\"value2\"},{\"name\": \"Tom\", \"age\": 30},{\"key\":\"value\"},{\"data\":123,\"type\":\"variant\"}",
                        "2,false,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-1.23,1.0E-4,-9999.9999,2024-12-25,2024-12-25T23:59:59,B,Doris Test,Another string!,[\"Charlie\", \"David\"],{\"k1\":\"v1\", \"k2\":\"v2\"},{\"name\": \"Jerry\", \"age\": 25},{\"status\":\"ok\"},{\"data\":[1,2,3]}",
                        "3,true,0,0,0,0,0,0.0,0.0,0.0000,2023-06-15,2023-06-15T08:00,C,Test Doris,Sample text,[\"Eve\", \"Frank\"],{\"alpha\":\"beta\"},{\"name\": \"Alice\", \"age\": 40},{\"nested\":{\"key\":\"value\"}},{\"variant\":\"test\"}",
                        "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null");

        String query = String.format("SELECT * FROM %s.%s", DATABASE_SINK, TABLE);
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, excepted, query, 20, false);
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
}
