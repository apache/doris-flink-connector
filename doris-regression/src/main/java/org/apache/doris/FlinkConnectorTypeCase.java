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

package org.apache.doris;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class FlinkConnectorTypeCase {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkConnectorTypeCase.class);
    private static String HOST = "";
    private static String TARGET_DORIS_DB = "";
    private static String USER = "";
    private static String PASSWORD = "";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        LOG.info("Input arguments: {}", String.join(" ", args));
        System.out.println(System.getProperty("java.version"));
        DorisArguments arguments = DorisArguments.parse(args);
        HOST = arguments.getFeAddress();
        TARGET_DORIS_DB = arguments.getDatabase();
        USER = arguments.getUser();
        PASSWORD = arguments.getPassword();
        String tlsSqlOptions = arguments.toFlinkSqlOptions();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE source_doris (" +
                        "`id` int,\n" +
                        "`c1` boolean,\n" +
                        "`c2` tinyint,\n" +
                        "`c3` smallint,\n" +
                        "`c4` int,\n" +
                        "`c5` bigint,\n" +
                        "`c6` string,\n" +
                        "`c7` float,\n" +
                        "`c8` double,\n" +
                        "`c9` decimal(12,4),\n" +
                        "`c10` date,\n" +
                        "`c11` TIMESTAMP,\n" +
                        "`c12` char(1),\n" +
                        "`c13` varchar(256),\n" +
                        "`c14` Array<String>,\n" +
                        "`c15` Map<String, String>,\n" +
                        "`c16` ROW<name String, age int>,\n" +
                        "`c17` STRING,\n" +
                        "`c18` STRING"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '" + HOST + "',\n"
                        + "  'table.identifier' = '" + TARGET_DORIS_DB + ".test_types_source',\n"
                        + "  'username' = '" + USER + "',\n"
                        + "  'password' = '" + PASSWORD + "'"
                        + tlsSqlOptions + "\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE doris_test_sink (" +
                        "`id` int,\n" +
                        "`c1` boolean,\n" +
                        "`c2` tinyint,\n" +
                        "`c3` smallint,\n" +
                        "`c4` int,\n" +
                        "`c5` bigint,\n" +
                        "`c6` string,\n" +
                        "`c7` float,\n" +
                        "`c8` double,\n" +
                        "`c9` decimal(12,4),\n" +
                        "`c10` date,\n" +
                        "`c11` TIMESTAMP,\n" +
                        "`c12` char(1),\n" +
                        "`c13` varchar(256),\n" +
                        "`c14` Array<String>,\n" +
                        "`c15` Map<String, String>,\n" +
                        "`c16` ROW<name String, age int>,\n" +
                        "`c17` STRING,\n" +
                        "`c18` STRING"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '" + HOST + "',\n"
                        + "  'table.identifier' = '" + TARGET_DORIS_DB + ".test_types_sink',\n"
                        + "  'username' = '" + USER + "',\n"
                        + "  'password' = '" + PASSWORD + "'"
                        + tlsSqlOptions + ",\n"
                        + "  'sink.properties.format' = 'json',\n"
                        + "  'sink.properties.read_json_by_line' = 'true',\n"
                        + "  'sink.label-prefix' = 'label" + UUID.randomUUID() + "'"
                        + ")");

        TableResult tableResult = tEnv.executeSql("INSERT INTO doris_test_sink select * from source_doris");
        tableResult.await();
        LOG.info("FlinkConnectorTypeCase execute success");
    }
}
