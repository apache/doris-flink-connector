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

package org.apache.doris.flink.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.UUID;

public class DorisSinkArraySQLExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE source (\n"
                        + "  `id` int,\n"
                        + "  `c_1` array<INT> ,\n"
                        + "  `c_2` array<TINYINT> ,\n"
                        + "  `c_3` array<SMALLINT> ,\n"
                        + "  `c_4` array<INT> ,\n"
                        + "  `c_5` array<BIGINT> ,\n"
                        + "  `c_6` array<BIGINT> ,\n"
                        + "  `c_7` array<FLOAT>,\n"
                        + "  `c_8` array<DOUBLE> ,\n"
                        + "  `c_9` array<DECIMAL(4,2)> ,\n"
                        + "  `c_10` array<DATE>  ,\n"
                        + "  `c_11` array<DATE>  ,\n"
                        + "  `c_12` array<TIMESTAMP>  ,\n"
                        + "  `c_13` array<TIMESTAMP>  ,\n"
                        + "  `c_14` array<CHAR(10)>  ,\n"
                        + "  `c_15` array<VARCHAR(256)>  ,\n"
                        + "  `c_16` array<STRING>  \n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen', \n"
                        + "  'fields.c_7.element.min' = '1', \n"
                        + "  'fields.c_7.element.max' = '10', \n"
                        + "  'fields.c_8.element.min' = '1', \n"
                        + "  'fields.c_8.element.max' = '10', \n"
                        + "  'fields.c_14.element.length' = '10', \n"
                        + "  'fields.c_15.element.length' = '10', \n"
                        + "  'fields.c_16.element.length' = '10', \n"
                        + "  'number-of-rows' = '5'  \n"
                        + ");");

        tEnv.executeSql(
                "CREATE TABLE source_doris ("
                        + "  `id` int,\n"
                        + "  `c_1` array<INT> ,\n"
                        + "  `c_2` array<TINYINT> ,\n"
                        + "  `c_3` array<SMALLINT> ,\n"
                        + "  `c_4` array<INT> ,\n"
                        + "  `c_5` array<BIGINT> ,\n"
                        + "  `c_6` array<STRING> ,\n"
                        + "  `c_7` array<FLOAT> ,\n"
                        + "  `c_8` array<DOUBLE> ,\n"
                        + "  `c_9` array<DECIMAL(4,2)> ,\n"
                        + "  `c_10` array<STRING>  ,\n"
                        + // ARRAY<DATE>
                        "  `c_11` array<STRING>  ,\n"
                        + // ARRAY<DATE>
                        "  `c_12` array<STRING>  ,\n"
                        + // ARRAY<TIMESTAMP>
                        "  `c_13` array<STRING>  ,\n"
                        + // ARRAY<TIMESTAMP>
                        "  `c_14` array<CHAR(10)>  ,\n"
                        + "  `c_15` array<VARCHAR(256)>  ,\n"
                        + "  `c_16` array<STRING>  \n"
                        + ") WITH ("
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '127.0.0.1:8030',\n"
                        + "  'table.identifier' = 'test.array_test_type',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = ''\n"
                        + ")");

        // define a dynamic aggregating query
        // final Table result = tEnv.sqlQuery("SELECT * from source_doris  ");

        // print the result to the console
        // tEnv.toRetractStream(result, Row.class).print();
        // env.execute();

        tEnv.executeSql(
                "CREATE TABLE sink ("
                        + "  `id` int,\n"
                        + "  `c_1` array<INT> ,\n"
                        + "  `c_2` array<TINYINT> ,\n"
                        + "  `c_3` array<SMALLINT> ,\n"
                        + "  `c_4` array<INT> ,\n"
                        + "  `c_5` array<BIGINT> ,\n"
                        + "  `c_6` array<STRING> ,\n"
                        + "  `c_7` array<FLOAT> ,\n"
                        + "  `c_8` array<DOUBLE> ,\n"
                        + "  `c_9` array<DECIMAL(4,2)> ,\n"
                        + "  `c_10` array<STRING>  ,\n"
                        + // ARRAY<DATE>
                        "  `c_11` array<STRING>  ,\n"
                        + // ARRAY<DATE>
                        "  `c_12` array<STRING>  ,\n"
                        + // ARRAY<TIMESTAMP>
                        "  `c_13` array<STRING>  ,\n"
                        + // ARRAY<TIMESTAMP>
                        "  `c_14` array<CHAR(10)>  ,\n"
                        + "  `c_15` array<VARCHAR(256)>  ,\n"
                        + "  `c_16` array<STRING>  \n"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '127.0.0.1:8030',\n"
                        + "  'table.identifier' = 'test.array_test_type_sink',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = '',\n"
                        + "  'sink.label-prefix' = 'doris_label4"
                        + UUID.randomUUID()
                        + "'"
                        + ")");
        tEnv.executeSql("INSERT INTO sink select * from source_doris");
    }
}
