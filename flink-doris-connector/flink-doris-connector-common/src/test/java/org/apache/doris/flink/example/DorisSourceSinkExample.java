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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.UUID;

public class DorisSourceSinkExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE doris_test ("
                        + "  id int,\n"
                        + "  c_1 boolean,\n"
                        + "  c_2 tinyint,\n"
                        + "  c_3 smallint,\n"
                        + "  c_4 int,\n"
                        + "  c_5 bigint,\n"
                        + "  c_6 bigint,\n"
                        + "  c_7 float,\n"
                        + "  c_8 double,\n"
                        + "  c_9 DECIMAL(4,2),\n"
                        + "  c_10 DECIMAL(4,1),\n"
                        + "  c_11 date,\n"
                        + "  c_12 date,\n"
                        + "  c_13 timestamp,\n"
                        + "  c_14 timestamp,\n"
                        + "  c_15 string,\n"
                        + "  c_16 string,\n"
                        + "  c_17 string,\n"
                        + "  c_18 array<int>,\n"
                        + "  c_19 Map<String,int>\n"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'datagen', \n"
                        + "  'fields.c_6.max' = '5', \n"
                        + "  'fields.c_9.max' = '5', \n"
                        + "  'fields.c_10.max' = '5', \n"
                        + "  'fields.c_15.length' = '5', \n"
                        + "  'fields.c_16.length' = '5', \n"
                        + "  'fields.c_17.length' = '5', \n"
                        + "  'fields.c_19.key.length' = '5', \n"
                        + "  'connector' = 'datagen', \n"
                        + "  'number-of-rows' = '1'  \n"
                        + ")");

        final Table result = tEnv.sqlQuery("SELECT * from doris_test  ");

        // print the result to the console
        tEnv.toRetractStream(result, Row.class).print();
        env.execute();

        tEnv.executeSql(
                "CREATE TABLE source_doris ("
                        + "  id int,\n"
                        + "  c_1 boolean,\n"
                        + "  c_2 tinyint,\n"
                        + "  c_3 smallint,\n"
                        + "  c_4 int,\n"
                        + "  c_5 bigint,\n"
                        + "  c_6 string,\n"
                        + "  c_7 float,\n"
                        + "  c_8 double,\n"
                        + "  c_9 DECIMAL(4,2),\n"
                        + "  c_10 DECIMAL(4,1),\n"
                        + "  c_11 date,\n"
                        + "  c_12 date,\n"
                        + "  c_13 timestamp,\n"
                        + "  c_14 timestamp,\n"
                        + "  c_15 string,\n"
                        + "  c_16 string,\n"
                        + "  c_17 string,\n"
                        + "  c_18 array<int>,\n"
                        + "  c_19 string\n"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '127.0.0.1:8030',\n"
                        + "  'table.identifier' = 'test.test_all_type',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = ''\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE doris_test_sink ("
                        + "  id int,\n"
                        + "  c_1 boolean,\n"
                        + "  c_2 tinyint,\n"
                        + "  c_3 smallint,\n"
                        + "  c_4 int,\n"
                        + "  c_5 bigint,\n"
                        + "  c_6 string,\n"
                        + "  c_7 float,\n"
                        + "  c_8 double,\n"
                        + "  c_9 DECIMAL(4,2),\n"
                        + "  c_10 DECIMAL(4,1),\n"
                        + "  c_11 date,\n"
                        + "  c_12 date,\n"
                        + "  c_13 timestamp,\n"
                        + "  c_14 timestamp,\n"
                        + "  c_15 string,\n"
                        + "  c_16 string,\n"
                        + "  c_17 string,\n"
                        + "  c_18 array<int>,\n"
                        + "  c_19 string\n"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '127.0.0.1:8030',\n"
                        + "  'table.identifier' = 'test.test_all_type_sink',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = '',\n"
                        + "  'sink.properties.format' = 'csv',\n"
                        + "  'sink.label-prefix' = 'doris_label4"
                        + UUID.randomUUID()
                        + "'"
                        + ")");

        tEnv.executeSql("INSERT INTO doris_test_sink select * from source_doris");
    }
}
