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

public class DorisSinkSQLExample {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE doris_test_source ("
                        + "name STRING,"
                        + "age INT"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '10.16.10.6:28737',\n"
                        + "  'table.identifier' = 'test.test_flink_10bucket',\n"
                        + "  'jdbc-url' = 'jdbc:mysql://10.16.10.6:29737',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = '',\n"
                        + "  'sink.properties.format' = 'json',\n"
                        + "  'sink.buffer-count' = '4',\n"
                        + "  'sink.buffer-size' = '4086',"
                        + "  'sink.label-prefix' = 'doris_lab1el112121',\n"
                        + "  'sink.properties.read_json_by_line' = 'true'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE doris_test_sink ("
                        + "name STRING,"
                        + "age INT"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '10.16.10.6:28737',\n"
                        + "  'table.identifier' = 'test.test_flink_a',\n"
                        + "  'jdbc-url' = 'jdbc:mysql://10.16.10.6:29737',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = '',\n"
                        + "  'sink.properties.format' = 'json',\n"
                        + "  'sink.buffer-count' = '4',\n"
                        + "  'sink.buffer-size' = '4086',"
                        + "  'sink.label-prefix' = 'doris_lab1el4',\n"
                        + "  'sink.properties.read_json_by_line' = 'true'\n"
                        + ")");

        tEnv.executeSql("INSERT OVERWRITE doris_test_sink select 'zhangsan',1");
    }
}
