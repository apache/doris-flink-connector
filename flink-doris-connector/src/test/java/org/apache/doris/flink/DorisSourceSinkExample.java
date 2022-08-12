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

package org.apache.doris.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DorisSourceSinkExample {

    public static void main(String[] args) {
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        TableEnvironment tEnv = TableEnvironment.create(settings);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE doris_test (" +
                        "id INT," +
                        "name STRING," +
                        "PRIMARY KEY (id) NOT ENFORCED" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'mysql-cdc',\n" +
                        "  'hostname' = '127.0.0.1',\n" +
                        "  'port' = '3306',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '123456'," +
                        "  'database-name' = 'test', " +
                        "  'table-name' = 'test'" +
                        ")");
        tEnv.executeSql(
                "CREATE TABLE doris_test_sink (" +
                        "id INT," +
                        "name STRING" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = '47.109.38.38:8030',\n" +
                        "  'table.identifier' = 'test.test',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '',\n" +
                        "  'sink.properties.format' = 'csv',\n" +
                        "  'sink.label-prefix' = 'doris_csv_table1222222'\n" +
                        ")");

        tEnv.executeSql("INSERT INTO doris_test_sink select id,name from doris_test");
    }
}
