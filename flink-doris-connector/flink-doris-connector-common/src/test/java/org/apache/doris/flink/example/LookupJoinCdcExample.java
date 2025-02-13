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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class LookupJoinCdcExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // env.disableOperatorChaining();
        env.enableCheckpointing(10000);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE mysql_tb ("
                        + "id INT,"
                        + "name STRING,"
                        + "create_time  TIMESTAMP(3),"
                        + "WATERMARK FOR create_time AS create_time - INTERVAL '60' SECOND,"
                        + "primary key(id) NOT ENFORCED"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'mysql-cdc',\n"
                        + "  'hostname' = '127.0.0.1',\n"
                        + "  'port' = '3306',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = '123456',\n"
                        + "  'database-name' = 'test',\n"
                        + "  'scan.startup.mode' = 'latest-offset',\n"
                        + "  'server-time-zone' = 'Asia/Shanghai',\n"
                        + "  'table-name' = 'student'  "
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE mysql_tb1 ("
                        + "id INT,"
                        + "name STRING,"
                        + "age STRING,"
                        + "create_time TIMESTAMP(3),"
                        + "WATERMARK FOR create_time AS create_time - INTERVAL '60' SECOND,"
                        + "primary key(id) NOT ENFORCED"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'mysql-cdc',\n"
                        + "  'hostname' = '127.0.0.1',\n"
                        + "  'port' = '3306',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = '123456',\n"
                        + "  'database-name' = 'test',\n"
                        + "  'scan.startup.mode' = 'latest-offset',\n"
                        + "  'server-time-zone' = 'Asia/Shanghai',\n"
                        + "  'table-name' = 'student_copy1'  "
                        + ")");

        //        tEnv.executeSql(
        //                "CREATE TABLE doris_tb ("
        //                        + "name STRING,"
        //                        + "age INT,"
        //                        + "primary key(name) NOT ENFORCED"
        //                        + ") "
        //                        + "WITH (\n"
        //                        + "  'connector' = 'doris',\n"
        //                        + "  'fenodes' = '10.16.10.6:28737',\n"
        //                        + "  'jdbc-url' = 'jdbc:mysql://10.16.10.6:29737',\n"
        //                        + "  'table.identifier' = 'test.student',\n"
        ////                        + "  'lookup.cache.max-rows' = '1000',"
        ////                        + "  'lookup.cache.ttl' = '1 hour',"
        //                        //                        + "  'lookup.jdbc.async' = 'true',\n"
        //                        + "  'username' = 'root',\n"
        //                        + "  'password' = ''\n"
        //                        + ")");

        //                tEnv.executeSql(
        //                "CREATE TABLE doris_tb ("
        //                        + "name STRING,"
        //                        + "age INT,"
        //                        + "primary key(name) NOT ENFORCED"
        //                        + ") "
        //                        + "WITH (\n"
        //                        + "  'connector' = 'jdbc',\n"
        //                        + "  'url' = 'jdbc:mysql://10.16.10.6:29737/test',"
        //                        + "  'table-name' = 'test_flink_a',\n"
        //                        + "  'username' = 'root',\n"
        //                        + "  'password' = ''\n"
        //                        + ")");

        Table table =
                tEnv.sqlQuery(
                        "SELECT a.id, a.name, b.age\n"
                                + "FROM mysql_tb a\n"
                                + "  left join mysql_tb1 FOR SYSTEM_TIME AS OF a.create_time AS b\n"
                                + "  ON a.name = b.name");

        tEnv.toRetractStream(table, Row.class).print();

        env.execute();
    }
}
