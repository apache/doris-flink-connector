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

package org.apache.doris.flink.lookup;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class LookupJoinCdcExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // env.disableOperatorChaining();
        env.enableCheckpointing(30000);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE mysql_tb ("
                        + "id INT,"
                        + "name STRING,"
                        + "process_time as proctime(),"
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
                        + "  'table-name' = 'fact_table'  "
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE doris_tb ("
                        + "id INT,"
                        + "age INT,"
                        + "dt DATE,"
                        + "dtime TIMESTAMP,"
                        + "primary key(id) NOT ENFORCED"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '127.0.0.1:8030',\n"
                        + "  'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',\n"
                        + "  'table.identifier' = 'test.dim_table_dt',\n"
                        + "  'lookup.cache.max-rows' = '1000',"
                        + "  'lookup.cache.ttl' = '1 hour',"
                        + "  'lookup.jdbc.async' = 'true',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = ''\n"
                        + ")");

        Table table =
                tEnv.sqlQuery(
                        "SELECT a.id, a.name, b.age, b.dt, b.dtime\n"
                                + "FROM mysql_tb a\n"
                                + "  left join doris_tb FOR SYSTEM_TIME AS OF a.process_time AS b\n"
                                + "  ON a.id = b.id");

        tEnv.toRetractStream(table, Row.class).print();

        env.execute();
    }
}
