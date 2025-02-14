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

public class DorisSourceExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register a table in the catalog
        tEnv.executeSql(
                "CREATE TABLE doris_source ("
                        + "name STRING,"
                        + "age int"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '10.16.10.6:28737',\n"
                        + "  'table.identifier' = 'test.test_flink_bucket',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = '',\n"
                        //                        + "  'doris.filter.query' = ' (dt =
                        // DATE_FORMAT(TIMESTAMPADD(DAY , -7, NOW()), ''yyyy-MM-dd'')) ',\n"
                        + "  'source.use-flight-sql' = 'true',\n"
                        + "  'doris.request.tablet.size' = '1',\n"
                        + "  'source.flight-sql-port' = '29747'"
                        + ")");

        // define a dynamic aggregating query
        final Table result = tEnv.sqlQuery("  SELECT * from doris_source limit 1 ");
        //        TableResult tableResult =  tEnv.executeSql("explain PLAN FOR  SELECT * from
        // doris_source limit 10  ");
        //        tableResult.print();
        //        final Table result = tEnv.sqlQuery("SELECT * from ( select name,age  as c,dt from
        // doris_source) tb where c >2");

        // print the result to the console
        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }
}
