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

public class CatalogExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE CATALOG doris_catalog WITH(\n"
                        + "'type' = 'doris',\n"
                        + "'default-database' = 'test',\n"
                        + "'username' = 'root',\n"
                        + "'password' = '',\n"
                        + "'fenodes' = '1127.0.0.1:8030',\n"
                        + "'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',\n"
                        + "'sink.label-prefix' = 'label'\n"
                        + ")");
        // define a dynamic aggregating query
        final Table result = tEnv.sqlQuery("SELECT * from doris_catalog.test.type_test");

        // print the result to the console
        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }
}
