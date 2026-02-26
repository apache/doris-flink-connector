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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.UUID;

public class DorisDateAndTimestampSqlTest {

    public static void main(String[] args) {
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "create table test_source ( "
                        + "        id INT, "
                        + "        score DECIMAL(10, 9), "
                        + "        submit_time TIMESTAMP "
                        + "        ) with ( "
                        + "        'password'='', "
                        + "        'connector'='doris', "
                        + "        'fenodes'='FE_HOST:FE_PORT', "
                        + "        'table.identifier'='db.source_table', "
                        + "        'username'='root' "
                        + ")");

        tEnv.executeSql(
                "create table test_sink ( "
                        + "        id INT, "
                        + "        score DECIMAL(10, 9), "
                        + "        submit_time DATE "
                        + "        ) with ( "
                        + "        'password'='', "
                        + "        'connector'='doris', "
                        + "        'fenodes'='FE_HOST:FE_PORT', "
                        + "        'sink.label-prefix' = 'label_"
                        + UUID.randomUUID()
                        + "' , "
                        + "        'table.identifier'='db.sink_table', "
                        + "        'username'='root' "
                        + ")");
        tEnv.executeSql(
                        "insert into "
                                + "    test_sink "
                                + "select "
                                + "    id, "
                                + "    score,"
                                + "    to_date(DATE_FORMAT(submit_time, 'yyyy-MM-dd')) as submit_time "
                                + "from "
                                + "    test_source "
                                + "where "
                                + "    submit_time>='2022-05-31 00:00:00'")
                .print();
    }
}
