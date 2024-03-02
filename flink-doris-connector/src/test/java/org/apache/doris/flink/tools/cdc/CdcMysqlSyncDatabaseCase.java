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

package org.apache.doris.flink.tools.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CdcMysqlSyncDatabaseCase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //        env.setParallelism(1);

        Map<String, String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval", "10s");
        flinkMap.put("pipeline.operator-chaining", "false");
        flinkMap.put("parallelism.default", "1");

        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);

        String database = "db1";
        String tablePrefix = "";
        String tableSuffix = "";
        Map<String, String> mysqlConfig = new HashMap<>();
        mysqlConfig.put("database-name", "db1");
        mysqlConfig.put("hostname", "127.0.0.1");
        mysqlConfig.put("port", "3306");
        mysqlConfig.put("username", "root");
        // mysqlConfig.put("password","");
        mysqlConfig.put("password", "12345678");
        Configuration config = Configuration.fromMap(mysqlConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put("fenodes", "10.20.30.1:8030");
        // sinkConfig.put("benodes","10.20.30.1:8040, 10.20.30.2:8040, 10.20.30.3:8040");
        sinkConfig.put("username", "root");
        sinkConfig.put("password", "");
        sinkConfig.put("jdbc-url", "jdbc:mysql://10.20.30.1:9030");
        sinkConfig.put("sink.label-prefix", UUID.randomUUID().toString());
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("replication_num", "1");
        tableConfig.put("table-buckets", "tbl1:10,tbl2:20,a.*:30,b.*:40,.*:50");
        // String includingTables = "tbl1|tbl2|tbl3";
        String includingTables = "a_.*|b_.*|c";
        String excludingTables = "";
        String multiToOneOrigin = "a_.*|b_.*";
        String multiToOneTarget = "a|b";
        boolean ignoreDefaultValue = false;
        boolean useNewSchemaChange = false;
        boolean singleSink = false;
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        databaseSync
                .setEnv(env)
                .setDatabase(database)
                .setConfig(config)
                .setTablePrefix(tablePrefix)
                .setTableSuffix(tableSuffix)
                .setIncludingTables(includingTables)
                .setExcludingTables(excludingTables)
                .setMultiToOneOrigin(multiToOneOrigin)
                .setMultiToOneTarget(multiToOneTarget)
                .setIgnoreDefaultValue(ignoreDefaultValue)
                .setSinkConfig(sinkConf)
                .setTableConfig(tableConfig)
                .setCreateTableOnly(false)
                .setNewSchemaChange(useNewSchemaChange)
                .setSingleSink(singleSink)
                .create();
        databaseSync.build();
        env.execute(String.format("MySQL-Doris Database Sync: %s", database));
    }
}
