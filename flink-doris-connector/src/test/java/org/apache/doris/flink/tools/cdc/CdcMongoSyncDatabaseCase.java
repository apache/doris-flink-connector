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

import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.table.DorisConfigOptions;
import org.apache.doris.flink.tools.cdc.mongodb.MongoDBDatabaseSync;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CdcMongoSyncDatabaseCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval", "10s");
        flinkMap.put("pipeline.operator-chaining", "false");
        flinkMap.put("parallelism.default", "8");

        String database = "cdc_test";
        String tablePrefix = "";
        String tableSuffix = "";

        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);
        Map<String, String> mongoConfig = new HashMap<>();
        mongoConfig.put(MongoDBSourceOptions.DATABASE.key(), "test");
        mongoConfig.put(MongoDBSourceOptions.HOSTS.key(), "127.0.0.1:27017");
        mongoConfig.put(MongoDBSourceOptions.USERNAME.key(), "flinkuser");
        mongoConfig.put(MongoDBSourceOptions.PASSWORD.key(), "flinkpwd");
        // mongoConfig.put(SourceOptions.SCAN_STARTUP_MODE.key(),
        // DorisCDCConfig.SCAN_STARTUP_MODE_VALUE_LATEST_OFFSET);
        mongoConfig.put(
                SourceOptions.SCAN_STARTUP_MODE.key(),
                DatabaseSyncConfig.SCAN_STARTUP_MODE_VALUE_INITIAL);
        mongoConfig.put("schema.sample-percent", "1");
        Configuration config = Configuration.fromMap(mongoConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put(DorisConfigOptions.FENODES.key(), "127.0.0.1:8030");
        sinkConfig.put(DorisConfigOptions.USERNAME.key(), "root");
        sinkConfig.put(DorisConfigOptions.PASSWORD.key(), "");
        sinkConfig.put(DorisConfigOptions.JDBC_URL.key(), "jdbc:mysql://127.0.0.1:9030");
        sinkConfig.put(DorisConfigOptions.SINK_LABEL_PREFIX.key(), UUID.randomUUID().toString());
        sinkConfig.put(DorisConfigOptions.AUTO_REDIRECT.key(), "false");
        // sinkConfig.put(DorisConfigOptions.SINK_ENABLE_BATCH_MODE.key(),"true");
        // sinkConfig.put(DorisConfigOptions.SINK_WRITE_MODE.key(),"stream_load_batch");
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put(DatabaseSyncConfig.REPLICATION_NUM, "1");
        tableConfig.put(DatabaseSyncConfig.TABLE_BUCKETS, ".*:1");
        String includingTables = "cdc_test";
        String excludingTables = "";
        String multiToOneOrigin = "a_.*|b_.*";
        String multiToOneTarget = "a|b";
        boolean ignoreDefaultValue = false;
        DatabaseSync databaseSync = new MongoDBDatabaseSync();
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
                .create();
        databaseSync.build();
        env.execute(String.format("Mongo-Doris Database Sync: %s", database));
    }
}
