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

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.table.DorisConfigOptions;
import org.apache.doris.flink.tools.cdc.db2.Db2DatabaseSync;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CdcDb2SyncDatabaseCase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(10000);

        String database = "db2_test";
        String tablePrefix = "";
        String tableSuffix = "";
        Map<String, String> sourceConfig = new HashMap<>();
        sourceConfig.put(JdbcSourceOptions.DATABASE_NAME.key(), "testdb");
        sourceConfig.put(JdbcSourceOptions.SCHEMA_NAME.key(), "DB2INST1");
        sourceConfig.put(JdbcSourceOptions.HOSTNAME.key(), "127.0.0.1");
        sourceConfig.put(Db2DatabaseSync.PORT.key(), "50000");
        sourceConfig.put(JdbcSourceOptions.USERNAME.key(), "db2inst1");
        sourceConfig.put(JdbcSourceOptions.PASSWORD.key(), "=doris123456");
        sourceConfig.put(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.key(), "true");
        // add jdbc properties configuration
        sourceConfig.put("jdbc.properties.allowNextOnExhaustedResultSet", "1");
        sourceConfig.put("jdbc.properties.resultSetHoldability", "1");
        sourceConfig.put("jdbc.properties.SSL", "false");

        Configuration config = Configuration.fromMap(sourceConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put(DorisConfigOptions.FENODES.key(), "127.0.0.1:8030");
        sinkConfig.put(DorisConfigOptions.USERNAME.key(), "root");
        sinkConfig.put(DorisConfigOptions.PASSWORD.key(), "123456");
        sinkConfig.put(DorisConfigOptions.JDBC_URL.key(), "jdbc:mysql://127.0.0.1:9030");
        sinkConfig.put(DorisConfigOptions.SINK_LABEL_PREFIX.key(), UUID.randomUUID().toString());
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put(DatabaseSyncConfig.REPLICATION_NUM, "1");
        tableConfig.put(DatabaseSyncConfig.TABLE_BUCKETS, "tbl1:10,tbl2:20,a.*:30,b.*:40,.*:50");
        String includingTables = "FULL_TYPES";
        String excludingTables = null;
        String multiToOneOrigin = null;
        String multiToOneTarget = null;
        boolean ignoreDefaultValue = false;
        boolean useNewSchemaChange = true;
        boolean singleSink = false;
        boolean ignoreIncompatible = false;
        DatabaseSync databaseSync = new Db2DatabaseSync();
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
                .setIgnoreIncompatible(ignoreIncompatible)
                .create();
        databaseSync.build();
        env.execute(String.format("DB2-Doris Database Sync: %s", database));
    }
}
