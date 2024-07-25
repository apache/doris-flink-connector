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

import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.table.DorisConfigOptions;
import org.apache.doris.flink.tools.cdc.oracle.OracleDatabaseSync;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CdcOraclelSyncDatabaseCase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(10000);

        String database = "db1";
        String tablePrefix = "";
        String tableSuffix = "";
        Map<String, String> sourceConfig = new HashMap<>();
        sourceConfig.put(OracleSourceOptions.DATABASE_NAME.key(), "XE");
        sourceConfig.put(OracleSourceOptions.SCHEMA_NAME.key(), "ADMIN");
        sourceConfig.put(OracleSourceOptions.HOSTNAME.key(), "127.0.0.1");
        sourceConfig.put(OracleSourceOptions.PORT.key(), "1521");
        sourceConfig.put(OracleSourceOptions.USERNAME.key(), "admin");
        sourceConfig.put(OracleSourceOptions.PASSWORD.key(), "");
        // sourceConfig.put("debezium.database.tablename.case.insensitive","false");
        sourceConfig.put("debezium.log.mining.strategy", "online_catalog");
        sourceConfig.put("debezium.log.mining.continuous.mine", "true");
        sourceConfig.put("debezium.database.history.store.only.captured.tables.ddl", "true");
        Configuration config = Configuration.fromMap(sourceConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put(DorisConfigOptions.FENODES.key(), "10.20.30.1:8030");
        sinkConfig.put(DorisConfigOptions.USERNAME.key(), "root");
        sinkConfig.put(DorisConfigOptions.PASSWORD.key(), "");
        sinkConfig.put(DorisConfigOptions.JDBC_URL.key(), "jdbc:mysql://10.20.30.1:9030");
        sinkConfig.put(DorisConfigOptions.SINK_LABEL_PREFIX.key(), UUID.randomUUID().toString());
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put(DatabaseSyncConfig.REPLICATION_NUM, "1");
        tableConfig.put(DatabaseSyncConfig.TABLE_BUCKETS, "tbl1:10,tbl2:20,a.*:30,b.*:40,.*:50");
        String includingTables = "a_.*|b_.*|c";
        String excludingTables = "";
        String multiToOneOrigin = "a_.*|b_.*";
        String multiToOneTarget = "a|b";
        boolean ignoreDefaultValue = false;
        boolean useNewSchemaChange = true;
        boolean ignoreIncompatible = false;
        DatabaseSync databaseSync = new OracleDatabaseSync();
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
                .setIgnoreIncompatible(ignoreIncompatible)
                .create();
        databaseSync.build();
        env.execute(String.format("Oracle-Doris Database Sync: %s", database));
    }
}
