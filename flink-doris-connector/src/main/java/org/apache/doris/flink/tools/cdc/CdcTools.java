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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.doris.flink.tools.cdc.mongodb.MongoDBDatabaseSync;
import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;
import org.apache.doris.flink.tools.cdc.oracle.OracleDatabaseSync;
import org.apache.doris.flink.tools.cdc.postgres.PostgresDatabaseSync;
import org.apache.doris.flink.tools.cdc.sqlserver.SqlServerDatabaseSync;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** cdc sync tools. */
public class CdcTools {
    private static final List<String> EMPTY_KEYS =
            Collections.singletonList(DatabaseSyncConfig.PASSWORD);

    public static void main(String[] args) throws Exception {
        System.out.println("Input args: " + Arrays.asList(args) + ".\n");
        String operation = args[0].toLowerCase();
        String[] opArgs = Arrays.copyOfRange(args, 1, args.length);
        switch (operation) {
            case DatabaseSyncConfig.MYSQL_SYNC_DATABASE:
                createMySQLSyncDatabase(opArgs);
                break;
            case DatabaseSyncConfig.ORACLE_SYNC_DATABASE:
                createOracleSyncDatabase(opArgs);
                break;
            case DatabaseSyncConfig.POSTGRES_SYNC_DATABASE:
                createPostgresSyncDatabase(opArgs);
                break;
            case DatabaseSyncConfig.SQLSERVER_SYNC_DATABASE:
                createSqlServerSyncDatabase(opArgs);
                break;
            case DatabaseSyncConfig.MONGODB_SYNC_DATABASE:
                createMongoDBSyncDatabase(opArgs);
                break;
            default:
                System.out.println("Unknown operation " + operation);
                System.exit(1);
        }
    }

    private static void createMySQLSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        Preconditions.checkArgument(params.has(DatabaseSyncConfig.MYSQL_CONF));
        Map<String, String> mysqlMap = getConfigMap(params, DatabaseSyncConfig.MYSQL_CONF);
        Configuration mysqlConfig = Configuration.fromMap(mysqlMap);
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        syncDatabase(params, databaseSync, mysqlConfig, SourceConnector.MYSQL);
    }

    private static void createOracleSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        Preconditions.checkArgument(params.has(DatabaseSyncConfig.ORACLE_CONF));
        Map<String, String> oracleMap = getConfigMap(params, DatabaseSyncConfig.ORACLE_CONF);
        Configuration oracleConfig = Configuration.fromMap(oracleMap);
        DatabaseSync databaseSync = new OracleDatabaseSync();
        syncDatabase(params, databaseSync, oracleConfig, SourceConnector.ORACLE);
    }

    private static void createPostgresSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        Preconditions.checkArgument(params.has(DatabaseSyncConfig.POSTGRES_CONF));
        Map<String, String> postgresMap = getConfigMap(params, DatabaseSyncConfig.POSTGRES_CONF);
        Configuration postgresConfig = Configuration.fromMap(postgresMap);
        DatabaseSync databaseSync = new PostgresDatabaseSync();
        syncDatabase(params, databaseSync, postgresConfig, SourceConnector.POSTGRES);
    }

    private static void createSqlServerSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        Preconditions.checkArgument(params.has(DatabaseSyncConfig.SQLSERVER_CONF));
        Map<String, String> postgresMap = getConfigMap(params, DatabaseSyncConfig.SQLSERVER_CONF);
        Configuration postgresConfig = Configuration.fromMap(postgresMap);
        DatabaseSync databaseSync = new SqlServerDatabaseSync();
        syncDatabase(params, databaseSync, postgresConfig, SourceConnector.SQLSERVER);
    }

    private static void createMongoDBSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        Preconditions.checkArgument(params.has(DatabaseSyncConfig.MONGODB_CONF));
        Map<String, String> mongoMap = getConfigMap(params, DatabaseSyncConfig.MONGODB_CONF);
        Configuration mongoConfig = Configuration.fromMap(mongoMap);
        DatabaseSync databaseSync = new MongoDBDatabaseSync();
        syncDatabase(params, databaseSync, mongoConfig, SourceConnector.MONGODB);
    }

    private static void syncDatabase(
            MultipleParameterTool params,
            DatabaseSync databaseSync,
            Configuration config,
            SourceConnector sourceConnector)
            throws Exception {
        String jobName = params.get(DatabaseSyncConfig.JOB_NAME);
        String database = params.get(DatabaseSyncConfig.DATABASE);
        String tablePrefix = params.get(DatabaseSyncConfig.TABLE_PREFIX);
        String tableSuffix = params.get(DatabaseSyncConfig.TABLE_SUFFIX);
        String includingTables = params.get(DatabaseSyncConfig.INCLUDING_TABLES);
        String excludingTables = params.get(DatabaseSyncConfig.EXCLUDING_TABLES);
        String multiToOneOrigin = params.get(DatabaseSyncConfig.MULTI_TO_ONE_ORIGIN);
        String multiToOneTarget = params.get(DatabaseSyncConfig.MULTI_TO_ONE_TARGET);
        String schemaChangeMode = params.get(DatabaseSyncConfig.SCHEMA_CHANGE_MODE);
        boolean createTableOnly = params.has(DatabaseSyncConfig.CREATE_TABLE_ONLY);
        boolean ignoreDefaultValue = params.has(DatabaseSyncConfig.IGNORE_DEFAULT_VALUE);
        boolean ignoreIncompatible = params.has(DatabaseSyncConfig.IGNORE_INCOMPATIBLE);
        boolean singleSink = params.has(DatabaseSyncConfig.SINGLE_SINK);

        Preconditions.checkArgument(params.has(DatabaseSyncConfig.SINK_CONF));
        Map<String, String> sinkMap = getConfigMap(params, DatabaseSyncConfig.SINK_CONF);
        Map<String, String> tableMap = getConfigMap(params, DatabaseSyncConfig.TABLE_CONF);
        Configuration sinkConfig = Configuration.fromMap(sinkMap);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                .setSinkConfig(sinkConfig)
                .setTableConfig(tableMap)
                .setCreateTableOnly(createTableOnly)
                .setSingleSink(singleSink)
                .setIgnoreIncompatible(ignoreIncompatible)
                .setSchemaChangeMode(schemaChangeMode)
                .create();
        databaseSync.build();
        if (StringUtils.isNullOrWhitespaceOnly(jobName)) {
            jobName =
                    String.format(
                            "%s-Doris Sync Database: %s",
                            sourceConnector.getConnectorName(),
                            config.getString(
                                    DatabaseSyncConfig.DATABASE_NAME, DatabaseSyncConfig.DB));
        }
        env.execute(jobName);
    }

    @VisibleForTesting
    public static Map<String, String> getConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            System.out.println(
                    "Can not find key ["
                            + key
                            + "] from args: "
                            + params.toMap().toString()
                            + ".\n");
            return null;
        }

        Map<String, String> map = new HashMap<>();
        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
                continue;
            } else if (kv.length == 1 && EMPTY_KEYS.contains(kv[0])) {
                map.put(kv[0], "");
                continue;
            }

            System.out.println("Invalid " + key + " " + param + ".\n");
            return null;
        }
        return map;
    }
}
