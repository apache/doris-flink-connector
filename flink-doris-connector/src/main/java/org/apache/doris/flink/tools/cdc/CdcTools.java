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


import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * cdc sync tools
 */
public class CdcTools {
    private static final String MYSQL_SYNC_DATABASE = "mysql-sync-database";
    private static final List<String> EMPTY_KEYS = Arrays.asList("password");

    public static void main(String[] args) throws Exception {
        String operation = args[0].toLowerCase();
        String[] opArgs = Arrays.copyOfRange(args, 1, args.length);
        switch (operation) {
            case MYSQL_SYNC_DATABASE:
                createMySQLSyncDatabase(opArgs);
            default:
                System.out.println("Unknown operation " + operation);
                System.exit(1);
        }
    }

    private static void createMySQLSyncDatabase(String[] opArgs) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(opArgs);
        String database = params.get("database");
        String tablePrefix = params.get("table-prefix");
        String tableSuffix = params.get("table-suffix");
        String includingTables = params.get("including-tables");
        String excludingTables = params.get("excluding-tables");

        Map<String, String> mysqlMap = getConfigMap(params, "mysql-conf");
        Map<String, String> sinkMap = getConfigMap(params, "sink-conf");
        Map<String, String> tableMap = getConfigMap(params, "table-conf");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration mysqlConfig = Configuration.fromMap(mysqlMap);
        Configuration sinkConfig = Configuration.fromMap(sinkMap);

        DatabaseSync databaseSync = new MysqlDatabaseSync();
        databaseSync.create(env, database, mysqlConfig, tablePrefix, tableSuffix, includingTables, excludingTables, sinkConfig, tableMap);
        databaseSync.build();
        env.execute(String.format("MySQL-Doris Database Sync: %s", database));
    }

    private static Map<String, String> getConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            return null;
        }

        Map<String, String> map = new HashMap<>();
        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=");
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
                continue;
            }else if(kv.length == 1 && EMPTY_KEYS.contains(kv[0])){
                map.put(kv[0], "");
                continue;
            }

            System.err.println(
                    "Invalid " + key + " " + param + ".\n");
            return null;
        }
        return map;
    }
}