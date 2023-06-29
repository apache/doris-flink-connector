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
package org.apache.doris.flink.tools.cdc.oracle;


import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OracleDatabaseSync extends DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(OracleDatabaseSync.class);

    private static String JDBC_URL = "jdbc:oracle:thin:@%s:%d";

    public OracleDatabaseSync() {
    }

    @Override
    public Connection getConnection() throws SQLException {
        String jdbcUrl = String.format(JDBC_URL, config.get(OracleSourceOptions.HOSTNAME), config.get(OracleSourceOptions.PORT));
        Properties pro = new Properties();
        pro.setProperty("user", config.get(OracleSourceOptions.USERNAME));
        pro.setProperty("password", config.get(OracleSourceOptions.PASSWORD));
        pro.put("remarksReporting", "true");
        return DriverManager.getConnection(jdbcUrl, pro);
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        String databaseName = config.get(OracleSourceOptions.DATABASE_NAME);
        String schemaName = config.get(OracleSourceOptions.SCHEMA_NAME);
        List<SourceSchema> schemaList = new ArrayList<>();
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables =
                         metaData.getTables(databaseName, schemaName, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String tableComment = tables.getString("REMARKS");
                    if (!isSyncNeeded(tableName)) {
                        continue;
                    }
                    SourceSchema sourceSchema =
                            new OracleSchema(metaData, databaseName, tableName, tableComment);
                    if (sourceSchema.primaryKeys.size() > 0) {
                        //Only sync tables with primary keys
                        schemaList.add(sourceSchema);
                    } else {
                        LOG.warn("table {} has no primary key, skip", tableName);
                        System.out.println("table " + tableName + " has no primary key, skip.");
                    }
                }
            }
        }
        return schemaList;
    }

    @Override
    public DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env) {
        OracleSource.Builder<String> sourceBuilder = OracleSource.builder();

        String databaseName = config.get(OracleSourceOptions.DATABASE_NAME);
        String schemaName = config.get(OracleSourceOptions.SCHEMA_NAME);
        Preconditions.checkNotNull(databaseName, "database-name in oracle is required");
        Preconditions.checkNotNull(schemaName, "schema-name in oracle is required");
        String tableName = config.get(OracleSourceOptions.TABLE_NAME);
        sourceBuilder
                .hostname(config.get(OracleSourceOptions.HOSTNAME))
                .port(config.get(OracleSourceOptions.PORT))
                .username(config.get(OracleSourceOptions.USERNAME))
                .password(config.get(OracleSourceOptions.PASSWORD))
                .database(databaseName)
                .schemaList(schemaName)
                .tableList(schemaName + "." + tableName);

        String startupMode = config.get(OracleSourceOptions.SCAN_STARTUP_MODE);
        if ("initial".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.initial());
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.latest());
        }

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("decimal.handling.mode", "string");

        //date to string
        debeziumProperties.putAll(OracleDateConverter.DEFAULT_PROPS);

        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX)) {
                debeziumProperties.put(
                        key.substring(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX.length()), value);
            }
        }
        sourceBuilder.debeziumProperties(debeziumProperties);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(false, customConverterConfigs);
        SourceFunction<String> oracleSource = sourceBuilder.deserializer(schema).build();

        DataStreamSource<String> streamSource = env.addSource(oracleSource, "Oracle Source");
        return streamSource;
    }
}
