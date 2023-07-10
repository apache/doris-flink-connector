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
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
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

import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECTION_POOL_SIZE;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_MAX_RETRIES;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;

public class OracleDatabaseSync extends DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(OracleDatabaseSync.class);

    private static String JDBC_URL = "jdbc:oracle:thin:@%s:%d:%s";

    public OracleDatabaseSync() {
    }

    @Override
    public Connection getConnection() throws SQLException {
        String jdbcUrl;
        if(!StringUtils.isNullOrWhitespaceOnly(config.get(OracleSourceOptions.URL))){
            jdbcUrl = config.get(OracleSourceOptions.URL);
        }else{
            jdbcUrl = String.format(JDBC_URL, config.get(OracleSourceOptions.HOSTNAME), config.get(OracleSourceOptions.PORT),config.get(OracleSourceOptions.DATABASE_NAME));
        }
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
        String databaseName = config.get(OracleSourceOptions.DATABASE_NAME);
        String schemaName = config.get(OracleSourceOptions.SCHEMA_NAME);
        Preconditions.checkNotNull(databaseName, "database-name in oracle is required");
        Preconditions.checkNotNull(schemaName, "schema-name in oracle is required");
        String tableName = config.get(OracleSourceOptions.TABLE_NAME);
        String url = config.get(OracleSourceOptions.URL);
        String hostname = config.get(OracleSourceOptions.HOSTNAME);
        Integer port = config.get(OracleSourceOptions.PORT);
        String username = config.get(OracleSourceOptions.USERNAME);
        String password = config.get(OracleSourceOptions.PASSWORD);

        StartupOptions startupOptions = StartupOptions.initial();
        String startupMode = config.get(OracleSourceOptions.SCAN_STARTUP_MODE);
        if ("initial".equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.initial();
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.latest();
        }

        //debezium properties set
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

        Map<String, Object> customConverterConfigs = new HashMap<>();
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(false, customConverterConfigs);

        if(config.getBoolean(OracleSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED, false)){
            JdbcIncrementalSource<String> incrSource = OracleSourceBuilder.OracleIncrementalSource.<String>builder()
                    .hostname(hostname)
                    .url(url)
                    .port(port)
                    .databaseList(database)
                    .schemaList(schemaName)
                    .tableList(schemaName + "." + tableName)
                    .username(username)
                    .password(password)
                    .startupOptions(startupOptions)
                    .deserializer(schema)
                    .debeziumProperties(debeziumProperties)
                    .splitSize(config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE))
                    .splitMetaGroupSize(config.get(CHUNK_META_GROUP_SIZE))
                    .fetchSize(config.get(SCAN_SNAPSHOT_FETCH_SIZE))
                    .connectTimeout(config.get(CONNECT_TIMEOUT))
                    .connectionPoolSize(config.get(CONNECTION_POOL_SIZE))
                    .connectMaxRetries(config.get(CONNECT_MAX_RETRIES))
                    .distributionFactorUpper(config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND))
                    .distributionFactorLower(config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND))
                    .build();
            return env.fromSource(incrSource,  WatermarkStrategy.noWatermarks(), "Oracle IncrSource");
        }else{
            DebeziumSourceFunction<String> oracleSource = OracleSource.<String>builder().url(url)
                    .hostname(hostname)
                    .port(port)
                    .username(username)
                    .password(password)
                    .database(databaseName)
                    .schemaList(schemaName)
                    .tableList(schemaName + "." + tableName)
                    .debeziumProperties(debeziumProperties)
                    .startupOptions(startupOptions)
                    .deserializer(schema)
                    .build();
            return env.addSource(oracleSource, "Oracle Source");
        }
    }
}
