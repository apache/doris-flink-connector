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

package org.apache.doris.flink.tools.cdc.sqlserver;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerSource;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.doris.flink.tools.cdc.DatabaseSyncConfig;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.doris.flink.tools.cdc.deserialize.DorisJsonDebeziumDeserializationSchema;
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

import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;

public class SqlServerDatabaseSync extends DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerDatabaseSync.class);
    private static final String JDBC_URL = "jdbc:sqlserver://%s:%d;database=%s;";
    private static final String PORT = "port";

    public SqlServerDatabaseSync() throws SQLException {
        super();
    }

    @Override
    public void registerDriver() throws SQLException {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException ex) {
            throw new SQLException(
                    "No suitable driver found, can not found class com.microsoft.sqlserver.jdbc.SQLServerDriver");
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        Properties jdbcProperties = getJdbcProperties();
        String jdbcUrlTemplate = getJdbcUrlTemplate(JDBC_URL, jdbcProperties);
        String jdbcUrl =
                String.format(
                        jdbcUrlTemplate,
                        config.get(JdbcSourceOptions.HOSTNAME),
                        config.getInteger(PORT, 1433),
                        config.get(JdbcSourceOptions.DATABASE_NAME));
        Properties pro = new Properties();
        pro.setProperty(DatabaseSyncConfig.USER, config.get(JdbcSourceOptions.USERNAME));
        pro.setProperty(DatabaseSyncConfig.PASSWORD, config.get(JdbcSourceOptions.PASSWORD));
        return DriverManager.getConnection(jdbcUrl, pro);
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        String databaseName = config.get(JdbcSourceOptions.DATABASE_NAME);
        String schemaName = config.get(JdbcSourceOptions.SCHEMA_NAME);
        List<SourceSchema> schemaList = new ArrayList<>();
        LOG.info("database-name {}, schema-name {}", databaseName, schemaName);
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables =
                    metaData.getTables(databaseName, schemaName, "%", new String[] {"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString(DatabaseSyncConfig.TABLE_NAME);
                    String tableComment = tables.getString(DatabaseSyncConfig.REMARKS);
                    if (!isSyncNeeded(tableName)) {
                        continue;
                    }
                    SourceSchema sourceSchema =
                            new SqlServerSchema(
                                    metaData, databaseName, schemaName, tableName, tableComment);
                    sourceSchema.setModel(
                            !sourceSchema.primaryKeys.isEmpty()
                                    ? DataModel.UNIQUE
                                    : DataModel.DUPLICATE);
                    schemaList.add(sourceSchema);
                }
            }
        }
        return schemaList;
    }

    @Override
    public DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env) {
        String databaseName = config.get(JdbcSourceOptions.DATABASE_NAME);
        String schemaName = config.get(JdbcSourceOptions.SCHEMA_NAME);
        Preconditions.checkNotNull(databaseName, "database-name in sqlserver is required");
        Preconditions.checkNotNull(schemaName, "schema-name in sqlserver is required");

        String tableName = config.get(JdbcSourceOptions.TABLE_NAME);
        String hostname = config.get(JdbcSourceOptions.HOSTNAME);
        int port = config.getInteger(PORT, 1433);
        String username = config.get(JdbcSourceOptions.USERNAME);
        String password = config.get(JdbcSourceOptions.PASSWORD);

        StartupOptions startupOptions = StartupOptions.initial();
        String startupMode = config.get(JdbcSourceOptions.SCAN_STARTUP_MODE);
        if (DatabaseSyncConfig.SCAN_STARTUP_MODE_VALUE_INITIAL.equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.initial();
        } else if (DatabaseSyncConfig.SCAN_STARTUP_MODE_VALUE_LATEST_OFFSET.equalsIgnoreCase(
                startupMode)) {
            startupOptions = StartupOptions.latest();
        }

        // debezium properties set
        Properties debeziumProperties = new Properties();
        debeziumProperties.putAll(SqlServerDateConverter.DEFAULT_PROPS);
        debeziumProperties.put(DatabaseSyncConfig.DECIMAL_HANDLING_MODE, "string");

        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX)) {
                debeziumProperties.put(
                        key.substring(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX.length()), value);
            }
        }

        DebeziumDeserializationSchema<String> schema;
        if (ignoreDefaultValue) {
            schema = new DorisJsonDebeziumDeserializationSchema();
        } else {
            Map<String, Object> customConverterConfigs = new HashMap<>();
            customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
            schema = new JsonDebeziumDeserializationSchema(false, customConverterConfigs);
        }

        if (config.getBoolean(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED, false)) {
            JdbcIncrementalSource<String> incrSource =
                    SqlServerSourceBuilder.SqlServerIncrementalSource.<String>builder()
                            .hostname(hostname)
                            .port(port)
                            .databaseList(databaseName)
                            .tableList(tableName)
                            .username(username)
                            .password(password)
                            .startupOptions(startupOptions)
                            .deserializer(schema)
                            .includeSchemaChanges(true)
                            .debeziumProperties(debeziumProperties)
                            .splitSize(config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE))
                            .splitMetaGroupSize(config.get(CHUNK_META_GROUP_SIZE))
                            .fetchSize(config.get(SCAN_SNAPSHOT_FETCH_SIZE))
                            .connectTimeout(config.get(CONNECT_TIMEOUT))
                            .connectionPoolSize(config.get(CONNECTION_POOL_SIZE))
                            .connectMaxRetries(config.get(CONNECT_MAX_RETRIES))
                            .distributionFactorUpper(
                                    config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND))
                            .distributionFactorLower(
                                    config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND))
                            .build();
            return env.fromSource(
                    incrSource, WatermarkStrategy.noWatermarks(), "SqlServer IncrSource");
        } else {
            DebeziumSourceFunction<String> sqlServerSource =
                    SqlServerSource.<String>builder()
                            .hostname(hostname)
                            .port(port)
                            .database(databaseName)
                            .tableList(tableName)
                            .username(username)
                            .password(password)
                            .debeziumProperties(debeziumProperties)
                            .startupOptions(startupOptions)
                            .deserializer(schema)
                            .build();
            return env.addSource(sqlServerSource, "SqlServer Source");
        }
    }

    @Override
    public String getTableListPrefix() {
        return config.get(JdbcSourceOptions.SCHEMA_NAME);
    }

    @Override
    public String getJdbcUrlTemplate(String initialJdbcUrl, Properties jdbcProperties) {
        StringBuilder jdbcUrlBuilder = new StringBuilder(initialJdbcUrl);
        jdbcProperties.forEach(
                (key, value) -> jdbcUrlBuilder.append(key).append("=").append(value).append(";"));
        return jdbcUrlBuilder.toString();
    }
}
