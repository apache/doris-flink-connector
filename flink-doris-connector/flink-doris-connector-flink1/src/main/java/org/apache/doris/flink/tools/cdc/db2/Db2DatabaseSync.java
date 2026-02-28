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

package org.apache.doris.flink.tools.cdc.db2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.db2.Db2Source;
import org.apache.flink.cdc.connectors.db2.source.Db2SourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.tools.cdc.DatabaseSync;
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
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;

public class Db2DatabaseSync extends DatabaseSync {
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(50000)
                    .withDescription("Integer port number of the DB2 database server.");
    private static final Logger LOG = LoggerFactory.getLogger(Db2DatabaseSync.class);

    private static final String JDBC_URL = "jdbc:db2://%s:%d/%s";

    public Db2DatabaseSync() throws SQLException {
        super();
    }

    @Override
    public void registerDriver() throws SQLException {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            LOG.info(" Loaded the JDBC driver");
        } catch (ClassNotFoundException ex) {
            throw new SQLException(
                    "No suitable driver found, can not found class com.ibm.db2.jcc.DB2Driver");
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
                        config.get(PORT),
                        config.get(JdbcSourceOptions.DATABASE_NAME));
        Properties pro = new Properties();
        pro.setProperty("user", config.get(JdbcSourceOptions.USERNAME));
        pro.setProperty("password", config.get(JdbcSourceOptions.PASSWORD));
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
                    metaData.getTables(null, schemaName, "%", new String[] {"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String tableComment = tables.getString("REMARKS");
                    if (!isSyncNeeded(tableName)) {
                        continue;
                    }
                    SourceSchema sourceSchema =
                            new Db2Schema(
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
        Preconditions.checkNotNull(databaseName, "database-name in DB2 is required");
        Preconditions.checkNotNull(schemaName, "schema-name in DB2 is required");

        String tableName = config.get(JdbcSourceOptions.TABLE_NAME);
        String hostname = config.get(JdbcSourceOptions.HOSTNAME);
        Integer port = config.get(PORT);
        String username = config.get(JdbcSourceOptions.USERNAME);
        String password = config.get(JdbcSourceOptions.PASSWORD);

        StartupOptions startupOptions = StartupOptions.initial();
        String startupMode = config.get(SourceOptions.SCAN_STARTUP_MODE);
        if ("initial".equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.initial();
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.latest();
        }

        // debezium properties set
        Properties debeziumProperties = new Properties();
        debeziumProperties.putAll(Db2DateConverter.DEFAULT_PROPS);
        debeziumProperties.put("decimal.handling.mode", "string");

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
            schema = new JsonDebeziumDeserializationSchema(false, customConverterConfigs);
        }

        if (config.getBoolean(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED, true)) {
            JdbcIncrementalSource<String> db2IncrementalSource =
                    Db2SourceBuilder.Db2IncrementalSource.<String>builder()
                            .hostname(hostname)
                            .port(port)
                            .databaseList(databaseName)
                            .tableList(tableName)
                            .username(username)
                            .password(password)
                            .deserializer(schema)
                            .debeziumProperties(debeziumProperties)
                            .startupOptions(startupOptions)
                            .includeSchemaChanges(true)
                            .debeziumProperties(debeziumProperties)
                            .serverTimeZone(config.get(SERVER_TIME_ZONE))
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
                    db2IncrementalSource, WatermarkStrategy.noWatermarks(), "Db2IncrementalSource");

        } else {
            DebeziumSourceFunction<String> db2Source =
                    Db2Source.<String>builder()
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
            return env.addSource(db2Source, "Db2 Source");
        }
    }

    @Override
    public String getTableListPrefix() {
        return config.get(JdbcSourceOptions.SCHEMA_NAME);
    }

    @Override
    protected String getJdbcUrlTemplate(String initialJdbcUrl, Properties jdbcProperties) {
        StringBuilder jdbcUrlBuilder = new StringBuilder(initialJdbcUrl);
        boolean firstParam = true;
        for (Map.Entry<Object, Object> entry : jdbcProperties.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            if (firstParam) {
                jdbcUrlBuilder.append(":").append(key).append("=").append(value).append(";");
                firstParam = false;
            } else {
                jdbcUrlBuilder.append(key).append("=").append(value).append(";");
            }
        }
        return jdbcUrlBuilder.toString();
    }
}
