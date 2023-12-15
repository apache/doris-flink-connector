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

package org.apache.doris.flink.tools.cdc.postgres;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import com.ververica.cdc.connectors.base.options.SourceOptions;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.doris.flink.tools.cdc.SourceSchema;
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
import static com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions.DECODING_PLUGIN_NAME;
import static com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions.HEARTBEAT_INTERVAL;
import static com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions.SLOT_NAME;

public class PostgresDatabaseSync extends DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresDatabaseSync.class);

    private static final String JDBC_URL = "jdbc:postgresql://%s:%d/%s";

    public PostgresDatabaseSync() throws SQLException {
        super();
    }

    @Override
    public void registerDriver() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            throw new SQLException(
                    "No suitable driver found, can not found class org.postgresql.Driver");
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        String jdbcUrl =
                String.format(
                        JDBC_URL,
                        config.get(PostgresSourceOptions.HOSTNAME),
                        config.get(PostgresSourceOptions.PG_PORT),
                        config.get(PostgresSourceOptions.DATABASE_NAME));
        Properties pro = new Properties();
        pro.setProperty("user", config.get(PostgresSourceOptions.USERNAME));
        pro.setProperty("password", config.get(PostgresSourceOptions.PASSWORD));
        return DriverManager.getConnection(jdbcUrl, pro);
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        String databaseName = config.get(PostgresSourceOptions.DATABASE_NAME);
        String schemaName = config.get(PostgresSourceOptions.SCHEMA_NAME);
        List<SourceSchema> schemaList = new ArrayList<>();
        LOG.info("database-name {}, schema-name {}", databaseName, schemaName);
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables =
                    metaData.getTables(databaseName, schemaName, "%", new String[] {"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String tableComment = tables.getString("REMARKS");
                    if (!isSyncNeeded(tableName)) {
                        continue;
                    }
                    SourceSchema sourceSchema =
                            new PostgresSchema(
                                    metaData, databaseName, schemaName, tableName, tableComment);
                    sourceSchema.setModel(
                            sourceSchema.primaryKeys.size() > 0
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
        String databaseName = config.get(PostgresSourceOptions.DATABASE_NAME);
        String schemaName = config.get(PostgresSourceOptions.SCHEMA_NAME);
        String slotName = config.get(SLOT_NAME);
        Preconditions.checkNotNull(databaseName, "database-name in postgres is required");
        Preconditions.checkNotNull(schemaName, "schema-name in postgres is required");
        Preconditions.checkNotNull(slotName, "slot.name in postgres is required");

        String tableName = config.get(PostgresSourceOptions.TABLE_NAME);
        String hostname = config.get(PostgresSourceOptions.HOSTNAME);
        Integer port = config.get(PostgresSourceOptions.PG_PORT);
        String username = config.get(PostgresSourceOptions.USERNAME);
        String password = config.get(PostgresSourceOptions.PASSWORD);

        StartupOptions startupOptions = StartupOptions.initial();
        String startupMode = config.get(PostgresSourceOptions.SCAN_STARTUP_MODE);
        if ("initial".equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.initial();
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.latest();
        }

        // debezium properties set
        Properties debeziumProperties = new Properties();
        debeziumProperties.putAll(PostgresDateConverter.DEFAULT_PROPS);
        debeziumProperties.put("decimal.handling.mode", "string");

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

        if (config.getBoolean(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED, false)) {
            JdbcIncrementalSource<String> incrSource =
                    PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                            .hostname(hostname)
                            .port(port)
                            .database(databaseName)
                            .schemaList(schemaName)
                            .tableList(tableName)
                            .username(username)
                            .password(password)
                            .deserializer(schema)
                            .slotName(slotName)
                            .decodingPluginName(config.get(DECODING_PLUGIN_NAME))
                            .includeSchemaChanges(true)
                            .debeziumProperties(debeziumProperties)
                            .startupOptions(startupOptions)
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
                            .heartbeatInterval(config.get(HEARTBEAT_INTERVAL))
                            .build();
            return env.fromSource(
                    incrSource, WatermarkStrategy.noWatermarks(), "Postgres IncrSource");
        } else {
            DebeziumSourceFunction<String> postgresSource =
                    PostgreSQLSource.<String>builder()
                            .hostname(hostname)
                            .port(port)
                            .database(databaseName)
                            .schemaList(schemaName)
                            .tableList(tableName)
                            .username(username)
                            .password(password)
                            .debeziumProperties(debeziumProperties)
                            .deserializer(schema)
                            .slotName(slotName)
                            .decodingPluginName(config.get(DECODING_PLUGIN_NAME))
                            .build();
            return env.addSource(postgresSource, "Postgres Source");
        }
    }

    @Override
    public String getTableListPrefix() {
        String schemaName = config.get(PostgresSourceOptions.SCHEMA_NAME);
        return schemaName;
    }
}
