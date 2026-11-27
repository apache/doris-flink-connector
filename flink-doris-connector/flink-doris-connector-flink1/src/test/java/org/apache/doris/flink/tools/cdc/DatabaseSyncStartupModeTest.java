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

import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

import org.apache.doris.flink.tools.cdc.db2.Db2DatabaseSync;
import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;
import org.apache.doris.flink.tools.cdc.oracle.OracleDatabaseSync;
import org.apache.doris.flink.tools.cdc.postgres.PostgresDatabaseSync;
import org.apache.doris.flink.tools.cdc.sqlserver.SqlServerDatabaseSync;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

/** Unit tests for startup mode handling in CDC sources. */
public class DatabaseSyncStartupModeTest {

    private static final String HOSTNAME = "127.0.0.1";
    private static final String USERNAME = "user";
    private static final String PASSWORD = "pass";
    private static final String DATABASE = "test_db";
    private static final String TABLE = "test_table";
    private static final String SCHEMA = "test_schema";

    private enum DatabaseType {
        MYSQL,
        POSTGRES,
        SQLSERVER,
        ORACLE,
        DB2
    }

    private static void assertStartupMode(
            DatabaseType dbType,
            Map<String, String> config,
            String expectedStartupMode,
            boolean expectException)
            throws Exception {
        DatabaseSync sync = createDatabaseSync(dbType);
        sync.setConfig(Configuration.fromMap(config));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        try {
            DataStreamSource<String> source = sync.buildCdcSource(env);
            if (expectException) {
                Assert.fail(
                        "Expected exception for invalid startup mode, but source was built successfully");
            }
            // Extract startup mode from source
            String actualStartupMode = extractStartupMode(source);
            Assert.assertEquals(dbType.name(), expectedStartupMode, actualStartupMode);
        } catch (IllegalArgumentException e) {
            if (!expectException) {
                throw e;
            }
            Assert.assertTrue(e.getMessage().contains("scan.startup.mode"));
            Assert.assertTrue(e.getMessage().contains(config.get("scan.startup.mode")));
        }
    }

    private static DatabaseSync createDatabaseSync(DatabaseType type) throws Exception {
        switch (type) {
            case MYSQL:
                return new MysqlDatabaseSync();
            case POSTGRES:
                return new PostgresDatabaseSync();
            case SQLSERVER:
                return new SqlServerDatabaseSync();
            case ORACLE:
                return new OracleDatabaseSync();
            case DB2:
                return new Db2DatabaseSync();
            default:
                throw new IllegalArgumentException("Unsupported database type: " + type);
        }
    }

    private static String extractStartupMode(DataStreamSource<String> source) throws Exception {
        SourceTransformation<?, ?, ?> transformation =
                (SourceTransformation<?, ?, ?>) source.getTransformation();
        Object sourceObject = transformation.getSource();
        if (sourceObject instanceof MySqlSource) {
            return ((MySqlSource<?>) sourceObject)
                    .getConfigFactory()
                    .createConfig(0)
                    .getStartupOptions()
                    .startupMode
                    .name();
        }
        Field configFactoryField =
                findFieldInClassHierarchy(sourceObject.getClass(), "configFactory");
        Assert.assertNotNull("configFactory field not found", configFactoryField);
        configFactoryField.setAccessible(true);
        Object configFactory = configFactoryField.get(sourceObject);
        Method createMethod = configFactory.getClass().getMethod("create", int.class);
        Object sourceConfig = createMethod.invoke(configFactory, 0);
        Method getStartupOptionsMethod = sourceConfig.getClass().getMethod("getStartupOptions");
        Object startupOptions = getStartupOptionsMethod.invoke(sourceConfig);
        Field startupModeField = startupOptions.getClass().getField("startupMode");
        Object startupModeEnum = startupModeField.get(startupOptions);
        return ((Enum<?>) startupModeEnum).name();
    }

    private static Field findFieldInClassHierarchy(Class<?> clazz, String fieldName) {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        return null;
    }

    @Test
    public void testDefaultStartupModeIsInitial() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {
            Map<String, String> config = buildRequiredConfig(dbType);
            // Default: do not set scan.startup.mode, so it should default to initial.
            assertStartupMode(dbType, config, "INITIAL", false);
        }
    }

    @Test
    public void testExplicitLatestOffset() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {
            Map<String, String> config = buildRequiredConfig(dbType);
            config.put("scan.startup.mode", "latest-offset");
            assertStartupMode(dbType, config, "LATEST_OFFSET", false);
        }
    }

    @Test
    public void testMixedCaseLatestOffset() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {
            Map<String, String> config = buildRequiredConfig(dbType);
            config.put("scan.startup.mode", "Latest-Offset");
            assertStartupMode(dbType, config, "LATEST_OFFSET", false);
        }
    }

    @Test
    public void testInvalidInitialOffsetRejected() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {
            Map<String, String> config = buildRequiredConfig(dbType);
            config.put("scan.startup.mode", "initial-offset");
            assertStartupMode(dbType, config, null, true);
        }
    }

    @Test
    public void testCommittedOffset() throws Exception {
        // Only PostgreSQL supports committed-offset. For others, it should be rejected.

        for (DatabaseType dbType : DatabaseType.values()) {
            if (dbType == DatabaseType.POSTGRES) {
                continue;
            }
            Map<String, String> config = buildRequiredConfig(dbType);
            config.put("scan.startup.mode", "committed-offset");
            assertStartupMode(dbType, config, null, true);
        }
        DatabaseType pgType = DatabaseType.POSTGRES;
        Map<String, String> pgConfig = buildRequiredConfig(pgType);
        pgConfig.put("scan.startup.mode", "committed-offset");
        assertStartupMode(pgType, pgConfig, "COMMITTED_OFFSETS", false);
    }

    private static Map<String, String> buildRequiredConfig(DatabaseType dbType) {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("hostname", HOSTNAME);
        config.put("username", USERNAME);
        config.put("password", PASSWORD);
        config.put("database-name", DATABASE);
        config.put("table-name", TABLE);
        config.put("scan.incremental.snapshot.enabled", "true");
        switch (dbType) {
            case MYSQL:
                config.put("port", "3306");
                break;
            case POSTGRES:
                config.put("port", "5432");
                config.put("schema-name", SCHEMA);
                config.put("slot.name", "test_slot");
                break;
            case SQLSERVER:
                config.put("port", "1433");
                config.put("schema-name", SCHEMA);
                break;
            case ORACLE:
                config.put("port", "1521");
                config.put("schema-name", SCHEMA);
                config.put("url", "jdbc:oracle:thin:@" + HOSTNAME + ":1521/" + DATABASE);
                break;
            case DB2:
                config.put("port", "50000");
                config.put("schema-name", SCHEMA);
                break;
            default:
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
        return config;
    }
}
