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

package org.apache.doris.flink.tls;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.container.instance.DorisCustomerContainer;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * Opt-in TLS integration test against an externally managed Doris cluster.
 *
 * <p>Enable it with {@code customer_env=true} and {@code doris_enable_tls=true}, supply the
 * existing customer-environment connection properties, and configure {@code
 * doris_tls_ca_certificate_path} when the cluster uses a private CA. The ADBC scenarios also
 * require the Doris FE {@code doris_flight_sql_port}.
 */
@RunWith(Parameterized.class)
public class DorisTlsExternalE2ECase {

    private static final Logger LOG = LoggerFactory.getLogger(DorisTlsExternalE2ECase.class);
    private static final int DEFAULT_PARALLELISM = 2;
    private static final List<String> EXPECTED_JDBC_ROWS =
            Arrays.asList("101,tls-alpha", "102,tls-beta", "103,tls-gamma");
    private static final List<String> EXPECTED_SOURCE_ROWS =
            Arrays.asList("[101, tls-alpha]", "[102, tls-beta]", "[103, tls-gamma]");
    private static DorisCustomerContainer doris;

    private final boolean useFlightRead;
    private final boolean batchMode;

    public DorisTlsExternalE2ECase(boolean useFlightRead, boolean batchMode) {
        this.useFlightRead = useFlightRead;
        this.batchMode = batchMode;
    }

    @Parameterized.Parameters(name = "useFlightRead: {0}, batchMode: {1}")
    public static Object[] parameters() {
        return new Object[][] {
            new Object[] {false, false},
            new Object[] {false, true},
            new Object[] {true, false},
            new Object[] {true, true}
        };
    }

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .build());

    @BeforeClass
    public static void useExternalTlsEnvironment() {
        Assume.assumeTrue(
                "External Doris environment is not enabled",
                Boolean.parseBoolean(System.getProperty("customer_env", "false")));
        Assume.assumeTrue(
                "External Doris TLS environment is not enabled",
                Boolean.parseBoolean(System.getProperty("doris_enable_tls", "false")));

        doris = new DorisCustomerContainer();
        doris.startContainer();
    }

    @AfterClass
    public static void closeExternalEnvironment() {
        if (doris != null) {
            doris.close();
        }
    }

    @Test
    public void testDataStreamTlsRoundTrip() throws Exception {
        runDataStreamRoundTrip(useFlightRead, batchMode);
    }

    @Test
    public void testFlinkSqlDorisToDorisTlsRoundTrip() throws Exception {
        runFlinkSqlDorisToDorisRoundTrip(useFlightRead, batchMode);
    }

    private void runDataStreamRoundTrip(boolean useFlightRead, boolean batchMode) throws Exception {
        String database = testDatabase();
        String table = uniqueTable("datastream_" + matrixName(useFlightRead, batchMode));
        DorisOptions dorisOptions = createDorisOptions(database, table);

        try {
            createTable(database, table);
            runDataStreamHttpsSink(dorisOptions, batchMode);
            waitForExpectedRows(database, table);
            assertEquals(EXPECTED_SOURCE_ROWS, runDataStreamSource(dorisOptions, useFlightRead));
        } finally {
            dropTable(database, table);
        }
    }

    private void runFlinkSqlDorisToDorisRoundTrip(boolean useFlightRead, boolean batchMode)
            throws Exception {
        String database = testDatabase();
        String matrixName = matrixName(useFlightRead, batchMode);
        String sourceTable = uniqueTable("sql_" + matrixName + "_source");
        String targetTable = uniqueTable("sql_" + matrixName + "_target");

        try {
            createTable(database, sourceTable);
            createTable(database, targetTable);

            runDataStreamHttpsSink(createDorisOptions(database, sourceTable), false);
            waitForExpectedRows(database, sourceTable);

            runFlinkSqlDorisToDoris(database, sourceTable, targetTable, useFlightRead, batchMode);
            waitForExpectedRows(database, targetTable);
        } finally {
            dropTable(database, targetTable);
            dropTable(database, sourceTable);
        }
    }

    private DorisOptions createDorisOptions(String database, String table) {
        return DorisOptions.builder()
                .setFenodes(doris.getFenodes())
                .setTableIdentifier(database + "." + table)
                .setUsername(doris.getUsername())
                .setPassword(doris.getPassword())
                .setTlsOptions(doris.getTlsOptions())
                .build();
    }

    private void createTable(String database, String table) throws Exception {
        try (Connection connection = doris.getQueryConnection()) {
            ContainerUtils.executeSQLStatement(
                    connection,
                    LOG,
                    "CREATE DATABASE IF NOT EXISTS `" + database + "`",
                    "CREATE TABLE `"
                            + database
                            + "`.`"
                            + table
                            + "` ("
                            + "`id` INT, `name` VARCHAR(64)) "
                            + "DUPLICATE KEY(`id`) "
                            + "DISTRIBUTED BY HASH(`id`) BUCKETS 1 "
                            + "PROPERTIES (\"replication_num\" = \"1\")");
        }
    }

    private void runDataStreamHttpsSink(DorisOptions dorisOptions, boolean batchMode)
            throws Exception {
        Properties streamLoadProperties = new Properties();
        streamLoadProperties.setProperty("format", "json");
        streamLoadProperties.setProperty("read_json_by_line", "true");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setLabelPrefix("tls_external_" + UUID.randomUUID())
                        .setStreamLoadProp(streamLoadProperties)
                        .setDeletable(false)
                        .setBatchMode(batchMode)
                        .build();
        DorisSink<String> sink =
                DorisSink.<String>builder()
                        .setDorisReadOptions(DorisReadOptions.defaults())
                        .setDorisExecutionOptions(executionOptions)
                        .setSerializer(new SimpleStringSerializer())
                        .setDorisOptions(dorisOptions)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(DEFAULT_PARALLELISM);
        env.fromElements(
                        "{\"id\":101,\"name\":\"tls-alpha\"}",
                        "{\"id\":102,\"name\":\"tls-beta\"}",
                        "{\"id\":103,\"name\":\"tls-gamma\"}")
                .sinkTo(sink);
        env.execute("Doris external TLS sink");
    }

    private void waitForExpectedRows(String database, String table) {
        await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            try (Connection connection = doris.getQueryConnection()) {
                                assertEquals(
                                        EXPECTED_JDBC_ROWS,
                                        ContainerUtils.executeSQLStatement(
                                                connection,
                                                LOG,
                                                "SELECT `id`, `name` FROM `"
                                                        + database
                                                        + "`.`"
                                                        + table
                                                        + "` ORDER BY `id`",
                                                2));
                            }
                        });
    }

    private List<String> runDataStreamSource(DorisOptions dorisOptions, boolean useFlightSql)
            throws Exception {
        DorisReadOptions.Builder readOptionsBuilder =
                DorisReadOptions.builder().setUseFlightSql(useFlightSql);
        if (useFlightSql) {
            readOptionsBuilder.setFlightSqlPort(flightSqlPort());
        }
        DorisSource<List<?>> source =
                DorisSource.<List<?>>builder()
                        .setDorisOptions(dorisOptions)
                        .setDorisReadOptions(readOptionsBuilder.build())
                        .setDeserializer(new SimpleListDeserializationSchema())
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(DEFAULT_PARALLELISM);

        List<String> actual = new ArrayList<>();
        String sourceName = useFlightSql ? "Doris ADBC source" : "Doris Thrift TLS source";
        try (CloseableIterator<List<?>> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceName)
                        .executeAndCollect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        Collections.sort(actual);
        return actual;
    }

    private void runFlinkSqlDorisToDoris(
            String database,
            String sourceTable,
            String targetTable,
            boolean useFlightRead,
            boolean batchMode)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(DEFAULT_PARALLELISM);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String sourceDDL =
                String.format(
                        "CREATE TABLE doris_tls_source (\n"
                                + " id INT,\n"
                                + " name STRING\n"
                                + ") WITH (\n"
                                + " 'connector' = '%s',\n"
                                + " 'fenodes' = '%s',\n"
                                + " 'table.identifier' = '%s',\n"
                                + " 'username' = '%s',\n"
                                + " 'password' = '%s',\n"
                                + " 'doris.enable.tls' = '%s',\n"
                                + " 'doris.tls.ca-certificate-path' = '%s',\n"
                                + " 'doris.tls.skip-hostname-verification' = '%s',\n"
                                + " 'doris.tls.excluded-protocols' = '%s',\n"
                                + " 'source.use-flight-sql' = '%s',\n"
                                + " 'source.flight-sql-port' = '%s'\n"
                                + ")",
                        DorisConfigOptions.IDENTIFIER,
                        sqlLiteral(doris.getFenodes()),
                        sqlLiteral(database + "." + sourceTable),
                        sqlLiteral(doris.getUsername()),
                        sqlLiteral(doris.getPassword()),
                        doris.getTlsOptions().isEnabled(),
                        sqlLiteral(doris.getTlsOptions().getCaCertificatePath()),
                        doris.getTlsOptions().isSkipHostnameVerification(),
                        sqlLiteral(System.getProperty("doris_tls_excluded_protocols", "")),
                        useFlightRead,
                        flightSqlPort());

        String targetDDL =
                String.format(
                        "CREATE TABLE doris_tls_target (\n"
                                + " id INT,\n"
                                + " name STRING\n"
                                + ") WITH (\n"
                                + " 'connector' = '%s',\n"
                                + " 'fenodes' = '%s',\n"
                                + " 'table.identifier' = '%s',\n"
                                + " 'username' = '%s',\n"
                                + " 'password' = '%s',\n"
                                + " 'doris.enable.tls' = '%s',\n"
                                + " 'doris.tls.ca-certificate-path' = '%s',\n"
                                + " 'doris.tls.skip-hostname-verification' = '%s',\n"
                                + " 'doris.tls.excluded-protocols' = '%s',\n"
                                + " 'sink.label-prefix' = '%s',\n"
                                + " 'sink.enable.batch-mode' = '%s',\n"
                                + " 'sink.properties.format' = 'json',\n"
                                + " 'sink.properties.read_json_by_line' = 'true'\n"
                                + ")",
                        DorisConfigOptions.IDENTIFIER,
                        sqlLiteral(doris.getFenodes()),
                        sqlLiteral(database + "." + targetTable),
                        sqlLiteral(doris.getUsername()),
                        sqlLiteral(doris.getPassword()),
                        doris.getTlsOptions().isEnabled(),
                        sqlLiteral(doris.getTlsOptions().getCaCertificatePath()),
                        doris.getTlsOptions().isSkipHostnameVerification(),
                        sqlLiteral(System.getProperty("doris_tls_excluded_protocols", "")),
                        "tls_sql_" + matrixName(useFlightRead, batchMode) + "_" + UUID.randomUUID(),
                        batchMode);

        tableEnvironment.executeSql(sourceDDL);
        tableEnvironment.executeSql(targetDDL);
        tableEnvironment
                .executeSql("INSERT INTO doris_tls_target SELECT * FROM doris_tls_source")
                .await();
    }

    private void dropTable(String database, String table) {
        try (Connection connection = doris.getQueryConnection()) {
            ContainerUtils.executeSQLStatement(
                    connection, LOG, "DROP TABLE IF EXISTS `" + database + "`.`" + table + "`");
        } catch (Exception e) {
            LOG.warn("Failed to drop external TLS test table {}.{}", database, table, e);
        }
    }

    private static String systemProperty(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }

    private static String testDatabase() {
        String database = systemProperty("doris_tls_test_database", "flink_tls_test");
        validateIdentifier(database, "doris_tls_test_database");
        return database;
    }

    private static String uniqueTable(String suffix) {
        String randomSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        return "tls_external_" + suffix + "_" + randomSuffix;
    }

    private static String matrixName(boolean useFlightRead, boolean batchMode) {
        return (useFlightRead ? "adbc" : "thrift") + "_" + (batchMode ? "batch" : "non_batch");
    }

    private static int flightSqlPort() {
        String value = System.getProperty("doris_flight_sql_port");
        if (value == null) {
            throw new IllegalArgumentException(
                    "doris_flight_sql_port is required for external ADBC tests");
        }
        int port = Integer.parseInt(value);
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException(
                    "doris_flight_sql_port must be between 1 and 65535: " + value);
        }
        return port;
    }

    private static String sqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private static void validateIdentifier(String identifier, String propertyName) {
        if (!identifier.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(
                    propertyName + " must be a valid Doris identifier: " + identifier);
        }
    }
}
