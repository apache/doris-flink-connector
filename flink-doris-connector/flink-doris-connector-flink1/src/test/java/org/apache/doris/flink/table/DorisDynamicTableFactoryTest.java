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

package org.apache.doris.flink.table;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.cfg.S3TvfOptions;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.apache.doris.flink.source.DorisBinlogIncrementType;
import org.apache.doris.flink.source.DorisSourceScanMode;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_BATCH_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_EXEC_MEM_LIMIT_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_THRIFT_MAX_MESSAGE_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.FLIGHT_SQL_PORT_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.USE_FLIGHT_SQL_DEFAULT;
import static org.apache.doris.flink.utils.FactoryMocks.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

public class DorisDynamicTableFactoryTest {

    @Test
    public void testScanParallelism() {
        for (Integer parallelism : new Integer[] {null, 4}) {
            for (String mode : Arrays.asList("snapshot", "initial", "latest", "from-timestamp")) {
                Map<String, String> properties = getAllOptions();
                properties.put("source.scan.mode", mode);
                properties.put("source.use-flight-sql", "true");
                if ("from-timestamp".equals(mode)) {
                    properties.put("source.scan.timestamp", "2026-07-20 10:00:00");
                }
                if (parallelism != null) {
                    properties.put("scan.parallelism", parallelism.toString());
                }
                DorisDynamicTableSource source =
                        (DorisDynamicTableSource)
                                FactoryMocks.createTableSource(SCHEMA, properties);
                DorisDynamicTableSource copy = (DorisDynamicTableSource) source.copy();
                ScanTableSource.ScanRuntimeProvider provider =
                        copy.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
                if (parallelism == null) {
                    assertTrue(provider instanceof SourceProvider);
                } else {
                    assertEquals(
                            Optional.of(parallelism),
                            ((ParallelismProvider) provider).getParallelism());
                    StreamExecutionEnvironment env =
                            StreamExecutionEnvironment.getExecutionEnvironment();
                    env.setParallelism(2);
                    assertEquals(
                            4,
                            ((DataStreamScanProvider) provider)
                                    .produceDataStream(ignored -> Optional.empty(), env)
                                    .getParallelism());
                    assertEquals("snapshot".equals(mode), provider.isBounded());
                }
            }
        }
    }

    @Test
    public void testOldApiScanParallelism() {
        try (MockedStatic<RestService> restService = mockStatic(RestService.class)) {
            restService
                    .when(() -> RestService.findPartitions(any(), any(), any()))
                    .thenReturn(Collections.emptyList());
            Map<String, String> properties = getAllOptions();
            properties.put("source.use-old-api", "true");
            properties.put("scan.parallelism", "3");
            DorisDynamicTableSource source =
                    (DorisDynamicTableSource) FactoryMocks.createTableSource(SCHEMA, properties);
            DataStreamScanProvider provider =
                    (DataStreamScanProvider)
                            source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
            assertEquals(Optional.of(3), ((ParallelismProvider) provider).getParallelism());
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);
            assertEquals(
                    3,
                    provider.produceDataStream(ignored -> Optional.empty(), env).getParallelism());
        }
    }

    @Test
    public void testScanParallelismInSqlPlan() {
        try (MockedStatic<RestService> restService = mockStatic(RestService.class)) {
            restService
                    .when(() -> RestService.findPartitions(any(), any(), any()))
                    .thenReturn(Collections.emptyList());
            for (boolean useOldApi : new boolean[] {false, true}) {
                for (Integer parallelism : new Integer[] {null, 3}) {
                    StreamExecutionEnvironment env =
                            StreamExecutionEnvironment.getExecutionEnvironment();
                    env.setParallelism(2);
                    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
                    TableDescriptor.Builder descriptor =
                            TableDescriptor.forConnector(DorisConfigOptions.IDENTIFIER)
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("a", DataTypes.STRING())
                                                    .column("b", DataTypes.INT())
                                                    .column("c", DataTypes.BOOLEAN())
                                                    .build())
                                    .option("fenodes", "127.0.0.1:8030")
                                    .option("table.identifier", "db.tbl")
                                    .option("source.use-old-api", Boolean.toString(useOldApi));
                    if (parallelism != null) {
                        descriptor.option("scan.parallelism", parallelism.toString());
                    }
                    tableEnv.createTemporaryTable("doris_source", descriptor.build());
                    tableEnv.toDataStream(tableEnv.sqlQuery("SELECT c, UPPER(a) FROM doris_source"))
                            .sinkTo(new DiscardingSink<>());

                    StreamGraph graph = env.getStreamGraph();
                    assertEquals(1, graph.getSourceIDs().size());
                    assertEquals(
                            parallelism == null ? 2 : parallelism.intValue(),
                            graph.getStreamNode(graph.getSourceIDs().iterator().next())
                                    .getParallelism());
                    // Only Flink 1.19+ isolates configured source parallelism from downstream.
                    int calcParallelism =
                            parallelism != null
                                            && !ParallelismProvider.class.isAssignableFrom(
                                                    DataStreamScanProvider.class)
                                    ? parallelism
                                    : 2;
                    assertThat(graph.getStreamNodes())
                            .filteredOn(node -> node.getOperatorName().startsWith("Calc"))
                            .singleElement()
                            .satisfies(
                                    node -> assertEquals(calcParallelism, node.getParallelism()));
                }
            }
        }
    }

    @Test
    public void testScanParallelismPreservesProjectedType() throws Exception {
        DataType projectedType =
                DataTypes.ROW(
                        DataTypes.FIELD("c", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("a", DataTypes.STRING()));
        try (MockedStatic<RestService> restService = mockStatic(RestService.class)) {
            restService
                    .when(() -> RestService.findPartitions(any(), any(), any()))
                    .thenReturn(Collections.emptyList());
            for (boolean useOldApi : new boolean[] {false, true}) {
                Map<String, String> properties = getAllOptions();
                properties.put("source.use-old-api", Boolean.toString(useOldApi));
                properties.put("scan.parallelism", "3");
                DorisDynamicTableSource source =
                        (DorisDynamicTableSource)
                                FactoryMocks.createTableSource(SCHEMA, properties);
                source.applyProjection(new int[][] {{2}, {0}}, projectedType);
                DorisDynamicTableSource copy = (DorisDynamicTableSource) source.copy();
                DataStreamScanProvider provider =
                        (DataStreamScanProvider)
                                copy.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
                StreamExecutionEnvironment env =
                        StreamExecutionEnvironment.getExecutionEnvironment();
                DataStream<RowData> stream =
                        provider.produceDataStream(ignored -> Optional.empty(), env);
                assertEquals(InternalTypeInfo.of(projectedType.getLogicalType()), stream.getType());

                TypeSerializer<RowData> serializer =
                        stream.getType().createSerializer(env.getConfig());
                RowData row =
                        GenericRowData.ofKind(
                                RowKind.UPDATE_AFTER, null, StringData.fromString("projected"));
                DataOutputSerializer output = new DataOutputSerializer(64);
                serializer.serialize(row, output);
                RowData restored =
                        serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
                assertEquals(2, restored.getArity());
                assertEquals(RowKind.UPDATE_AFTER, restored.getRowKind());
                assertTrue(restored.isNullAt(0));
                assertEquals("projected", restored.getString(1).toString());
            }
        }
    }

    @Test
    public void testRejectNonPositiveScanParallelism() {
        for (String parallelism : Arrays.asList("0", "-1")) {
            Map<String, String> properties = getAllOptions();
            properties.put("scan.parallelism", parallelism);
            assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, properties))
                    .hasRootCauseInstanceOf(ValidationException.class)
                    .hasRootCauseMessage("scan.parallelism must be greater than 0");
        }
    }

    @Test
    public void testIncrementalSourceRejectsOldApi() {
        Map<String, String> properties = getAllOptions();
        properties.put("source.scan.mode", "latest");
        properties.put("source.use-old-api", "true");

        try {
            FactoryMocks.createTableSource(SCHEMA, properties);
            org.junit.Assert.fail("Expected incremental mode to reject source.use-old-api=true");
        } catch (ValidationException e) {
            Throwable rootCause = e;
            while (rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            assertEquals(
                    "source.use-old-api=true only supports source.scan.mode=snapshot",
                    rootCause.getMessage());
        }
    }

    @Test
    public void testDorisSourceProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("doris.request.query.timeout", "21600s");
        properties.put("doris.request.tablet.size", "1");
        properties.put("doris.batch.size", "4064");
        properties.put("doris.exec.mem.limit", "8192mb");
        properties.put("doris.deserialize.arrow.async", "false");
        properties.put("doris.deserialize.queue.size", "64");
        properties.put("doris.thrift.max.message.size", Integer.MAX_VALUE + "");

        properties.put("lookup.cache.max-rows", "100");
        properties.put("lookup.cache.ttl", "20s");
        properties.put("lookup.max-retries", "1");
        properties.put("lookup.jdbc.async", "true");
        properties.put("lookup.jdbc.read.batch.size", "16");
        properties.put("lookup.jdbc.read.batch.queue-size", "16");
        properties.put("lookup.jdbc.read.thread-size", "1");
        properties.put("source.use-flight-sql", "true");
        properties.put("source.flight-sql-port", "-1");
        properties.put("source.scan.mode", "from-timestamp");
        properties.put("source.scan.timestamp", "2026-07-20 10:00:00");
        properties.put("source.binlog.increment-type", "min_delta");
        properties.put("source.binlog.poll-interval", "3s");
        properties.put("source.binlog.visible-wait-timeout", "7m");
        DynamicTableSource actual = FactoryMocks.createTableSource(SCHEMA, properties);
        DorisOptions options =
                DorisOptions.builder()
                        .setTableIdentifier("db.tbl")
                        .setFenodes("127.0.0.1:8030")
                        .setBenodes("127.0.0.1:8040")
                        .setAutoRedirect(true)
                        .setUsername("root")
                        .setPassword("")
                        .setJdbcUrl("jdbc:mysql://127.0.0.1:9030")
                        .setTlsOptions(tlsOptions())
                        .build();
        DorisLookupOptions lookupOptions =
                DorisLookupOptions.builder()
                        .setCacheExpireMs(20000)
                        .setCacheMaxSize(100)
                        .setAsync(true)
                        .setJdbcReadBatchQueueSize(16)
                        .setJdbcReadBatchSize(16)
                        .setJdbcReadThreadSize(1)
                        .setMaxRetryTimes(1)
                        .build();

        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();
        readOptionBuilder
                .setDeserializeArrowAsync(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                .setDeserializeQueueSize(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                .setExecMemLimit(DORIS_EXEC_MEM_LIMIT_DEFAULT)
                .setRequestQueryTimeoutS(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
                .setRequestBatchSize(DORIS_BATCH_SIZE_DEFAULT)
                .setRequestConnectTimeoutMs(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                .setRequestReadTimeoutMs(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                .setRequestRetries(DORIS_REQUEST_RETRIES_DEFAULT)
                .setRequestTabletSize(DORIS_TABLET_SIZE_DEFAULT)
                .setUseFlightSql(USE_FLIGHT_SQL_DEFAULT)
                .setFlightSqlPort(FLIGHT_SQL_PORT_DEFAULT)
                .setThriftMaxMessageSize(DORIS_THRIFT_MAX_MESSAGE_SIZE_DEFAULT)
                .setScanMode(DorisSourceScanMode.FROM_TIMESTAMP)
                .setScanTimestamp("2026-07-20 10:00:00")
                .setBinlogIncrementType(DorisBinlogIncrementType.MIN_DELTA)
                .setBinlogPollIntervalMs(3_000L)
                .setBinlogVisibleWaitTimeoutMs(7 * 60_000L);
        DorisDynamicTableSource expected =
                new DorisDynamicTableSource(
                        options,
                        readOptionBuilder.build(),
                        lookupOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        SCHEMA.toPhysicalRowDataType(),
                        null);

        assertEquals(actual, expected);
    }

    @Test
    public void testDorisSinkProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("doris.request.query.timeout", "21600s");
        properties.put("doris.request.tablet.size", "1");
        properties.put("doris.batch.size", "4064");
        properties.put("doris.exec.mem.limit", "8192mb");
        properties.put("doris.deserialize.arrow.async", "false");
        properties.put("doris.deserialize.queue.size", "64");

        properties.put("sink.label-prefix", "abc");
        properties.put("sink.properties.format", "json");
        properties.put("sink.properties.read_json_by_line", "true");
        properties.put("sink.enable-delete", "true");
        properties.put("sink.enable-2pc", "true");
        properties.put("sink.buffer-size", "1MB");
        properties.put("sink.buffer-count", "3");
        properties.put("sink.max-retries", "1");
        properties.put("sink.check-interval", "10s");
        properties.put("sink.use-cache", "true");
        properties.put("sink.enable.batch-mode", "true");
        properties.put("sink.flush.queue-size", "2");
        properties.put("sink.buffer-flush.max-rows", "10000");
        properties.put("sink.buffer-flush.max-bytes", "10MB");
        properties.put("sink.buffer-flush.interval", "10s");
        properties.put("sink.ignore.update-before", "true");
        properties.put("sink.ignore.commit-error", "false");
        properties.put("sink.parallelism", "1");

        DynamicTableSink actual = FactoryMocks.createTableSink(SCHEMA, properties);
        DorisOptions options =
                DorisOptions.builder()
                        .setTableIdentifier("db.tbl")
                        .setFenodes("127.0.0.1:8030")
                        .setBenodes("127.0.0.1:8040")
                        .setAutoRedirect(true)
                        .setUsername("root")
                        .setPassword("")
                        .setJdbcUrl("jdbc:mysql://127.0.0.1:9030")
                        .setTlsOptions(tlsOptions())
                        .build();

        Properties prop = new Properties();
        prop.put("format", "json");
        prop.put("read_json_by_line", "true");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setLabelPrefix("abc")
                        .setBufferCount(3)
                        .setBufferSize(1024 * 1024)
                        .setStreamLoadProp(prop)
                        .setMaxRetries(1)
                        .setCheckInterval(10000)
                        .setBatchMode(true)
                        .enable2PC()
                        .setBufferFlushIntervalMs(10000)
                        .setBufferFlushMaxBytes(10 * 1024 * 1024)
                        .setBufferFlushMaxRows(10000)
                        .setFlushQueueSize(2)
                        .setUseCache(true)
                        .setIgnoreCommitError(false)
                        .build();

        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();
        readOptionBuilder
                .setDeserializeArrowAsync(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                .setDeserializeQueueSize(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                .setExecMemLimit(DORIS_EXEC_MEM_LIMIT_DEFAULT)
                .setRequestQueryTimeoutS(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
                .setRequestBatchSize(DORIS_BATCH_SIZE_DEFAULT)
                .setRequestConnectTimeoutMs(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                .setRequestReadTimeoutMs(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                .setRequestRetries(DORIS_REQUEST_RETRIES_DEFAULT)
                .setRequestTabletSize(DORIS_TABLET_SIZE_DEFAULT)
                .setUseFlightSql(USE_FLIGHT_SQL_DEFAULT)
                .setFlightSqlPort(FLIGHT_SQL_PORT_DEFAULT);
        DorisDynamicTableSink expected =
                new DorisDynamicTableSink(
                        options,
                        readOptionBuilder.build(),
                        executionOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        1);

        assertEquals(expected, expected);
        assertNotEquals(expected, null);
        assertEquals(actual, expected);

        options.setTableIdentifier("xxxxx");
        DorisDynamicTableSink expected2 =
                new DorisDynamicTableSink(
                        options,
                        readOptionBuilder.build(),
                        executionOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        1);
        assertNotEquals(actual, expected2);
        options.setTableIdentifier("db.tbl");

        readOptionBuilder.setExecMemLimit(1L);
        DorisDynamicTableSink expected3 =
                new DorisDynamicTableSink(
                        options,
                        readOptionBuilder.build(),
                        executionOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        1);
        assertNotEquals(actual, expected3);
        readOptionBuilder.setExecMemLimit(DORIS_EXEC_MEM_LIMIT_DEFAULT);

        executionOptions.setEnable2PC(false);
        DorisDynamicTableSink expected4 =
                new DorisDynamicTableSink(
                        options,
                        readOptionBuilder.build(),
                        executionOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        1);
        assertNotEquals(actual, expected4);
        executionOptions.setEnable2PC(true);

        DynamicTableSink actual2 = FactoryMocks.createTableSink(SCHEMA, new HashMap<>());
        assertNotEquals(actual2, expected);
    }

    @Test
    public void testTvfSinkProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("sink.write-mode", "TVF");
        properties.put("sink.label-prefix", "flink");
        properties.put("sink.buffer-flush.max-bytes", "10MB");
        properties.put("sink.s3.endpoint", "https://s3.example.com");
        properties.put("sink.s3.region", "us-east-1");
        properties.put("sink.s3.bucket", "bucket");
        properties.put("sink.s3.prefix", "prefix");
        properties.put("sink.s3.role-arn", "arn:aws:iam::123456789012:role/doris");
        properties.put("sink.s3.external-id", "external-id");
        properties.put("sink.s3.path-style-access", "true");
        properties.put("sink.properties.columns", "a,c");

        DorisDynamicTableSink actual =
                (DorisDynamicTableSink) FactoryMocks.createTableSink(SCHEMA, properties);

        DorisOptions options =
                DorisOptions.builder()
                        .setTableIdentifier("db.tbl")
                        .setFenodes("127.0.0.1:8030")
                        .setBenodes("127.0.0.1:8040")
                        .setAutoRedirect(true)
                        .setUsername("root")
                        .setPassword("")
                        .setJdbcUrl("jdbc:mysql://127.0.0.1:9030")
                        .setTlsOptions(tlsOptions())
                        .build();
        S3TvfOptions s3TvfOptions =
                S3TvfOptions.builder()
                        .setEndpoint("https://s3.example.com")
                        .setRegion("us-east-1")
                        .setBucket("bucket")
                        .setPrefix("prefix")
                        .setRoleArn("arn:aws:iam::123456789012:role/doris")
                        .setExternalId("external-id")
                        .setPathStyleAccess(true)
                        .build();
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setWriteMode(WriteMode.TVF)
                        .setLabelPrefix("flink")
                        .setBufferFlushMaxBytes(10 * 1024 * 1024)
                        .setStreamLoadProp(
                                new Properties() {
                                    {
                                        setProperty("columns", "a,c");
                                    }
                                })
                        .setS3TvfOptions(s3TvfOptions)
                        .build();

        DorisDynamicTableSink expected =
                new DorisDynamicTableSink(
                        options,
                        DorisReadOptions.builder().build(),
                        executionOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        null);
        assertEquals(expected, actual);

        properties.remove("sink.s3.role-arn");
        properties.remove("sink.s3.external-id");
        properties.put("sink.s3.access-key", "ak");
        properties.put("sink.s3.secret-key", "sk");
        actual = (DorisDynamicTableSink) FactoryMocks.createTableSink(SCHEMA, properties);

        s3TvfOptions =
                S3TvfOptions.builder()
                        .setEndpoint("https://s3.example.com")
                        .setRegion("us-east-1")
                        .setBucket("bucket")
                        .setPrefix("prefix")
                        .setAccessKey("ak")
                        .setSecretKey("sk")
                        .setPathStyleAccess(true)
                        .build();
        executionOptions =
                DorisExecutionOptions.builder()
                        .setWriteMode(WriteMode.TVF)
                        .setLabelPrefix("flink")
                        .setBufferFlushMaxBytes(10 * 1024 * 1024)
                        .setStreamLoadProp(
                                new Properties() {
                                    {
                                        setProperty("columns", "a,c");
                                    }
                                })
                        .setS3TvfOptions(s3TvfOptions)
                        .build();
        expected =
                new DorisDynamicTableSink(
                        options,
                        DorisReadOptions.builder().build(),
                        executionOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        null);
        assertEquals(expected, actual);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", DorisConfigOptions.IDENTIFIER);
        options.put("fenodes", "127.0.0.1:8030");
        options.put("benodes", "127.0.0.1:8040");
        options.put("jdbc-url", "jdbc:mysql://127.0.0.1:9030");
        options.put("table.identifier", "db.tbl");
        options.put("username", "root");
        options.put("password", "");
        options.put("auto-redirect", "true");
        options.put("doris.enable.tls", "true");
        options.put("doris.tls.ca-certificate-path", "/etc/doris/ca.pem");
        options.put("doris.tls.skip-hostname-verification", "true");
        options.put("doris.tls.excluded-protocols", "arrowflight");
        options.put("doris.request.retries", "3");
        options.put("doris.request.connect.timeout", "60s");
        options.put("doris.request.read.timeout", "60s");
        return options;
    }

    private DorisTlsOptions tlsOptions() {
        return DorisTlsOptions.builder()
                .setEnabled(true)
                .setCaCertificatePath("/etc/doris/ca.pem")
                .setSkipHostnameVerification(true)
                .setExcludedProtocols("arrowflight")
                .build();
    }
}
