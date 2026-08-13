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

package org.apache.doris.flink.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.RowKind;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.S3TvfOptions;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.apache.doris.flink.sink.writer.tvf.S3TvfRowDataSerializer;
import org.apache.doris.flink.utils.MockSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.flink.api.common.JobStatus.RUNNING;

/** End-to-end coverage for staging files in S3 and loading them through the Doris S3 TVF. */
public class S3TvfSinkITCase extends AbstractITCaseService {

    private static final Logger LOG = LoggerFactory.getLogger(S3TvfSinkITCase.class);
    private static final String DATABASE = "test_s3_tvf_sink";
    private static final String BUCKET = "doris-tvf-it";
    private static final String REGION = "us-east-1";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final int MINIO_PORT = 9000;
    private static final String MINIO_IMAGE = "minio/minio:RELEASE.2024-10-13T13-34-11Z";

    private static GenericContainer<?> minio;
    private static S3Client s3Client;
    private static String s3Endpoint;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @BeforeClass
    public static void startObjectStorage() throws Exception {
        minio =
                new GenericContainer<>(DockerImageName.parse(MINIO_IMAGE))
                        .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                        .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                        .withCommand("server", "/data")
                        .withExposedPorts(MINIO_PORT)
                        .waitingFor(Wait.forHttp("/minio/health/live").forPort(MINIO_PORT));
        minio.start();

        String dockerHost = DockerClientFactory.instance().dockerHostIpAddress();
        String endpointHost = resolveEndpointHost(dockerHost);
        s3Endpoint = "http://" + endpointHost + ":" + minio.getMappedPort(MINIO_PORT);
        s3Client = createS3Client(s3Endpoint);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }

    @AfterClass
    public static void stopObjectStorage() {
        if (s3Client != null) {
            s3Client.close();
        }
        if (minio != null) {
            minio.stop();
        }
    }

    @Test
    public void testStagesMultipleJsonFilesAndLoadsDuplicateTable() throws Exception {
        String table = uniqueName("duplicate_multi_file");
        String objectPrefix = uniqueName("objects");
        String labelPrefix = uniqueName("label");
        createDuplicateTable(table, "`id` INT, `name` VARCHAR(128), `note` VARCHAR(128) NULL");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(
                String.format(
                        "CREATE TABLE tvf_sink (id INT, name STRING, note STRING) WITH ("
                                + "'connector' = 'doris',"
                                + "'fenodes' = '%s',"
                                + "'jdbc-url' = '%s',"
                                + "'table.identifier' = '%s.%s',"
                                + "'username' = '%s',"
                                + "'password' = '%s',"
                                + "'sink.write-mode' = 'TVF',"
                                + "'sink.parallelism' = '1',"
                                + "'sink.buffer-flush.max-bytes' = '64b',"
                                + "'sink.label-prefix' = '%s',"
                                + "'sink.s3.endpoint' = '%s',"
                                + "'sink.s3.region' = '%s',"
                                + "'sink.s3.bucket' = '%s',"
                                + "'sink.s3.prefix' = '%s',"
                                + "'sink.s3.access-key' = '%s',"
                                + "'sink.s3.secret-key' = '%s',"
                                + "'sink.s3.path-style-access' = 'true')",
                        getFenodes(),
                        getDorisQueryUrl(),
                        DATABASE,
                        table,
                        getDorisUsername(),
                        getDorisPassword(),
                        labelPrefix,
                        s3Endpoint,
                        REGION,
                        BUCKET,
                        objectPrefix,
                        ACCESS_KEY,
                        SECRET_KEY));
        TableResult result =
                tableEnv.executeSql(
                        "INSERT INTO tvf_sink VALUES "
                                + "(1, 'doris', '中文'), "
                                + "(2, 'flink', 'quote-''and-\"'), "
                                + "(3, 'null-value', CAST(NULL AS STRING))");
        waitForJobStatus(
                result.getJobClient().get(),
                Collections.singletonList(FINISHED),
                Deadline.fromNow(Duration.ofSeconds(120)));

        assertResult(
                table,
                "id,name,note",
                Arrays.asList("1,doris,中文", "2,flink,quote-'and-\"", "3,null-value,null"),
                3);
        int objectCount =
                s3Client.listObjectsV2(
                                ListObjectsV2Request.builder()
                                        .bucket(BUCKET)
                                        .prefix(objectPrefix + "/" + labelPrefix + "_" + table)
                                        .build())
                        .keyCount();
        Assert.assertTrue("Expected the buffer limit to create multiple objects", objectCount > 1);
    }

    @Test
    public void testLoadsFromParallelSubtasks() throws Exception {
        String table = uniqueName("parallel");
        createDuplicateTable(table, "`id` INT, `task_value` VARCHAR(128)");
        List<RowData> rows = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            rows.add(row(RowKind.INSERT, i, "value-" + i));
            expected.add(i + ",value-" + i);
        }

        String[] fieldNames = {"id", "task_value"};
        DataType[] dataTypes = {DataTypes.INT(), DataTypes.STRING()};
        LogicalType[] logicalTypes =
                Arrays.stream(dataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.ofFields(logicalTypes, fieldNames);
        DorisSink<RowData> sink =
                DorisSink.<RowData>builder()
                        .setDorisOptions(dorisOptions(table))
                        .setDorisReadOptions(DorisReadOptions.builder().build())
                        .setDorisExecutionOptions(
                                DorisExecutionOptions.builder()
                                        .setWriteMode(WriteMode.TVF)
                                        .setLabelPrefix(uniqueName("label"))
                                        .setBufferFlushMaxBytes(1024)
                                        .setS3TvfOptions(s3Options(uniqueName("parallel_objects")))
                                        .build())
                        .setSerializer(
                                new S3TvfRowDataSerializer(
                                        fieldNames,
                                        dataTypes,
                                        Arrays.asList("id", "task_value"),
                                        false))
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);
        env.fromCollection(rows, typeInfo)
                .rebalance()
                .map((MapFunction<RowData, RowData>) value -> value)
                .returns(typeInfo)
                .setParallelism(2)
                .sinkTo(sink)
                .setParallelism(2);
        env.execute();

        assertResult(table, "id,task_value", expected, 2);
    }

    @Test
    public void testAppliesUniqueKeyUpdateAndDelete() throws Exception {
        String table = uniqueName("unique_changes");
        createUniqueTable(table, "`id` INT, `name` VARCHAR(128), `score` INT");

        String[] fieldNames = {"id", "name", "score"};
        DataType[] dataTypes = {DataTypes.INT(), DataTypes.STRING(), DataTypes.INT()};
        LogicalType[] logicalTypes =
                Arrays.stream(dataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.ofFields(logicalTypes, fieldNames);
        List<String> columns = Arrays.asList("id", "name", "score");

        DorisSink<RowData> insertSink =
                DorisSink.<RowData>builder()
                        .setDorisOptions(dorisOptions(table))
                        .setDorisReadOptions(DorisReadOptions.builder().build())
                        .setDorisExecutionOptions(
                                DorisExecutionOptions.builder()
                                        .setWriteMode(WriteMode.TVF)
                                        .setLabelPrefix(uniqueName("insert_label"))
                                        .setBufferFlushMaxBytes(1024)
                                        .setS3TvfOptions(s3Options(uniqueName("insert_objects")))
                                        .build())
                        .setSerializer(
                                new S3TvfRowDataSerializer(fieldNames, dataTypes, columns, true))
                        .build();
        StreamExecutionEnvironment insertEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        insertEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        insertEnv.setParallelism(1);
        insertEnv
                .fromCollection(
                        Arrays.asList(
                                row(RowKind.INSERT, 1, "before", 10),
                                row(RowKind.INSERT, 2, "delete-me", 20)),
                        typeInfo)
                .sinkTo(insertSink)
                .setParallelism(1);
        insertEnv.execute();

        DorisSink<RowData> changeSink =
                DorisSink.<RowData>builder()
                        .setDorisOptions(dorisOptions(table))
                        .setDorisReadOptions(DorisReadOptions.builder().build())
                        .setDorisExecutionOptions(
                                DorisExecutionOptions.builder()
                                        .setWriteMode(WriteMode.TVF)
                                        .setLabelPrefix(uniqueName("change_label"))
                                        .setBufferFlushMaxBytes(1024)
                                        .setS3TvfOptions(s3Options(uniqueName("change_objects")))
                                        .build())
                        .setSerializer(
                                new S3TvfRowDataSerializer(fieldNames, dataTypes, columns, true))
                        .build();
        StreamExecutionEnvironment changeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        changeEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        changeEnv.setParallelism(1);
        changeEnv
                .fromCollection(
                        Arrays.asList(
                                row(RowKind.UPDATE_AFTER, 1, "after", 11),
                                row(RowKind.DELETE, 2, "delete-me", 20)),
                        typeInfo)
                .sinkTo(changeSink)
                .setParallelism(1);
        changeEnv.execute();

        assertResult(table, "id,name,score", Collections.singletonList("1,after,11"), 3);
    }

    @Test
    public void testUpdatesOnlyConfiguredColumns() throws Exception {
        String table = uniqueName("partial_update");
        String objectPrefix = uniqueName("partial_objects");
        createUniqueTable(table, "`id` INT, `name` VARCHAR(128), `score` INT");
        executeSql(String.format("INSERT INTO %s.%s VALUES (1, 'before', 10)", DATABASE, table));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(
                String.format(
                        "CREATE TABLE tvf_sink (id INT, name STRING) WITH ("
                                + "'connector' = 'doris',"
                                + "'fenodes' = '%s',"
                                + "'jdbc-url' = '%s',"
                                + "'table.identifier' = '%s.%s',"
                                + "'username' = '%s',"
                                + "'password' = '%s',"
                                + "'sink.write-mode' = 'TVF',"
                                + "'sink.parallelism' = '1',"
                                + "'sink.buffer-flush.max-bytes' = '1024b',"
                                + "'sink.label-prefix' = '%s',"
                                + "'sink.s3.endpoint' = '%s',"
                                + "'sink.s3.region' = '%s',"
                                + "'sink.s3.bucket' = '%s',"
                                + "'sink.s3.prefix' = '%s',"
                                + "'sink.s3.access-key' = '%s',"
                                + "'sink.s3.secret-key' = '%s',"
                                + "'sink.s3.path-style-access' = 'true',"
                                + "'sink.properties.columns' = 'id,name',"
                                + "'sink.properties.partial_columns' = 'true')",
                        getFenodes(),
                        getDorisQueryUrl(),
                        DATABASE,
                        table,
                        getDorisUsername(),
                        getDorisPassword(),
                        uniqueName("partial_label"),
                        s3Endpoint,
                        REGION,
                        BUCKET,
                        objectPrefix,
                        ACCESS_KEY,
                        SECRET_KEY));
        TableResult result = tableEnv.executeSql("INSERT INTO tvf_sink VALUES (1, 'after')");
        waitForJobStatus(
                result.getJobClient().get(),
                Collections.singletonList(FINISHED),
                Deadline.fromNow(Duration.ofSeconds(120)));

        assertResult(table, "id,name,score", Collections.singletonList("1,after,10"), 3);
    }

    @Test
    public void testRestoresFromCheckpointAfterJobManagerFailover() throws Exception {
        String table = uniqueName("checkpoint_failover");
        createDuplicateTable(table, "`id` INT, `task_id` INT");
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(0));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(500);

        String[] fieldNames = {"id", "task_id"};
        DataType[] dataTypes = {DataTypes.INT(), DataTypes.INT()};
        LogicalType[] logicalTypes =
                Arrays.stream(dataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.ofFields(logicalTypes, fieldNames);
        DataStream<RowData> rows =
                env.addSource(new MockSource(5))
                        .map(
                                (MapFunction<String, RowData>)
                                        value -> {
                                            String[] values = value.split(",", -1);
                                            return row(
                                                    RowKind.INSERT,
                                                    Integer.parseInt(values[0]),
                                                    Integer.parseInt(values[1]));
                                        })
                        .returns(typeInfo);
        DorisSink<RowData> sink =
                DorisSink.<RowData>builder()
                        .setDorisOptions(dorisOptions(table))
                        .setDorisReadOptions(DorisReadOptions.builder().build())
                        .setDorisExecutionOptions(
                                DorisExecutionOptions.builder()
                                        .setWriteMode(WriteMode.TVF)
                                        .setLabelPrefix(uniqueName("failover_label"))
                                        .setBufferFlushMaxBytes(1024)
                                        .setS3TvfOptions(s3Options(uniqueName("failover_objects")))
                                        .build())
                        .setSerializer(
                                new S3TvfRowDataSerializer(
                                        fieldNames,
                                        dataTypes,
                                        Arrays.asList("id", "task_id"),
                                        false))
                        .build();
        rows.sinkTo(sink).setParallelism(DEFAULT_PARALLELISM);

        JobClient jobClient = env.executeAsync();
        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(10)));
        waitUntilCondition(
                () -> queryCount(table) >= DEFAULT_PARALLELISM,
                Deadline.fromNow(Duration.ofSeconds(30)),
                200,
                "No TVF checkpoint was committed before failover.");

        JobID jobId = jobClient.getJobID();
        triggerFailover(
                FailoverType.JM, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(100));
        waitForJobStatus(
                jobClient,
                Collections.singletonList(FINISHED),
                Deadline.fromNow(Duration.ofSeconds(120)));

        List<String> expected = new ArrayList<>();
        for (int id = 1; id <= 5; id++) {
            for (int subtask = 0; subtask < DEFAULT_PARALLELISM; subtask++) {
                expected.add(id + "," + subtask);
            }
        }
        assertResult(table, "id,task_id", expected, 2);
    }

    private DorisOptions dorisOptions(String table) {
        return DorisOptions.builder()
                .setFenodes(getFenodes())
                .setJdbcUrl(getDorisQueryUrl())
                .setTableIdentifier(DATABASE + "." + table)
                .setUsername(getDorisUsername())
                .setPassword(getDorisPassword())
                .build();
    }

    private S3TvfOptions s3Options(String objectPrefix) {
        return S3TvfOptions.builder()
                .setEndpoint(s3Endpoint)
                .setRegion(REGION)
                .setBucket(BUCKET)
                .setPrefix(objectPrefix)
                .setAccessKey(ACCESS_KEY)
                .setSecretKey(SECRET_KEY)
                .setPathStyleAccess(true)
                .build();
    }

    private void createDuplicateTable(String table, String columns) {
        createTable(table, columns, "DUPLICATE KEY(`id`)", "");
    }

    private void createUniqueTable(String table, String columns) {
        createTable(
                table,
                columns,
                "UNIQUE KEY(`id`)",
                ", \"enable_unique_key_merge_on_write\" = \"true\"");
    }

    private void createTable(
            String table, String columns, String keyDefinition, String additionalProperties) {
        executeSql(
                String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
                String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
                String.format(
                        "CREATE TABLE %s.%s (%s) %s "
                                + "DISTRIBUTED BY HASH(`id`) BUCKETS 1 "
                                + "PROPERTIES (\"replication_num\" = \"1\"%s)",
                        DATABASE, table, columns, keyDefinition, additionalProperties));
    }

    private void executeSql(String... sql) {
        ContainerUtils.executeSQLStatement(getDorisQueryConnection(), LOG, sql);
    }

    private void assertResult(
            String table, String columns, List<String> expected, int columnCount) {
        String query =
                String.format(
                        "SELECT %s FROM %s.%s ORDER BY %s", columns, DATABASE, table, columns);
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, expected, query, columnCount, true);
    }

    private int queryCount(String table) {
        return Integer.parseInt(
                ContainerUtils.executeSQLStatement(
                                getDorisQueryConnection(),
                                LOG,
                                String.format("SELECT COUNT(*) FROM %s.%s", DATABASE, table),
                                1)
                        .get(0));
    }

    private static GenericRowData row(RowKind kind, Object... values) {
        GenericRowData row = new GenericRowData(kind, values.length);
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            row.setField(
                    i, value instanceof String ? StringData.fromString((String) value) : value);
        }
        return row;
    }

    private static S3Client createS3Client(String endpoint) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(REGION))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .serviceConfiguration(
                        S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }

    private static String resolveEndpointHost(String dockerHost) throws Exception {
        InetAddress dockerAddress = InetAddress.getByName(dockerHost);
        if (!dockerAddress.isLoopbackAddress() && !dockerAddress.isAnyLocalAddress()) {
            return dockerHost;
        }
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                continue;
            }
            Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (address instanceof Inet4Address
                        && !address.isLoopbackAddress()
                        && !address.isLinkLocalAddress()) {
                    return address.getHostAddress();
                }
            }
        }
        return dockerHost;
    }

    private static String uniqueName(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }
}
