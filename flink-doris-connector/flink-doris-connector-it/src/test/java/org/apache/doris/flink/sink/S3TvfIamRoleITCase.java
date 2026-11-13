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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.S3TvfOptions;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.container.instance.DorisCustomerContainer;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.apache.doris.flink.sink.writer.tvf.S3TvfRowDataSerializer;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Arrays;
import java.util.UUID;

/** Opt-in integration test for S3 TVF writes with an AWS IAM role. */
public class S3TvfIamRoleITCase {

    private static final Logger LOG = LoggerFactory.getLogger(S3TvfIamRoleITCase.class);
    private static final String DATABASE = "test_s3_tvf_iam_role";
    private static DorisCustomerContainer doris;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .build());

    @BeforeClass
    public static void useExternalEnvironment() {
        Assume.assumeTrue(
                "IAM role ITCase requires -Ds3_tvf_iam_role_it=true",
                Boolean.getBoolean("s3_tvf_iam_role_it"));
        Assume.assumeTrue(
                "IAM role ITCase requires -Dcustomer_env=true", Boolean.getBoolean("customer_env"));
        requiredProperty("s3_endpoint");
        requiredProperty("s3_region");
        requiredProperty("s3_bucket");
        requiredProperty("s3_role_arn");

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
    public void testWritesThroughIamRole() throws Exception {
        String table = "iam_role_" + UUID.randomUUID().toString().replace("-", "");
        try {
            createTable(table);
            runTvfSink(table);
            assertRows(table);
        } finally {
            dropTable(table);
        }
    }

    private void runTvfSink(String table) throws Exception {
        String[] fieldNames = {"id", "name"};
        DataType[] dataTypes = {DataTypes.INT(), DataTypes.STRING()};
        LogicalType[] logicalTypes =
                Arrays.stream(dataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.ofFields(logicalTypes, fieldNames);

        S3TvfOptions s3Options =
                S3TvfOptions.builder()
                        .setEndpoint(requiredProperty("s3_endpoint"))
                        .setRegion(requiredProperty("s3_region"))
                        .setBucket(requiredProperty("s3_bucket"))
                        .setPrefix(System.getProperty("s3_prefix", "doris-flink-connector-it"))
                        .setRoleArn(requiredProperty("s3_role_arn"))
                        .setExternalId(optionalProperty("s3_external_id"))
                        .setPathStyleAccess(Boolean.getBoolean("s3_path_style_access"))
                        .build();
        DorisSink<RowData> sink =
                DorisSink.<RowData>builder()
                        .setDorisOptions(
                                DorisOptions.builder()
                                        .setFenodes(doris.getFenodes())
                                        .setJdbcUrl(doris.getJdbcUrl())
                                        .setTableIdentifier(DATABASE + "." + table)
                                        .setUsername(doris.getUsername())
                                        .setPassword(doris.getPassword())
                                        .build())
                        .setDorisReadOptions(DorisReadOptions.builder().build())
                        .setDorisExecutionOptions(
                                DorisExecutionOptions.builder()
                                        .setWriteMode(WriteMode.TVF)
                                        .setLabelPrefix("iam_role_" + UUID.randomUUID())
                                        .setBufferFlushMaxBytes(1024)
                                        .setS3TvfOptions(s3Options)
                                        .build())
                        .setSerializer(
                                new S3TvfRowDataSerializer(
                                        fieldNames, dataTypes, Arrays.asList(fieldNames), false))
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        env.fromCollection(Arrays.asList(row(1, "doris"), row(2, "flink")), typeInfo)
                .sinkTo(sink)
                .setParallelism(1);
        env.execute("S3 TVF IAM role ITCase");
    }

    private static void createTable(String table) {
        executeSql(
                "CREATE DATABASE IF NOT EXISTS `" + DATABASE + "`",
                "CREATE TABLE `"
                        + DATABASE
                        + "`.`"
                        + table
                        + "` (`id` INT, `name` VARCHAR(64)) "
                        + "DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 "
                        + "PROPERTIES (\"replication_num\" = \"1\")");
    }

    private static void assertRows(String table) {
        ContainerUtils.checkResult(
                doris.getQueryConnection(),
                LOG,
                Arrays.asList("1,doris", "2,flink"),
                "SELECT id,name FROM `" + DATABASE + "`.`" + table + "` ORDER BY id",
                2,
                true);
    }

    private static void dropTable(String table) {
        executeSql("DROP TABLE IF EXISTS `" + DATABASE + "`.`" + table + "`");
    }

    private static void executeSql(String... statements) {
        try (Connection connection = doris.getQueryConnection()) {
            ContainerUtils.executeSQLStatement(connection, LOG, statements);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static GenericRowData row(int id, String name) {
        return GenericRowData.of(id, StringData.fromString(name));
    }

    private static String requiredProperty(String name) {
        String value = optionalProperty(name);
        if (value == null) {
            throw new IllegalArgumentException("Missing required system property: " + name);
        }
        return value;
    }

    private static String optionalProperty(String name) {
        String value = System.getProperty(name);
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
