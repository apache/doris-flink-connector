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

package org.apache.doris.flink.sink.writer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Field;
import org.apache.doris.flink.rest.models.Schema;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * test for JsonDebeziumSchemaSerializer.
 */
public class TestJsonDebeziumSchemaSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(TestJsonDebeziumSchemaSerializer.class);
    static DorisOptions dorisOptions;
    static JsonDebeziumSchemaSerializer serializer;
    static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    public static void setUp() {
        dorisOptions = DorisOptions.builder().setFenodes("127.0.0.1:8030")
                .setTableIdentifier("test.t1")
                .setUsername("root")
                .setPassword("").build();
        serializer = JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build();
    }

    @Test
    public void testSerializeInsert() throws IOException {
        //insert into t1 VALUES(1,"doris",'2022-01-01','2022-01-01 10:01:02','2022-01-01 10:01:03');
        byte[] serializedValue = serializer.serialize("{\"before\":null,\"after\":{\"id\":1,\"name\":\"doris\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663923840000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":11834,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1663923840146,\"transaction\":null}");
        Map<String, String> valueMap = objectMapper.readValue(new String(serializedValue, StandardCharsets.UTF_8), new TypeReference<Map<String, String>>(){});
        Assert.assertEquals("1", valueMap.get("id"));
        Assert.assertEquals("doris", valueMap.get("name"));
        Assert.assertEquals("2022-01-01", valueMap.get("dt"));
        Assert.assertEquals("2022-01-01 10:01:02", valueMap.get("dtime"));
        Assert.assertEquals("2022-01-01 10:01:03", valueMap.get("ts"));
        Assert.assertEquals("0", valueMap.get("__DORIS_DELETE_SIGN__"));
        Assert.assertEquals(6, valueMap.size());

    }

    @Test
    public void testSerializeUpdate() throws IOException {
        //update t1 set name='doris-update' WHERE id =1;
        byte[] serializedValue = serializer.serialize("{\"before\":{\"id\":1,\"name\":\"doris\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924082000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12154,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"u\",\"ts_ms\":1663924082186,\"transaction\":null}");
        Map<String, String> valueMap = objectMapper.readValue(new String(serializedValue, StandardCharsets.UTF_8), new TypeReference<Map<String, String>>(){});
        Assert.assertEquals("1", valueMap.get("id"));
        Assert.assertEquals("doris-update", valueMap.get("name"));
        Assert.assertEquals("2022-01-01", valueMap.get("dt"));
        Assert.assertEquals("2022-01-01 10:01:02", valueMap.get("dtime"));
        Assert.assertEquals("2022-01-01 10:01:03", valueMap.get("ts"));
        Assert.assertEquals("0", valueMap.get("__DORIS_DELETE_SIGN__"));
        Assert.assertEquals(6, valueMap.size());
    }

    @Test
    public void testSerializeUpdateBefore() throws IOException {
        serializer = JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions)
                .setExecutionOptions(DorisExecutionOptions.builderDefaults().setIgnoreUpdateBefore(false).build()).build();
        //update t1 set name='doris-update' WHERE id =1;
        byte[] serializedValue = serializer.serialize("{\"before\":{\"id\":1,\"name\":\"doris\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924082000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12154,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"u\",\"ts_ms\":1663924082186,\"transaction\":null}");
        String row = new String(serializedValue, StandardCharsets.UTF_8);
        String[] split = row.split("\n");
        Map<String, String> valueMap = objectMapper.readValue(split[1], new TypeReference<Map<String, String>>(){});
        Assert.assertEquals("1", valueMap.get("id"));
        Assert.assertEquals("doris-update", valueMap.get("name"));
        Assert.assertEquals("2022-01-01", valueMap.get("dt"));
        Assert.assertEquals("2022-01-01 10:01:02", valueMap.get("dtime"));
        Assert.assertEquals("2022-01-01 10:01:03", valueMap.get("ts"));
        Assert.assertEquals("0", valueMap.get("__DORIS_DELETE_SIGN__"));
        Assert.assertEquals(6, valueMap.size());

        Map<String, String> beforeMap = objectMapper.readValue(split[0], new TypeReference<Map<String, String>>(){});
        Assert.assertEquals("doris", beforeMap.get("name"));
    }

    @Test
    public void testSerializeDelete() throws IOException {
        byte[] serializedValue = serializer.serialize("{\"before\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":null,\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924328000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12500,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"d\",\"ts_ms\":1663924328869,\"transaction\":null}");
        Map<String, String> valueMap = objectMapper.readValue(new String(serializedValue, StandardCharsets.UTF_8), new TypeReference<Map<String, String>>(){});
        Assert.assertEquals("1", valueMap.get("id"));
        Assert.assertEquals("doris-update", valueMap.get("name"));
        Assert.assertEquals("2022-01-01", valueMap.get("dt"));
        Assert.assertEquals("2022-01-01 10:01:02", valueMap.get("dtime"));
        Assert.assertEquals("2022-01-01 10:01:03", valueMap.get("ts"));
        Assert.assertEquals("1", valueMap.get("__DORIS_DELETE_SIGN__"));
        Assert.assertEquals(6, valueMap.size());
    }

    @Test
    public void testExtractDDL() throws IOException {
        String srcDDL = "ALTER TABLE test.t1 add COLUMN c_1 varchar(600)";
        String record = "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        String ddl = serializer.extractDDL(recordRoot);
        Assert.assertEquals(srcDDL, ddl);

        String targetDDL = "ALTER TABLE test.t1 add COLUMN c_1 varchar(65533)";
        String record1 = "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(30000)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot1 = objectMapper.readTree(record1);
        String ddl1 = serializer.extractDDL(recordRoot1);
        Assert.assertEquals(targetDDL, ddl1);

        String srcDDL2 = "ALTER TABLE test.t1 add COLUMN c_1 varchar(600) NOT NULL DEFAULT 'test_default' COMMENT 'test_comment'";
        String record2 = "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200) not null DEFAULT 'test_default' COMMENT 'test_comment'\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot2 = objectMapper.readTree(record2);
        String ddl2 = serializer.extractDDL(recordRoot2);
        Assert.assertEquals(srcDDL2, ddl2);
    }

    @Test
    public void testExtractDDLListMultipleColumns() throws IOException {
        String sql0 = "ALTER TABLE test.t1 ADD COLUMN c2 INT";
        String sql1 = "ALTER TABLE test.t1 ADD COLUMN c555 VARCHAR(300)";
        String sql2 = "ALTER TABLE test.t1 ADD COLUMN c666 INT DEFAULT '100'";
        String sql3 = "ALTER TABLE test.t1 ADD COLUMN c4 BIGINT DEFAULT '555'";
        String sql4 = "ALTER TABLE test.t1 ADD COLUMN c199 INT";
        String sql5 = "ALTER TABLE test.t1 ADD COLUMN c12 INT DEFAULT '100'";
        String sql6 = "ALTER TABLE test.t1 DROP COLUMN name";
        String sql7 = "ALTER TABLE test.t1 DROP COLUMN test_time";
        String sql8 = "ALTER TABLE test.t1 DROP COLUMN c1";
        String sql9 = "ALTER TABLE test.t1 DROP COLUMN cc";
        List<String> srcSqlList = Arrays.asList(sql0, sql1, sql2, sql3, sql4,sql5,sql6,sql7,sql8,sql9);

        String record = "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1691033764674,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000029\",\"pos\":23305,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23305,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1691033764,\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23464,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 drop c11, drop column c3, add c12 int default 100\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c555\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c666\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c4\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"555\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c199\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c12\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":7,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = serializer.extractDDLList(recordRoot);
        for (int i = 0; i < ddlSQLList.size(); i++) {
            String srcSQL = srcSqlList.get(i);
            String targetSQL = ddlSQLList.get(i);
            Assert.assertEquals(srcSQL, targetSQL);
        }
    }

    @Test
    public void testExtractDDLListRenameColumn() throws IOException {
        String record = "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1691034519226,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000029\",\"pos\":23752,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23752,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1691034519,\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23886,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 rename column c22 to c33\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c555\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c666\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c4\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"555\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c199\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c33\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":7,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = serializer.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testFillOriginSchema() throws IOException {
        Map<String, FieldSchema> srcFiledSchemaMap = new LinkedHashMap<>();
        srcFiledSchemaMap.put("id", new FieldSchema("id", "INT", null, null));
        srcFiledSchemaMap.put("name", new FieldSchema("name", "VARCHAR(150)", null, null));
        srcFiledSchemaMap.put("test_time", new FieldSchema("test_time", "DATETIMEV2(0)", null, null));
        srcFiledSchemaMap.put("c1", new FieldSchema("c1", "INT", "'100'", null));

        String columnsString = "[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":50,\"position\":2,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"test_time\",\"jdbcType\":93,\"typeName\":\"DATETIME\",\"typeExpression\":\"DATETIME\",\"charsetName\":null,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"c1\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"100\",\"enumValues\":[]},{\"name\":\"cc\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"100\",\"enumValues\":[]}]";
        JsonNode columns = objectMapper.readTree(columnsString);
        serializer.fillOriginSchema(columns);
        Map<String, FieldSchema> originFieldSchemaMap = serializer.getOriginFieldSchemaMap();

        Iterator<Entry<String, FieldSchema>> originFieldSchemaIterator = originFieldSchemaMap.entrySet().iterator();
        for (Entry<String, FieldSchema> entry : srcFiledSchemaMap.entrySet()) {
            FieldSchema srcFiledSchema = entry.getValue();
            Entry<String, FieldSchema> originField = originFieldSchemaIterator.next();

            Assert.assertEquals(entry.getKey(), originField.getKey());
            Assert.assertEquals(srcFiledSchema.getName(), originField.getValue().getName());
            Assert.assertEquals(srcFiledSchema.getTypeString(), originField.getValue().getTypeString());
            Assert.assertEquals(srcFiledSchema.getDefaultValue(), originField.getValue().getDefaultValue());
            Assert.assertEquals(srcFiledSchema.getComment(), originField.getValue().getComment());
        }
    }

    @Ignore
    @Test
    public void testSerializeAddColumn() throws IOException, DorisException {
        // alter table t1 add  column  c_1 varchar(200)
        String record = "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n column  c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        boolean flag = serializer.schemaChange(recordRoot);
        Assert.assertEquals(true, flag);

        Field targetField = getField("c_1");
        Assert.assertNotNull(targetField);
        Assert.assertEquals("c_1", targetField.getName());
        Assert.assertEquals("VARCHAR", targetField.getType());
    }

    @Ignore
    @Test
    public void testSerializeDropColumn() throws IOException, DorisException {
        //alter table  t1 drop  column  c_1;
        String ddl = "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663925897321,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13298,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13298,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663925897,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13422,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table    t1 drop \\\\n column  c_1\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(ddl);
        boolean flag = serializer.schemaChange(recordRoot);
        Assert.assertEquals(true, flag);

        Field targetField = getField("c_1");
        Assert.assertNull(targetField);
    }

    private static Field getField(String column) throws DorisException{
        //get table schema
        Schema schema = RestService.getSchema(dorisOptions, DorisReadOptions.builder().build(), LOG);
        List<Field> properties = schema.getProperties();
        Field targetField = null;
        for(Field field : properties){
            if(column.equals(field.getName())){
                targetField = field;
                break;
            }
        }
        return targetField;
    }
}
