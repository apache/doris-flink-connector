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
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Field;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** test for JsonDebeziumSchemaSerializer. */
public class TestJsonDebeziumSchemaSerializer {
    private static final Logger LOG =
            LoggerFactory.getLogger(TestJsonDebeziumSchemaSerializer.class);
    static DorisOptions dorisOptions;
    static JsonDebeziumSchemaSerializer serializer;
    static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    public static void setUp() {
        dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("test.t1")
                        .setUsername("root")
                        .setPassword("")
                        .build();
        serializer = JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build();
    }

    @Test
    public void testSerializeInsert() throws IOException {
        // insert into t1 VALUES(1,"doris",'2022-01-01','2022-01-01 10:01:02','2022-01-01
        // 10:01:03');
        byte[] serializedValue =
                serializer
                        .serialize(
                                "{\"before\":null,\"after\":{\"id\":1,\"name\":\"doris\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663923840000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":11834,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1663923840146,\"transaction\":null}")
                        .getRow();
        Map<String, String> valueMap =
                objectMapper.readValue(
                        new String(serializedValue, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, String>>() {});
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
        // update t1 set name='doris-update' WHERE id =1;
        byte[] serializedValue =
                serializer
                        .serialize(
                                "{\"before\":{\"id\":1,\"name\":\"doris\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924082000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12154,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"u\",\"ts_ms\":1663924082186,\"transaction\":null}")
                        .getRow();
        Map<String, String> valueMap =
                objectMapper.readValue(
                        new String(serializedValue, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, String>>() {});
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
        serializer =
                JsonDebeziumSchemaSerializer.builder()
                        .setDorisOptions(dorisOptions)
                        .setExecutionOptions(
                                DorisExecutionOptions.builderDefaults()
                                        .setIgnoreUpdateBefore(false)
                                        .build())
                        .build();
        // update t1 set name='doris-update' WHERE id =1;
        byte[] serializedValue =
                serializer
                        .serialize(
                                "{\"before\":{\"id\":1,\"name\":\"doris\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924082000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12154,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"u\",\"ts_ms\":1663924082186,\"transaction\":null}")
                        .getRow();
        String row = new String(serializedValue, StandardCharsets.UTF_8);
        String[] split = row.split("\n");
        Map<String, String> valueMap =
                objectMapper.readValue(split[1], new TypeReference<Map<String, String>>() {});
        Assert.assertEquals("1", valueMap.get("id"));
        Assert.assertEquals("doris-update", valueMap.get("name"));
        Assert.assertEquals("2022-01-01", valueMap.get("dt"));
        Assert.assertEquals("2022-01-01 10:01:02", valueMap.get("dtime"));
        Assert.assertEquals("2022-01-01 10:01:03", valueMap.get("ts"));
        Assert.assertEquals("0", valueMap.get("__DORIS_DELETE_SIGN__"));
        Assert.assertEquals(6, valueMap.size());

        Map<String, String> beforeMap =
                objectMapper.readValue(split[0], new TypeReference<Map<String, String>>() {});
        Assert.assertEquals("doris", beforeMap.get("name"));
    }

    @Test
    public void testSerializeDelete() throws IOException {
        byte[] serializedValue =
                serializer
                        .serialize(
                                "{\"before\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":null,\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924328000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12500,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"d\",\"ts_ms\":1663924328869,\"transaction\":null}")
                        .getRow();
        Map<String, String> valueMap =
                objectMapper.readValue(
                        new String(serializedValue, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, String>>() {});
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
        String record =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        String ddl = serializer.extractDDL(recordRoot);
        Assert.assertEquals(srcDDL, ddl);

        String targetDDL = "ALTER TABLE test.t1 add COLUMN c_1 varchar(65533)";
        String record1 =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(30000)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot1 = objectMapper.readTree(record1);
        String ddl1 = serializer.extractDDL(recordRoot1);
        Assert.assertEquals(targetDDL, ddl1);
    }

    @Test
    public void testExtractDDLListMultipleColumns() throws IOException {
        String sql0 = "ALTER TABLE test.t1 ADD COLUMN id INT DEFAULT '10000'";
        String sql1 = "ALTER TABLE test.t1 ADD COLUMN c199 INT";
        String sql2 = "ALTER TABLE test.t1 ADD COLUMN c12 INT DEFAULT '100'";
        String sql3 = "ALTER TABLE test.t1 DROP COLUMN c13";
        List<String> srcSqlList = Arrays.asList(sql0, sql1, sql2, sql3);

        Map<String, FieldSchema> originFiledSchemaMap = new LinkedHashMap<>();
        originFiledSchemaMap.put("c2", new FieldSchema());
        originFiledSchemaMap.put("c555", new FieldSchema());
        originFiledSchemaMap.put("c666", new FieldSchema());
        originFiledSchemaMap.put("c4", new FieldSchema());
        originFiledSchemaMap.put("c13", new FieldSchema());

        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1691033764674,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000029\",\"pos\":23305,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23305,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1691033764,\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23464,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 drop c11, drop column c3, add c12 int default 100\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c555\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c666\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c4\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"555\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c199\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c12\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":7,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        serializer.setOriginFieldSchemaMap(originFiledSchemaMap);
        List<String> ddlSQLList = serializer.extractDDLList(recordRoot);
        for (int i = 0; i < ddlSQLList.size(); i++) {
            String srcSQL = srcSqlList.get(i);
            String targetSQL = ddlSQLList.get(i);
            Assert.assertEquals(srcSQL, targetSQL);
        }
    }

    @Test
    public void testExtractDDLListCreateTable() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"oracle\",\"name\":\"oracle_logminer\",\"ts_ms\":1696945825065,\"snapshot\":\"true\",\"db\":\"HELOWIN\",\"sequence\":null,\"schema\":\"ADMIN\",\"table\":\"PERSONS\",\"txId\":null,\"scn\":\"1199617\",\"commit_scn\":null,\"lcr_position\":null,\"rs_id\":null,\"ssn\":0,\"redo_thread\":null},\"databaseName\":\"HELOWIN\",\"schemaName\":\"ADMIN\",\"ddl\":\"\\n  CREATE TABLE \\\"ADMIN\\\".\\\"PERSONS\\\" \\n   (\\t\\\"ID\\\" NUMBER(10,0), \\n\\t\\\"NAME4\\\" VARCHAR2(128) NOT NULL ENABLE, \\n\\t\\\"age4\\\" VARCHAR2(128), \\n\\t\\\"c100\\\" LONG, \\n\\t\\\"c55\\\" VARCHAR2(255), \\n\\t\\\"c77\\\" VARCHAR2(255), \\n\\t PRIMARY KEY (\\\"ID\\\") ENABLE\\n   ) ;\\n \",\"tableChanges\":[{\"type\":\"CREATE\",\"id\":\"\\\"HELOWIN\\\".\\\"ADMIN\\\".\\\"PERSONS\\\"\",\"table\":{\"defaultCharsetName\":null,\"primaryKeyColumnNames\":[\"ID\"],\"columns\":[{\"name\":\"ID\",\"jdbcType\":2,\"nativeType\":null,\"typeName\":\"NUMBER\",\"typeExpression\":\"NUMBER\",\"charsetName\":null,\"length\":10,\"scale\":0,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null},{\"name\":\"NAME4\",\"jdbcType\":12,\"nativeType\":null,\"typeName\":\"VARCHAR2\",\"typeExpression\":\"VARCHAR2\",\"charsetName\":null,\"length\":128,\"scale\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null},{\"name\":\"age4\",\"jdbcType\":12,\"nativeType\":null,\"typeName\":\"VARCHAR2\",\"typeExpression\":\"VARCHAR2\",\"charsetName\":null,\"length\":128,\"scale\":null,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null},{\"name\":\"c100\",\"jdbcType\":-1,\"nativeType\":null,\"typeName\":\"LONG\",\"typeExpression\":\"LONG\",\"charsetName\":null,\"length\":0,\"scale\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null},{\"name\":\"c55\",\"jdbcType\":12,\"nativeType\":null,\"typeName\":\"VARCHAR2\",\"typeExpression\":\"VARCHAR2\",\"charsetName\":null,\"length\":255,\"scale\":null,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null},{\"name\":\"c77\",\"jdbcType\":12,\"nativeType\":null,\"typeName\":\"VARCHAR2\",\"typeExpression\":\"VARCHAR2\",\"charsetName\":null,\"length\":255,\"scale\":null,\"position\":6,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null}],\"comment\":null}}]}";
        JsonNode recordRoot = objectMapper.readTree(record);
        serializer.setSourceConnector("oracle");
        List<String> ddlSQLList = serializer.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
        serializer.setSourceConnector("mysql");
    }

    @Test
    public void testExtractDDLListTruncateTable() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1696944601264,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink11\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000043\",\"pos\":5719,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":5719,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1696944601,\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":5824,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"truncate table test_sink11\\\",\\\"tableChanges\\\":[]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = serializer.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testExtractDDLListDropTable() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1696944747956,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink11\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000043\",\"pos\":5901,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":5901,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1696944747,\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6037,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"DROP TABLE `test`.`test_sink11`\\\",\\\"tableChanges\\\":[]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = serializer.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testExtractDDLListChangeColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1696945030603,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000043\",\"pos\":6521,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6521,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1696945030,\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6661,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table test_sink change column c555 c777 bigint\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"test_sink\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c777\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = serializer.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testExtractDDLListModifyColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1696945306941,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000043\",\"pos\":6738,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6738,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1696945306,\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6884,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table test_sink modify column c777 tinyint default 7\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"test_sink\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c777\\\",\\\"jdbcType\\\":5,\\\"typeName\\\":\\\"TINYINT\\\",\\\"typeExpression\\\":\\\"TINYINT\\\",\\\"charsetName\\\":null,\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"7\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = serializer.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testExtractDDLListRenameColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1691034519226,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000029\",\"pos\":23752,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23752,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1691034519,\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23886,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 rename column c22 to c33\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c555\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c666\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c4\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"555\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c199\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c33\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":7,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = serializer.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testFillOriginSchema() throws IOException {
        Map<String, FieldSchema> srcFiledSchemaMap = new LinkedHashMap<>();
        srcFiledSchemaMap.put("id", new FieldSchema("id", "INT", null, null));
        srcFiledSchemaMap.put("name", new FieldSchema("name", "VARCHAR(150)", null, null));
        srcFiledSchemaMap.put(
                "test_time", new FieldSchema("test_time", "DATETIMEV2(0)", null, null));
        srcFiledSchemaMap.put("c1", new FieldSchema("c1", "INT", "'100'", null));

        serializer.setSourceConnector("mysql");
        String columnsString =
                "[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":50,\"position\":2,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"test_time\",\"jdbcType\":93,\"typeName\":\"DATETIME\",\"typeExpression\":\"DATETIME\",\"charsetName\":null,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"c1\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"100\",\"enumValues\":[]},{\"name\":\"cc\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"100\",\"enumValues\":[]}]";
        JsonNode columns = objectMapper.readTree(columnsString);
        serializer.fillOriginSchema(columns);
        Map<String, FieldSchema> originFieldSchemaMap = serializer.getOriginFieldSchemaMap();

        Iterator<Entry<String, FieldSchema>> originFieldSchemaIterator =
                originFieldSchemaMap.entrySet().iterator();
        for (Entry<String, FieldSchema> entry : srcFiledSchemaMap.entrySet()) {
            FieldSchema srcFiledSchema = entry.getValue();
            Entry<String, FieldSchema> originField = originFieldSchemaIterator.next();

            Assert.assertEquals(entry.getKey(), originField.getKey());
            Assert.assertEquals(srcFiledSchema.getName(), originField.getValue().getName());
            Assert.assertEquals(
                    srcFiledSchema.getTypeString(), originField.getValue().getTypeString());
            Assert.assertEquals(
                    srcFiledSchema.getDefaultValue(), originField.getValue().getDefaultValue());
            Assert.assertEquals(srcFiledSchema.getComment(), originField.getValue().getComment());
        }
    }

    @Test
    public void testBuildMysql2DorisTypeName() throws IOException {
        String columnInfo =
                "{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"10\",\"enumValues\":[]}";
        serializer.setSourceConnector("mysql");
        JsonNode columns = objectMapper.readTree(columnInfo);
        String dorisTypeName = serializer.buildDorisTypeName(columns);
        Assert.assertEquals(dorisTypeName, "INT");
    }

    @Test
    public void testBuildOracle2DorisTypeName() throws IOException {
        String columnInfo =
                "{\"name\":\"NAME\",\"jdbcType\":12,\"nativeType\":null,\"typeName\":\"VARCHAR2\",\"typeExpression\":\"VARCHAR2\",\"charsetName\":null,\"length\":128,\"scale\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null}";
        serializer.setSourceConnector("oracle");
        JsonNode columns = objectMapper.readTree(columnInfo);
        String dorisTypeName = serializer.buildDorisTypeName(columns);
        Assert.assertEquals(dorisTypeName, "VARCHAR(384)");
    }

    @Test
    public void testExtractDDLListRename() throws IOException {
        String columnInfo =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1698314781975,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000046\",\"pos\":5197,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000046\\\",\\\"pos\\\":5197,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1698314781,\\\"file\\\":\\\"binlog.000046\\\",\\\"pos\\\":5331,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 rename column c3 to c333\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c333\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":10,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        Map<String, FieldSchema> originFieldSchemaMap = Maps.newHashMap();
        JsonNode record = objectMapper.readTree(columnInfo);

        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("test.t1")
                        .setUsername("root")
                        .setPassword("")
                        .build();
        JsonDebeziumSchemaSerializer serializer =
                JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build();
        serializer.setSourceConnector("mysql");

        originFieldSchemaMap.put("id", new FieldSchema("id", "INT", "", ""));
        originFieldSchemaMap.put("c2", new FieldSchema("c2", "INT", "", ""));
        originFieldSchemaMap.put("c3", new FieldSchema("c3", "VARCHAR(30)", "", ""));
        serializer.setOriginFieldSchemaMap(originFieldSchemaMap);

        List<String> ddlList = serializer.extractDDLList(record);
        Assert.assertEquals("ALTER TABLE test.t1 RENAME COLUMN c3 c333", ddlList.get(0));
    }

    @Ignore
    @Test
    public void testSerializeAddColumn() throws IOException, DorisException {
        // alter table t1 add  column  c_1 varchar(200)
        String record =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n column  c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
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
        // alter table  t1 drop  column  c_1;
        String ddl =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663925897321,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13298,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13298,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663925897,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13422,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table    t1 drop \\\\n column  c_1\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(ddl);
        boolean flag = serializer.schemaChange(recordRoot);
        Assert.assertEquals(true, flag);

        Field targetField = getField("c_1");
        Assert.assertNull(targetField);
    }

    private static Field getField(String column) throws DorisException {
        // get table schema
        Schema schema =
                RestService.getSchema(dorisOptions, DorisReadOptions.builder().build(), LOG);
        List<Field> properties = schema.getProperties();
        Field targetField = null;
        for (Field field : properties) {
            if (column.equals(field.getName())) {
                targetField = field;
                break;
            }
        }
        return targetField;
    }

    @Test
    public void testGetCdcTableIdentifier() throws Exception {
        String insert =
                "{\"before\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":null,\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924328000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12500,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"d\",\"ts_ms\":1663924328869,\"transaction\":null}";
        JsonNode recordRoot = objectMapper.readTree(insert);
        String identifier = serializer.getCdcTableIdentifier(recordRoot);
        Assert.assertEquals("test.t1", identifier);

        String insertSchema =
                "{\"before\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":null,\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924328000,\"snapshot\":\"false\",\"db\":\"test\",\"schema\":\"dbo\",\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12500,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"d\",\"ts_ms\":1663924328869,\"transaction\":null}";
        String identifierSchema =
                serializer.getCdcTableIdentifier(objectMapper.readTree(insertSchema));
        Assert.assertEquals("test.dbo.t1", identifierSchema);

        String ddl =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        String ddlRes = serializer.getCdcTableIdentifier(objectMapper.readTree(ddl));
        Assert.assertEquals("test.t1", ddlRes);
    }

    @Test
    public void testGetDorisTableIdentifier() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("test.dbo.t1", "test.t1");
        serializer.setTableMapping(map);
        String identifier = serializer.getDorisTableIdentifier("test.dbo.t1");
        Assert.assertEquals("test.t1", identifier);

        identifier = serializer.getDorisTableIdentifier("test.t1");
        Assert.assertEquals("test.t1", identifier);

        String tmp = dorisOptions.getTableIdentifier();
        dorisOptions.setTableIdentifier(null);
        identifier = serializer.getDorisTableIdentifier("test.t1");
        Assert.assertNull(identifier);
        dorisOptions.setTableIdentifier(tmp);
    }

    @Test
    public void testSchemaChangeMultiTable() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("mysql.t1", "doris.t1");
        map.put("mysql.t2", "doris.t2");
        serializer.setTableMapping(map);
        String tmp = dorisOptions.getTableIdentifier();
        dorisOptions.setTableIdentifier(null);
        String ddl1 =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"mysql\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        String ddl2 =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"mysql\",\"sequence\":null,\"table\":\"t2\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        String exceptDDL1 = "ALTER TABLE doris.t1 add COLUMN c_1 varchar(600)";
        String exceptDDL2 = "ALTER TABLE doris.t2 add COLUMN c_1 varchar(600)";

        Assert.assertEquals(exceptDDL1, serializer.extractDDL(objectMapper.readTree(ddl1)));
        Assert.assertEquals(exceptDDL2, serializer.extractDDL(objectMapper.readTree(ddl2)));

        // Assert.assertEquals(exceptDDL1, serializer.extractDDLList(objectMapper.readTree(ddl1)));
        // Assert.assertEquals(exceptDDL2, serializer.extractDDLList(objectMapper.readTree(ddl2)));

        dorisOptions.setTableIdentifier(tmp);
    }

    @Test
    @Ignore
    public void testAutoCreateTable() throws Exception {
        String record =
                "{    \"source\":{        \"version\":\"1.9.7.Final\",        \"connector\":\"oracle\",        \"name\":\"oracle_logminer\",        \"ts_ms\":1696945825065,        \"snapshot\":\"true\",        \"db\":\"TESTDB\",        \"sequence\":null,        \"schema\":\"ADMIN\",        \"table\":\"PERSONS\",        \"txId\":null,        \"scn\":\"1199617\",        \"commit_scn\":null,        \"lcr_position\":null,        \"rs_id\":null,        \"ssn\":0,        \"redo_thread\":null    },    \"databaseName\":\"TESTDB\",    \"schemaName\":\"ADMIN\",    \"ddl\":\"\\n  CREATE TABLE \\\"ADMIN\\\".\\\"PERSONS\\\" \\n   (\\t\\\"ID\\\" NUMBER(10,0), \\n\\t\\\"NAME4\\\" VARCHAR2(128) NOT NULL ENABLE, \\n\\t\\\"age4\\\" VARCHAR2(128), \\n\\t PRIMARY KEY (\\\"ID\\\") ENABLE\\n   ) ;\\n \",    \"tableChanges\":[        {            \"type\":\"CREATE\",            \"id\":\"\\\"TESTDB\\\".\\\"ADMIN\\\".\\\"PERSONS\\\"\",            \"table\":{                \"defaultCharsetName\":null,                \"primaryKeyColumnNames\":[                    \"ID\"                ],                \"columns\":[                    {                        \"name\":\"ID\",                        \"jdbcType\":2,                        \"nativeType\":null,                        \"typeName\":\"NUMBER\",                        \"typeExpression\":\"NUMBER\",                        \"charsetName\":null,                        \"length\":10,                        \"scale\":0,                        \"position\":1,                        \"optional\":false,                        \"autoIncremented\":false,                        \"generated\":false,                        \"comment\":null                    },                    {                        \"name\":\"NAME4\",                        \"jdbcType\":12,                        \"nativeType\":null,                        \"typeName\":\"VARCHAR2\",                        \"typeExpression\":\"VARCHAR2\",                        \"charsetName\":null,                        \"length\":128,                        \"scale\":null,                        \"position\":2,                        \"optional\":false,                        \"autoIncremented\":false,                        \"generated\":false,                        \"comment\":null                    },                    {                        \"name\":\"age4\",                        \"jdbcType\":12,                        \"nativeType\":null,                        \"typeName\":\"VARCHAR2\",                        \"typeExpression\":\"VARCHAR2\",                        \"charsetName\":null,                        \"length\":128,                        \"scale\":null,                        \"position\":3,                        \"optional\":true,                        \"autoIncremented\":false,                        \"generated\":false,                        \"comment\":null                    }                ],                \"comment\":null            }        }    ]}";
        JsonNode recordRoot = objectMapper.readTree(record);
        dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("")
                        .setUsername("root")
                        .setPassword("")
                        .build();
        serializer = JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build();
        serializer.setSourceConnector(SourceConnector.ORACLE.connectorName);
        TableSchema tableSchema = serializer.extractCreateTableSchema(recordRoot);
        Assert.assertEquals("TESTDB", tableSchema.getDatabase());
        Assert.assertEquals("PERSONS", tableSchema.getTable());
        Assert.assertArrayEquals(new String[] {"ID"}, tableSchema.getKeys().toArray());
        Assert.assertEquals(3, tableSchema.getFields().size());
        Assert.assertEquals("ID", tableSchema.getFields().get("ID").getName());
        Assert.assertEquals("NAME4", tableSchema.getFields().get("NAME4").getName());
        Assert.assertEquals("age4", tableSchema.getFields().get("age4").getName());
        serializer.setSourceConnector(SourceConnector.MYSQL.connectorName);
    }
}
