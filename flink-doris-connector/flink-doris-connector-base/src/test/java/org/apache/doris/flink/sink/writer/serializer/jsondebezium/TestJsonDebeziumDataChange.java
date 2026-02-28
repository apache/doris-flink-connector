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

package org.apache.doris.flink.sink.writer.serializer.jsondebezium;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Test for JsonDebeziumDataChange. */
public class TestJsonDebeziumDataChange extends TestJsonDebeziumChangeBase {

    private JsonDebeziumDataChange dataChange;
    private JsonDebeziumChangeContext changeContext;

    @Before
    public void setUp() {
        super.setUp();
        changeContext =
                new JsonDebeziumChangeContext(
                        dorisOptions,
                        tableMapping,
                        null,
                        null,
                        null,
                        objectMapper,
                        null,
                        lineDelimiter,
                        ignoreUpdateBefore,
                        "",
                        "",
                        true);
        dataChange = new JsonDebeziumDataChange(changeContext);
    }

    @Test
    public void testSerializeInsert() throws IOException {
        // insert into t1
        // VALUES(1,"doris",'2022-01-01','2022-01-01 10:01:02','2022-01-0110:01:03');
        String record =
                "{\"before\":null,\"after\":{\"id\":1,\"name\":\"doris\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663923840000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":11834,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1663923840146,\"transaction\":null}";
        Map<String, String> valueMap = extractValueMap(record);
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
        String record =
                "{\"before\":{\"id\":1,\"name\":\"doris\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924082000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12154,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"u\",\"ts_ms\":1663924082186,\"transaction\":null}";
        Map<String, String> valueMap = extractValueMap(record);
        Assert.assertEquals("1", valueMap.get("id"));
        Assert.assertEquals("doris-update", valueMap.get("name"));
        Assert.assertEquals("2022-01-01", valueMap.get("dt"));
        Assert.assertEquals("2022-01-01 10:01:02", valueMap.get("dtime"));
        Assert.assertEquals("2022-01-01 10:01:03", valueMap.get("ts"));
        Assert.assertEquals("0", valueMap.get("__DORIS_DELETE_SIGN__"));
        Assert.assertEquals(6, valueMap.size());
    }

    @Test
    public void testSerializeDelete() throws IOException {
        String record =
                "{\"before\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":null,\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924328000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12500,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"d\",\"ts_ms\":1663924328869,\"transaction\":null}";
        Map<String, String> valueMap = extractValueMap(record);
        Assert.assertEquals("1", valueMap.get("id"));
        Assert.assertEquals("doris-update", valueMap.get("name"));
        Assert.assertEquals("2022-01-01", valueMap.get("dt"));
        Assert.assertEquals("2022-01-01 10:01:02", valueMap.get("dtime"));
        Assert.assertEquals("2022-01-01 10:01:03", valueMap.get("ts"));
        Assert.assertEquals("1", valueMap.get("__DORIS_DELETE_SIGN__"));
        Assert.assertEquals(6, valueMap.size());
    }

    @Test
    public void testSerializeUpdateBefore() throws IOException {
        changeContext =
                new JsonDebeziumChangeContext(
                        dorisOptions,
                        tableMapping,
                        null,
                        null,
                        null,
                        objectMapper,
                        null,
                        lineDelimiter,
                        false,
                        "",
                        "",
                        true);
        dataChange = new JsonDebeziumDataChange(changeContext);

        // update t1 set name='doris-update' WHERE id =1;
        String record =
                "{\"before\":{\"id\":1,\"name\":\"doris\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924082000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12154,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"u\",\"ts_ms\":1663924082186,\"transaction\":null}";
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        String op = extractJsonNode(recordRoot, "op");
        DorisRecord dorisRecord = dataChange.serialize(record, recordRoot, op);
        byte[] serializedValue = dorisRecord.getRow();
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

    private Map<String, String> extractValueMap(String record) throws IOException {
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        String op = extractJsonNode(recordRoot, "op");
        DorisRecord dorisRecord = dataChange.serialize(record, recordRoot, op);
        byte[] serializedValue = dorisRecord.getRow();
        return objectMapper.readValue(
                new String(serializedValue, StandardCharsets.UTF_8),
                new TypeReference<Map<String, String>>() {});
    }

    @Test
    public void testGetCdcTableIdentifier() throws Exception {
        String insert =
                "{\"before\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":null,\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924328000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12500,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"d\",\"ts_ms\":1663924328869,\"transaction\":null}";
        JsonNode recordRoot = objectMapper.readTree(insert);
        String identifier = JsonDebeziumChangeUtils.getCdcTableIdentifier(recordRoot);
        Assert.assertEquals("test.t1", identifier);

        String insertSchema =
                "{\"before\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":null,\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924328000,\"snapshot\":\"false\",\"db\":\"test\",\"schema\":\"dbo\",\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12500,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"d\",\"ts_ms\":1663924328869,\"transaction\":null}";
        String identifierSchema =
                JsonDebeziumChangeUtils.getCdcTableIdentifier(objectMapper.readTree(insertSchema));
        Assert.assertEquals("test.dbo.t1", identifierSchema);

        String ddl =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        String ddlRes = JsonDebeziumChangeUtils.getCdcTableIdentifier(objectMapper.readTree(ddl));
        Assert.assertEquals("test.t1", ddlRes);
    }

    @Test
    public void testGetDorisTableIdentifier() throws Exception {
        String identifier =
                JsonDebeziumChangeUtils.getDorisTableIdentifier(
                        "test.dbo.t1", dorisOptions, tableMapping);
        Assert.assertEquals("test.t1", identifier);

        identifier =
                JsonDebeziumChangeUtils.getDorisTableIdentifier(
                        "test.t1", dorisOptions, tableMapping);
        Assert.assertEquals("test.t1", identifier);

        String tmp = dorisOptions.getTableIdentifier();
        dorisOptions.setTableIdentifier(null);
        identifier =
                JsonDebeziumChangeUtils.getDorisTableIdentifier(
                        "test.t1", dorisOptions, tableMapping);
        Assert.assertNull(identifier);
        dorisOptions.setTableIdentifier(tmp);
    }

    private String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null && !(record.get(key) instanceof NullNode)
                ? record.get(key).asText()
                : null;
    }
}
