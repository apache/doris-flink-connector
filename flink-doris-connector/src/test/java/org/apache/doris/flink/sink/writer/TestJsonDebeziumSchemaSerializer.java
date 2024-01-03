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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumSchemaChange;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Test for JsonDebeziumSchemaSerializer. */
public class TestJsonDebeziumSchemaSerializer {

    protected static DorisOptions dorisOptions;
    protected ObjectMapper objectMapper = new ObjectMapper();
    private JsonDebeziumSchemaSerializer serializer;
    private SchemaChangeManager mockSchemaChangeManager;

    @Before
    public void setUp() throws IOException, IllegalArgumentException {
        dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("test.t1")
                        .setUsername("root")
                        .setPassword("")
                        .build();
        this.objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        this.objectMapper.setNodeFactory(jsonNodeFactory);
        this.serializer =
                JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build();
        this.mockSchemaChangeManager = Mockito.mock(SchemaChangeManager.class);
        Mockito.when(
                        mockSchemaChangeManager.checkSchemaChange(
                                Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(true);
    }

    @Test
    public void testJsonDebeziumDataChange() throws IOException {
        // insert into t1
        // VALUES(1,"doris",'2022-01-01','2022-01-01 10:01:02','2022-01-0110:01:03');
        String record =
                "{\"before\":null,\"after\":{\"id\":1,\"name\":\"doris\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663923840000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":11834,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1663923840146,\"transaction\":null}";
        DorisRecord dorisRecord = serializer.serialize(record);
        byte[] row = dorisRecord.getRow();
        Map<String, String> valueMap =
                objectMapper.readValue(
                        new String(row, StandardCharsets.UTF_8),
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
    public void testTestJsonDebeziumSchemaChangeImpl() throws IOException {
        // ALTER TABLE test.t1 add COLUMN c_1 varchar(600)
        String record =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonDebeziumSchemaChange schemaChange = serializer.getJsonDebeziumSchemaChange();
        schemaChange.setSchemaChangeManager(mockSchemaChangeManager);
        DorisRecord serializeRecord = serializer.serialize(record);
        Assert.assertNull(serializeRecord);
    }

    @Test
    public void testTestJsonDebeziumSchemaChangeImplV2() throws IOException {
        // alter table test_sink add column c4 varchar(100) default 'aaa', drop column c3;
        String recordValue =
                "{\"before\":null,\"after\":{\"id\":1,\"c3\":1111},\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":0,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink\",\"server_id\":0,\"gtid\":null,\"file\":\"\",\"pos\":0,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"r\",\"ts_ms\":1703151083107,\"transaction\":null}";
        String recordSchema =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1703151236431,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000050\",\"pos\":6040,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000050\\\",\\\"pos\\\":6040,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1703151236,\\\"file\\\":\\\"binlog.000050\\\",\\\"pos\\\":6206,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table test_sink add column c4 varchar(100) default 'aaa', drop column c3\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"test_sink\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c4\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":100,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"aaa\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        this.serializer =
                JsonDebeziumSchemaSerializer.builder()
                        .setDorisOptions(dorisOptions)
                        .setNewSchemaChange(true)
                        .build();
        JsonDebeziumSchemaChange schemaChange = serializer.getJsonDebeziumSchemaChange();
        schemaChange.setSchemaChangeManager(mockSchemaChangeManager);
        serializer.serialize(recordValue);
        DorisRecord serializeRecord = serializer.serialize(recordSchema);
        Assert.assertNull(serializeRecord);
    }
}
