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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Field;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for JsonDebeziumSchemaChangeImpl. */
public class TestJsonDebeziumSchemaChangeImpl extends TestJsonDebeziumChangeBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(TestJsonDebeziumSchemaChangeImpl.class);

    private JsonDebeziumSchemaChangeImpl schemaChange;
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
        schemaChange = new JsonDebeziumSchemaChangeImpl(changeContext);
    }

    @Test
    public void testExtractDDL() throws IOException, IllegalArgumentException {
        String srcDDL = "ALTER TABLE test.t1 add COLUMN c_1 varchar(600)";
        String record =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        String ddl = schemaChange.extractDDL(recordRoot);
        Assert.assertEquals(srcDDL, ddl);

        String targetDDL = "ALTER TABLE test.t1 add COLUMN c_1 varchar(65533)";
        String record1 =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(30000)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot1 = objectMapper.readTree(record1);
        String ddl1 = schemaChange.extractDDL(recordRoot1);
        Assert.assertEquals(targetDDL, ddl1);
    }

    @Test
    public void testSchemaChangeMultiTable() throws Exception {
        String tmp = dorisOptions.getTableIdentifier();
        dorisOptions.setTableIdentifier(null);
        String ddl1 =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"mysql\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        String ddl2 =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"mysql\",\"sequence\":null,\"table\":\"t2\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        String exceptDDL1 = "ALTER TABLE doris.t1 add COLUMN c_1 varchar(600)";
        String exceptDDL2 = "ALTER TABLE doris.t2 add COLUMN c_1 varchar(600)";

        Assert.assertEquals(exceptDDL1, schemaChange.extractDDL(objectMapper.readTree(ddl1)));
        Assert.assertEquals(exceptDDL2, schemaChange.extractDDL(objectMapper.readTree(ddl2)));

        // Assert.assertEquals(exceptDDL1, serializer.extractDDLList(objectMapper.readTree(ddl1)));
        // Assert.assertEquals(exceptDDL2, serializer.extractDDLList(objectMapper.readTree(ddl2)));

        dorisOptions.setTableIdentifier(tmp);
    }

    @Test
    public void testSerializeAddColumn() throws IOException, DorisException {
        // alter table t1 add  column  c_1 varchar(200)
        String record =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n column  c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        SchemaChangeManager manager = mock(SchemaChangeManager.class);
        schemaChange.setSchemaChangeManager(manager);
        when(manager.checkSchemaChange(any(), any(), any())).thenReturn(true);
        when(manager.execute(any(), any())).thenReturn(true);
        boolean flag = schemaChange.schemaChange(recordRoot);
        Assert.assertTrue(flag);

        String record2 =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db1\":\"test\",\"sequence\":null,\"table1\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl1\\\":\\\"alter table t1 add \\\\n column  c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot2 = objectMapper.readTree(record2);
        Assert.assertFalse(schemaChange.schemaChange(recordRoot2));

        //        Field targetField = getField("c_1");
        //        Assert.assertNotNull(targetField);
        //        Assert.assertEquals("c_1", targetField.getName());
        //        Assert.assertEquals("VARCHAR", targetField.getType());
    }

    @Ignore
    @Test
    public void testSerializeDropColumn() throws IOException, DorisException {
        // alter table  t1 drop  column  c_1;
        String ddl =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663925897321,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13298,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13298,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663925897,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13422,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table    t1 drop \\\\n column  c_1\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(ddl);
        boolean flag = schemaChange.schemaChange(recordRoot);
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
}
