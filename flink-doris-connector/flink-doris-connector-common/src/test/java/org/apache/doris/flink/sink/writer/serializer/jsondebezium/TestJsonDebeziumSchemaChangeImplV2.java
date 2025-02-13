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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

/** Test for JsonDebeziumSchemaChangeImplV2. */
public class TestJsonDebeziumSchemaChangeImplV2 extends TestJsonDebeziumChangeBase {

    private JsonDebeziumSchemaChangeImplV2 schemaChange;
    private JsonDebeziumChangeContext changeContext;
    private MockedStatic<RestService> mockRestService;

    @Before
    public void setUp() {
        super.setUp();
        String sourceTableName = null;
        String targetDatabase = "TESTDB";
        Map<String, String> tableProperties = new HashMap<>();
        String schemaStr =
                "{\"keysType\":\"UNIQUE_KEYS\",\"properties\":[{\"name\":\"id\",\"aggregation_type\":\"\",\"comment\":\"\",\"type\":\"INT\"},{\"name\":\"name\",\"aggregation_type\":\"NONE\",\"comment\":\"\",\"type\":\"VARCHAR\"}],\"status\":200}";
        Schema schema = null;
        try {
            schema = objectMapper.readValue(schemaStr, Schema.class);
        } catch (JsonProcessingException e) {
            throw new DorisRuntimeException(e);
        }
        mockRestService = mockStatic(RestService.class);
        mockRestService
                .when(() -> RestService.getSchema(any(), any(), any(), any()))
                .thenReturn(schema);
        changeContext =
                new JsonDebeziumChangeContext(
                        dorisOptions,
                        tableMapping,
                        sourceTableName,
                        targetDatabase,
                        new DorisTableConfig(tableProperties),
                        objectMapper,
                        null,
                        lineDelimiter,
                        ignoreUpdateBefore,
                        "",
                        "",
                        true);
        schemaChange = new JsonDebeziumSchemaChangeImplV2(changeContext);
    }

    @Test
    public void testExtractDDLListMultipleColumns() throws IOException {
        String sql0 = "ALTER TABLE `test`.`t1` ADD COLUMN `id` INT DEFAULT '10000'";
        String sql1 = "ALTER TABLE `test`.`t1` ADD COLUMN `c199` INT";
        String sql2 = "ALTER TABLE `test`.`t1` ADD COLUMN `c12` INT DEFAULT '100'";
        String sql3 = "ALTER TABLE `test`.`t1` DROP COLUMN `c13`";
        List<String> srcSqlList = Arrays.asList(sql0, sql1, sql2, sql3);

        Map<String, Map<String, FieldSchema>> originFiledSchemaMap = new LinkedHashMap<>();
        Map<String, FieldSchema> filedSchemaMap = new LinkedHashMap<>();
        filedSchemaMap.put("c2", new FieldSchema());
        filedSchemaMap.put("c555", new FieldSchema());
        filedSchemaMap.put("c666", new FieldSchema());
        filedSchemaMap.put("c4", new FieldSchema());
        filedSchemaMap.put("c13", new FieldSchema());
        originFiledSchemaMap.put("test.t1", filedSchemaMap);

        String record1 =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1691033764674,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000029\",\"pos\":23305,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23305,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1691033764,\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23464,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 drop c11, drop column c3, add c12 int default 100\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c555\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c666\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c4\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"555\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c199\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c12\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":7,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record1);
        schemaChange.setOriginFieldSchemaMap(originFiledSchemaMap);
        List<String> ddlSQLList = schemaChange.extractDDLList(recordRoot);
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
        schemaChange.setSourceConnector("oracle");
        List<String> ddlSQLList = schemaChange.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
        schemaChange.setSourceConnector("mysql");
    }

    @Test
    public void testExtractDDLListTruncateTable() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1696944601264,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink11\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000043\",\"pos\":5719,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":5719,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1696944601,\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":5824,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"truncate table test_sink11\\\",\\\"tableChanges\\\":[]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = schemaChange.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testExtractDDLListDropTable() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1696944747956,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink11\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000043\",\"pos\":5901,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":5901,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1696944747,\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6037,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"DROP TABLE `test`.`test_sink11`\\\",\\\"tableChanges\\\":[]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = schemaChange.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testExtractDDLListChangeColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1696945030603,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000043\",\"pos\":6521,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6521,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1696945030,\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6661,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table test_sink change column c555 c777 bigint\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"test_sink\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c777\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = schemaChange.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testExtractDDLListModifyColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1696945306941,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000043\",\"pos\":6738,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6738,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1696945306,\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6884,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table test_sink modify column c777 tinyint default 7\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"test_sink\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c777\\\",\\\"jdbcType\\\":5,\\\"typeName\\\":\\\"TINYINT\\\",\\\"typeExpression\\\":\\\"TINYINT\\\",\\\"charsetName\\\":null,\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"7\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = schemaChange.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testExtractDDLListRenameColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1691034519226,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000029\",\"pos\":23752,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23752,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1691034519,\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23886,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 rename column c22 to c33\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c555\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c666\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c4\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"555\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c199\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c33\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":7,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = schemaChange.extractDDLList(recordRoot);
        Assert.assertTrue(CollectionUtils.isEmpty(ddlSQLList));
    }

    @Test
    public void testFillOriginSchema() throws IOException {
        Map<String, FieldSchema> srcFiledSchemaMap = new LinkedHashMap<>();
        srcFiledSchemaMap.put("id", new FieldSchema("id", "INT", null, null));
        srcFiledSchemaMap.put("name", new FieldSchema("name", "VARCHAR(150)", null, null));
        srcFiledSchemaMap.put(
                "test_time", new FieldSchema("test_time", "DATETIMEV2(0)", null, null));
        srcFiledSchemaMap.put("c1", new FieldSchema("c1", "INT", "100", null));

        String tableName = "db.test_fill";
        schemaChange.setOriginFieldSchemaMap(buildOriginFiledSchema());
        schemaChange.setSourceConnector("mysql");
        String columnsString =
                "[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":50,\"position\":2,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"test_time\",\"jdbcType\":93,\"typeName\":\"DATETIME\",\"typeExpression\":\"DATETIME\",\"charsetName\":null,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"c1\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"100\",\"enumValues\":[]},{\"name\":\"cc\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"100\",\"enumValues\":[]}]";
        JsonNode columns = objectMapper.readTree(columnsString);
        schemaChange.fillOriginSchema(tableName, columns);
        Map<String, Map<String, FieldSchema>> originFieldSchemaMap =
                schemaChange.getOriginFieldSchemaMap();
        Map<String, FieldSchema> fieldSchemaMap = originFieldSchemaMap.get(tableName);

        eqFiledSchema(fieldSchemaMap, srcFiledSchemaMap);
    }

    @Test
    public void testFillOriginSchemaWithoutFiledSchema() throws IOException {
        Map<String, FieldSchema> srcFiledSchemaMap = new LinkedHashMap<>();
        srcFiledSchemaMap.put("id", new FieldSchema("id", "INT", null, ""));
        srcFiledSchemaMap.put("name", new FieldSchema("name", "VARCHAR", null, ""));

        schemaChange.setSourceConnector("mysql");
        String tableName = "test.test_sink3";
        String columnsString =
                "[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"0\",\"enumValues\":[]},{\"name\":\"c1\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":2,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"c2\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":100,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]}]";
        JsonNode columns = objectMapper.readTree(columnsString);
        schemaChange.fillOriginSchema(tableName, columns);

        Map<String, Map<String, FieldSchema>> originFieldSchemaMap =
                schemaChange.getOriginFieldSchemaMap();
        Assert.assertTrue(originFieldSchemaMap.containsKey(tableName));
        Map<String, FieldSchema> fieldSchemaMap = originFieldSchemaMap.get(tableName);

        eqFiledSchema(fieldSchemaMap, srcFiledSchemaMap);
    }

    private void eqFiledSchema(
            Map<String, FieldSchema> fieldSchemaMap, Map<String, FieldSchema> srcFiledSchemaMap) {
        Iterator<Entry<String, FieldSchema>> originFieldSchemaIterator =
                fieldSchemaMap.entrySet().iterator();
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
    public void testMultipleFillOriginSchema() throws IOException {
        Map<String, Map<String, FieldSchema>> originFiledSchema = buildOriginFiledSchema();
        String tableName1 = "db.test_fill";
        String tableName2 = "test.t1";

        schemaChange.setOriginFieldSchemaMap(originFiledSchema);
        schemaChange.setSourceConnector("mysql");
        String columnsString1 =
                "[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":50,\"position\":2,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"test_time\",\"jdbcType\":93,\"typeName\":\"DATETIME\",\"typeExpression\":\"DATETIME\",\"charsetName\":null,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"c1\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"100\",\"enumValues\":[]},{\"name\":\"cc\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"100\",\"enumValues\":[]}]";
        JsonNode columns1 = objectMapper.readTree(columnsString1);

        String columnsString2 =
                "[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"10000\",\"enumValues\":[]},{\"name\":\"c2\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":2,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"c555\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":100,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"c666\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"100\",\"enumValues\":[]},{\"name\":\"c4\",\"jdbcType\":-5,\"typeName\":\"BIGINT\",\"typeExpression\":\"BIGINT\",\"charsetName\":null,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"555\",\"enumValues\":[]},{\"name\":\"c199\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":6,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"c12\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":7,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"100\",\"enumValues\":[]}]";
        JsonNode columns2 = objectMapper.readTree(columnsString2);

        schemaChange.fillOriginSchema(tableName1, columns1);
        schemaChange.fillOriginSchema(tableName2, columns2);
        Map<String, Map<String, FieldSchema>> originFieldSchemaMap =
                schemaChange.getOriginFieldSchemaMap();

        Map<String, FieldSchema> originSchema1 = originFieldSchemaMap.get(tableName1);
        Map<String, FieldSchema> originSchema2 = originFieldSchemaMap.get(tableName2);

        Map<String, Map<String, FieldSchema>> scrFiledSchema = buildSrcFiledSchema();
        Map<String, FieldSchema> scrSchema1 = scrFiledSchema.get(tableName1);
        Map<String, FieldSchema> scrSchema2 = scrFiledSchema.get(tableName2);

        compareResults(originSchema1, scrSchema1);
        compareResults(originSchema2, scrSchema2);
    }

    private void compareResults(
            Map<String, FieldSchema> originSchema, Map<String, FieldSchema> scrSchema) {
        Iterator<Entry<String, FieldSchema>> originFieldSchemaIterator =
                originSchema.entrySet().iterator();
        for (Entry<String, FieldSchema> srcEntry : scrSchema.entrySet()) {
            FieldSchema srcFiledSchema = srcEntry.getValue();
            Entry<String, FieldSchema> originField = originFieldSchemaIterator.next();

            Assert.assertEquals(srcEntry.getKey(), originField.getKey());
            Assert.assertEquals(srcFiledSchema.getName(), originField.getValue().getName());
            Assert.assertEquals(
                    srcFiledSchema.getTypeString(), originField.getValue().getTypeString());
            Assert.assertEquals(
                    srcFiledSchema.getDefaultValue(), originField.getValue().getDefaultValue());
            Assert.assertEquals(srcFiledSchema.getComment(), originField.getValue().getComment());
        }
    }

    private Map<String, Map<String, FieldSchema>> buildSrcFiledSchema() {
        String tab1 = "db.test_fill";
        String tab2 = "test.t1";
        Map<String, Map<String, FieldSchema>> scrFiledSchema = new LinkedHashMap<>();
        Map<String, FieldSchema> filedSchemaMap1 = new LinkedHashMap<>();
        filedSchemaMap1.put("id", new FieldSchema("id", "INT", null, null));
        filedSchemaMap1.put("name", new FieldSchema("name", "VARCHAR(150)", null, null));
        filedSchemaMap1.put("test_time", new FieldSchema("test_time", "DATETIMEV2(0)", null, null));
        filedSchemaMap1.put("c1", new FieldSchema("c1", "INT", "100", null));
        scrFiledSchema.put(tab1, filedSchemaMap1);

        Map<String, FieldSchema> filedSchemaMap2 = new LinkedHashMap<>();
        filedSchemaMap2.put("id", new FieldSchema("id", "INT", "10000", null));
        filedSchemaMap2.put("c2", new FieldSchema("c2", "INT", null, null));
        filedSchemaMap2.put("c555", new FieldSchema("c555", "VARCHAR(300)", null, null));
        filedSchemaMap2.put("c666", new FieldSchema("c666", "INT", "100", null));
        filedSchemaMap2.put("c4", new FieldSchema("c4", "BIGINT", "555", null));
        filedSchemaMap2.put("c199", new FieldSchema("c199", "INT", null, null));
        filedSchemaMap2.put("c12", new FieldSchema("c12", "INT", "100", null));
        scrFiledSchema.put(tab2, filedSchemaMap2);
        return scrFiledSchema;
    }

    private Map<String, Map<String, FieldSchema>> buildOriginFiledSchema() {
        String tab1 = "db.test_fill";
        String tab2 = "test.t1";
        Map<String, Map<String, FieldSchema>> originFiledSchema = new LinkedHashMap<>();
        Map<String, FieldSchema> filedSchemaMap1 = new LinkedHashMap<>();
        filedSchemaMap1.put("id", new FieldSchema());
        filedSchemaMap1.put("name", new FieldSchema());
        filedSchemaMap1.put("test_time", new FieldSchema());
        filedSchemaMap1.put("c1", new FieldSchema());
        originFiledSchema.put(tab1, filedSchemaMap1);

        Map<String, FieldSchema> filedSchemaMap2 = new LinkedHashMap<>();
        filedSchemaMap2.put("id", new FieldSchema());
        filedSchemaMap2.put("c2", new FieldSchema());
        filedSchemaMap2.put("c555", new FieldSchema());
        filedSchemaMap2.put("c666", new FieldSchema());
        filedSchemaMap2.put("c4", new FieldSchema());
        filedSchemaMap2.put("c199", new FieldSchema());
        filedSchemaMap2.put("c12", new FieldSchema());
        originFiledSchema.put(tab2, filedSchemaMap2);
        return originFiledSchema;
    }

    @Test
    public void testBuildMysql2DorisTypeName() throws IOException {
        String columnInfo =
                "{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"10\",\"enumValues\":[]}";
        schemaChange.setSourceConnector("mysql");
        JsonNode columns = objectMapper.readTree(columnInfo);
        String dorisTypeName = schemaChange.buildDorisTypeName(columns);
        Assert.assertEquals(dorisTypeName, "INT");
    }

    @Test
    public void testBuildOracle2DorisTypeName() throws IOException {
        String columnInfo =
                "{\"name\":\"NAME\",\"jdbcType\":12,\"nativeType\":null,\"typeName\":\"VARCHAR2\",\"typeExpression\":\"VARCHAR2\",\"charsetName\":null,\"length\":128,\"scale\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null}";
        schemaChange.setSourceConnector("oracle");
        JsonNode columns = objectMapper.readTree(columnInfo);
        String dorisTypeName = schemaChange.buildDorisTypeName(columns);
        Assert.assertEquals(dorisTypeName, "VARCHAR(384)");
        columnInfo =
                "{\"name\":\"NAME\",\"jdbcType\":12,\"nativeType\":null,\"typeName\":\"FLOAT\",\"typeExpression\":\"FLOAT\",\"charsetName\":null,\"length\":0,\"scale\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null}";
        columns = objectMapper.readTree(columnInfo);
        dorisTypeName = schemaChange.buildDorisTypeName(columns);
        Assert.assertEquals(dorisTypeName, "DOUBLE");
    }

    @Test
    public void testBuildPostgres2DorisTypeName() throws IOException {
        String columnInfo =
                "{\"name\":\"NAME\",\"jdbcType\":12,\"nativeType\":null,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":null,\"length\":128,\"scale\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null}";
        schemaChange.setSourceConnector("postgres");
        JsonNode columns = objectMapper.readTree(columnInfo);
        String dorisTypeName = schemaChange.buildDorisTypeName(columns);
        Assert.assertEquals(dorisTypeName, "VARCHAR(384)");
    }

    @Test
    public void testBuildSqlserver2DorisTypeName() throws IOException {
        String columnInfo =
                "{\"name\":\"NAME\",\"jdbcType\":12,\"nativeType\":null,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":null,\"length\":128,\"scale\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null}";
        schemaChange.setSourceConnector("sqlserver");
        JsonNode columns = objectMapper.readTree(columnInfo);
        String dorisTypeName = schemaChange.buildDorisTypeName(columns);
        Assert.assertEquals(dorisTypeName, "VARCHAR(384)");
    }

    @Test
    public void testExtractDDLListRename() throws IOException {
        String columnInfo =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1698314781975,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000046\",\"pos\":5197,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000046\\\",\\\"pos\\\":5197,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1698314781,\\\"file\\\":\\\"binlog.000046\\\",\\\"pos\\\":5331,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 rename column c3 to c333\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c333\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":10,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        Map<String, Map<String, FieldSchema>> originFieldSchemaHashMap = new LinkedHashMap<>();
        Map<String, FieldSchema> fieldSchemaHashMap = Maps.newHashMap();
        JsonNode record = objectMapper.readTree(columnInfo);
        schemaChange.setSourceConnector("mysql");

        fieldSchemaHashMap.put("id", new FieldSchema("id", "INT", "", ""));
        fieldSchemaHashMap.put("c2", new FieldSchema("c2", "INT", "", ""));
        fieldSchemaHashMap.put("c3", new FieldSchema("c3", "VARCHAR(30)", "", ""));
        originFieldSchemaHashMap.put("test.t1", fieldSchemaHashMap);
        schemaChange.setOriginFieldSchemaMap(originFieldSchemaHashMap);

        List<String> ddlList = schemaChange.extractDDLList(record);
        Assert.assertEquals("ALTER TABLE `test`.`t1` RENAME COLUMN `c3` `c333`", ddlList.get(0));
    }

    @Test
    public void testExtractDDlListChangeName() throws IOException {
        String columnInfo =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1710925209991,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"mysql-bin.000288\",\"pos\":81654,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"mysql-bin.000288\\\",\\\"pos\\\":81654,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1710925209,\\\"file\\\":\\\"mysql-bin.000288\\\",\\\"pos\\\":81808,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 change age age1 int\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8\\\",\\\"primaryKeyColumnNames\\\":[\\\"name\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8\\\",\\\"length\\\":256,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":false,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"age1\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"length\\\":11,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        Map<String, Map<String, FieldSchema>> originFieldSchemaHashMap = new LinkedHashMap<>();
        Map<String, FieldSchema> fieldSchemaHashMap = Maps.newHashMap();
        JsonNode record = objectMapper.readTree(columnInfo);
        schemaChange.setSourceConnector("mysql");

        fieldSchemaHashMap.put("name", new FieldSchema("name", "VARCHAR(256)", "", ""));
        fieldSchemaHashMap.put("age", new FieldSchema("age", "INT", "", ""));
        originFieldSchemaHashMap.put("test.t1", fieldSchemaHashMap);
        schemaChange.setOriginFieldSchemaMap(originFieldSchemaHashMap);
        List<String> changeNameList = schemaChange.extractDDLList(record);
        Assert.assertEquals(
                "ALTER TABLE `test`.`t1` RENAME COLUMN `age` `age1`", changeNameList.get(0));
    }

    @Test
    public void testExtractDDlListChangeNameWithColumn() throws IOException {
        String columnInfo =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1711088321412,\"snapshot\":\"false\",\"db\":\"doris_test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"mysql-bin.000292\",\"pos\":55695,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"mysql-bin.000292\\\",\\\"pos\\\":55695,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1711088321,\\\"file\\\":\\\"mysql-bin.000292\\\",\\\"pos\\\":55891,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1\\\\n    change column `key` key_word int default 1 not null\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"length\\\":11,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":false,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"key_word\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"length\\\":11,\\\"position\\\":2,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"1\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        Map<String, Map<String, FieldSchema>> originFieldSchemaHashMap = new LinkedHashMap<>();
        Map<String, FieldSchema> fieldSchemaHashMap = Maps.newHashMap();
        JsonNode record = objectMapper.readTree(columnInfo);
        schemaChange.setSourceConnector("mysql");

        fieldSchemaHashMap.put("id", new FieldSchema("id", "int", "", ""));
        fieldSchemaHashMap.put("key", new FieldSchema("key", "int", "", ""));
        originFieldSchemaHashMap.put("test.t1", fieldSchemaHashMap);
        schemaChange.setOriginFieldSchemaMap(originFieldSchemaHashMap);
        List<String> changeNameList = schemaChange.extractDDLList(record);
        Assert.assertEquals(
                "ALTER TABLE `test`.`t1` RENAME COLUMN `key` `key_word`", changeNameList.get(0));
    }

    @Test
    public void testGetCdcTableIdentifier() throws Exception {
        String insert =
                "{\"before\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":null,\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924328000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12500,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"d\",\"ts_ms\":1663924328869,\"transaction\":null}";
        JsonNode recordRoot = objectMapper.readTree(insert);
        String identifier = schemaChange.getCdcTableIdentifier(recordRoot);
        Assert.assertEquals("test.t1", identifier);

        String insertSchema =
                "{\"before\":{\"id\":1,\"name\":\"doris-update\",\"dt\":\"2022-01-01\",\"dtime\":\"2022-01-01 10:01:02\",\"ts\":\"2022-01-01 10:01:03\"},\"after\":null,\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924328000,\"snapshot\":\"false\",\"db\":\"test\",\"schema\":\"dbo\",\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":12500,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"d\",\"ts_ms\":1663924328869,\"transaction\":null}";
        String identifierSchema =
                schemaChange.getCdcTableIdentifier(objectMapper.readTree(insertSchema));
        Assert.assertEquals("test.dbo.t1", identifierSchema);

        String ddl =
                "{\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663924503565,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000006\",\"pos\":13088,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13088,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1663924503,\\\"file\\\":\\\"binlog.000006\\\",\\\"pos\\\":13221,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 add \\\\n    c_1 varchar(200)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":128,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dt\\\",\\\"jdbcType\\\":91,\\\"typeName\\\":\\\"DATE\\\",\\\"typeExpression\\\":\\\"DATE\\\",\\\"charsetName\\\":null,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"dtime\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"ts\\\",\\\"jdbcType\\\":2014,\\\"typeName\\\":\\\"TIMESTAMP\\\",\\\"typeExpression\\\":\\\"TIMESTAMP\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false},{\\\"name\\\":\\\"c_1\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":200,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false}]}}]}\"}";
        String ddlRes = schemaChange.getCdcTableIdentifier(objectMapper.readTree(ddl));
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

    @Test
    public void testAutoCreateTable() throws Exception {
        String record =
                "{    \"source\":{        \"version\":\"1.9.7.Final\",        \"connector\":\"oracle\",        \"name\":\"oracle_logminer\",        \"ts_ms\":1696945825065,        \"snapshot\":\"true\",        \"db\":\"TESTDB\",        \"sequence\":null,        \"schema\":\"ADMIN\",        \"table\":\"PERSONS\",        \"txId\":null,        \"scn\":\"1199617\",        \"commit_scn\":null,        \"lcr_position\":null,        \"rs_id\":null,        \"ssn\":0,        \"redo_thread\":null    },    \"databaseName\":\"TESTDB\",    \"schemaName\":\"ADMIN\",    \"ddl\":\"\\n  CREATE TABLE \\\"ADMIN\\\".\\\"PERSONS\\\" \\n   (\\t\\\"ID\\\" NUMBER(10,0), \\n\\t\\\"NAME4\\\" VARCHAR2(128) NOT NULL ENABLE, \\n\\t\\\"age4\\\" VARCHAR2(128), \\n\\t PRIMARY KEY (\\\"ID\\\") ENABLE\\n   ) ;\\n \",    \"tableChanges\":[        {            \"type\":\"CREATE\",            \"id\":\"\\\"TESTDB\\\".\\\"ADMIN\\\".\\\"PERSONS\\\"\",            \"table\":{                \"defaultCharsetName\":null,                \"primaryKeyColumnNames\":[                    \"ID\"                ],                \"columns\":[                    {                        \"name\":\"ID\",                        \"jdbcType\":2,                        \"nativeType\":null,                        \"typeName\":\"NUMBER\",                        \"typeExpression\":\"NUMBER\",                        \"charsetName\":null,                        \"length\":10,                        \"scale\":0,                        \"position\":1,                        \"optional\":false,                        \"autoIncremented\":false,                        \"generated\":false,                        \"comment\":null                    },                    {                        \"name\":\"NAME4\",                        \"jdbcType\":12,                        \"nativeType\":null,                        \"typeName\":\"VARCHAR2\",                        \"typeExpression\":\"VARCHAR2\",                        \"charsetName\":null,                        \"length\":128,                        \"scale\":null,                        \"position\":2,                        \"optional\":false,                        \"autoIncremented\":false,                        \"generated\":false,                        \"comment\":null                    },                    {                        \"name\":\"age4\",                        \"jdbcType\":12,                        \"nativeType\":null,                        \"typeName\":\"VARCHAR2\",                        \"typeExpression\":\"VARCHAR2\",                        \"charsetName\":null,                        \"length\":128,                        \"scale\":null,                        \"position\":3,                        \"optional\":true,                        \"autoIncremented\":false,                        \"generated\":false,                        \"comment\":null                    }                ],                \"comment\":null            }        }    ]}";
        JsonNode recordRoot = objectMapper.readTree(record);
        schemaChange.setSourceConnector(SourceConnector.ORACLE.connectorName);
        TableSchema tableSchema = schemaChange.extractCreateTableSchema(recordRoot);
        Assert.assertEquals("TESTDB", tableSchema.getDatabase());
        Assert.assertEquals("PERSONS", tableSchema.getTable());
        Assert.assertArrayEquals(new String[] {"ID"}, tableSchema.getKeys().toArray());
        Assert.assertEquals(3, tableSchema.getFields().size());
        Assert.assertEquals("ID", tableSchema.getFields().get("ID").getName());
        Assert.assertEquals("NAME4", tableSchema.getFields().get("NAME4").getName());
        Assert.assertEquals("age4", tableSchema.getFields().get("age4").getName());
        schemaChange.setSourceConnector(SourceConnector.MYSQL.connectorName);
    }

    @Test
    public void testDateTimeFullOrigin() throws JsonProcessingException {
        Map<String, Map<String, FieldSchema>> originFieldSchemaMap = new LinkedHashMap<>();
        Map<String, FieldSchema> srcFiledSchemaMap = new LinkedHashMap<>();
        srcFiledSchemaMap.put("id", new FieldSchema("id", "INT", null, null));
        srcFiledSchemaMap.put(
                "test_dt_0", new FieldSchema("test_dt_0", "DATETIMEV2(0)", null, null));
        srcFiledSchemaMap.put(
                "test_dt_1", new FieldSchema("test_dt_1", "DATETIMEV2(1)", null, null));
        srcFiledSchemaMap.put(
                "test_dt_3", new FieldSchema("test_dt_3", "DATETIMEV2(3)", null, null));
        srcFiledSchemaMap.put(
                "test_dt_6", new FieldSchema("test_dt_6", "DATETIMEV2(6)", null, null));
        srcFiledSchemaMap.put(
                "test_ts_0", new FieldSchema("test_ts_0", "DATETIMEV2(0)", null, null));
        srcFiledSchemaMap.put(
                "test_ts_1",
                new FieldSchema("test_ts_1", "DATETIMEV2(1)", "current_timestamp", null));
        srcFiledSchemaMap.put(
                "test_ts_3",
                new FieldSchema("test_ts_3", "DATETIMEV2(3)", "current_timestamp", null));
        srcFiledSchemaMap.put(
                "test_ts_6",
                new FieldSchema("test_ts_6", "DATETIMEV2(6)", "current_timestamp", null));

        String tableName = "db.test_fill";
        originFieldSchemaMap.put(tableName, buildDatetimeFieldSchemaMap());
        schemaChange.setSourceConnector("mysql");
        schemaChange.setOriginFieldSchemaMap(originFieldSchemaMap);
        String columnsString =
                "[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":false,\"enumValues\":[]},{\"name\":\"test_dt_0\",\"jdbcType\":93,\"typeName\":\"DATETIME\",\"typeExpression\":\"DATETIME\",\"charsetName\":null,\"position\":2,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"test_dt_1\",\"jdbcType\":93,\"typeName\":\"DATETIME\",\"typeExpression\":\"DATETIME\",\"charsetName\":null,\"length\":1,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"test_dt_3\",\"jdbcType\":93,\"typeName\":\"DATETIME\",\"typeExpression\":\"DATETIME\",\"charsetName\":null,\"length\":3,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"test_dt_6\",\"jdbcType\":93,\"typeName\":\"DATETIME\",\"typeExpression\":\"DATETIME\",\"charsetName\":null,\"length\":6,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"test_ts_0\",\"jdbcType\":2014,\"typeName\":\"TIMESTAMP\",\"typeExpression\":\"TIMESTAMP\",\"charsetName\":null,\"position\":6,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"enumValues\":[]},{\"name\":\"test_ts_1\",\"jdbcType\":2014,\"typeName\":\"TIMESTAMP\",\"typeExpression\":\"TIMESTAMP\",\"charsetName\":null,\"length\":1,\"position\":7,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"1970-01-01 00:00:00\",\"enumValues\":[]},{\"name\":\"test_ts_3\",\"jdbcType\":2014,\"typeName\":\"TIMESTAMP\",\"typeExpression\":\"TIMESTAMP\",\"charsetName\":null,\"length\":3,\"position\":8,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"1970-01-01 00:00:00\",\"enumValues\":[]},{\"name\":\"test_ts_6\",\"jdbcType\":2014,\"typeName\":\"TIMESTAMP\",\"typeExpression\":\"TIMESTAMP\",\"charsetName\":null,\"length\":6,\"position\":9,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"hasDefaultValue\":true,\"defaultValueExpression\":\"1970-01-01 00:00:00\",\"enumValues\":[]}]},\"comment\":null}]}";
        JsonNode columns = objectMapper.readTree(columnsString);
        schemaChange.fillOriginSchema(tableName, columns);
        Map<String, Map<String, FieldSchema>> targetOriginFieldSchemaMap =
                schemaChange.getOriginFieldSchemaMap();
        Map<String, FieldSchema> fieldSchemaMap = targetOriginFieldSchemaMap.get(tableName);

        eqFiledSchema(fieldSchemaMap, srcFiledSchemaMap);
    }

    private Map<String, FieldSchema> buildDatetimeFieldSchemaMap() {
        Map<String, FieldSchema> filedSchemaMap = new LinkedHashMap<>();
        filedSchemaMap.put("id", new FieldSchema());
        filedSchemaMap.put("test_dt_0", new FieldSchema());
        filedSchemaMap.put("test_dt_1", new FieldSchema());
        filedSchemaMap.put("test_dt_3", new FieldSchema());
        filedSchemaMap.put("test_dt_6", new FieldSchema());
        filedSchemaMap.put("test_ts_0", new FieldSchema());
        filedSchemaMap.put("test_ts_1", new FieldSchema());
        filedSchemaMap.put("test_ts_3", new FieldSchema());
        filedSchemaMap.put("test_ts_6", new FieldSchema());
        return filedSchemaMap;
    }

    @Test
    public void buildFieldSchemaTest() {
        Map<String, FieldSchema> result = new HashMap<>();
        String columnInfo =
                "{\"name\":\"order_ts\",\"jdbcType\":2014,\"typeName\":\"TIMESTAMP\",\"typeExpression\":\"TIMESTAMP\",\"charsetName\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":\"中文注释\",\"hasDefaultValue\":true,\"defaultValueExpression\":\"1970-01-01 00:00:00\",\"enumValues\":[]}\n";
        schemaChange.setSourceConnector("mysql");
        JsonNode columns = null;
        try {
            columns = objectMapper.readTree(columnInfo);
        } catch (IOException e) {
            e.printStackTrace();
        }
        schemaChange.buildFieldSchema(result, columns);
        Assert.assertTrue(result.containsKey("order_ts"));
        FieldSchema fieldSchema = result.get("order_ts");
        Assert.assertEquals(fieldSchema.getName().toLowerCase(), "order_ts");
        Assert.assertEquals(fieldSchema.getTypeString().toLowerCase(), "datetimev2(0)");
        Assert.assertEquals(fieldSchema.getDefaultValue().toLowerCase(), "current_timestamp");
        Assert.assertEquals(fieldSchema.getComment(), "中文注释");

        columnInfo =
                "{\"name\":\"other_no\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":50,\"position\":23,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":\"comment\",\"hasDefaultValue\":true,\"defaultValueExpression\":\"\",\"enumValues\":[]}\n";
        schemaChange.setSourceConnector("mysql");
        columns = null;
        try {
            columns = objectMapper.readTree(columnInfo);
        } catch (IOException e) {
            e.printStackTrace();
        }
        schemaChange.buildFieldSchema(result, columns);
        Assert.assertTrue(result.containsKey("other_no"));
        fieldSchema = result.get("other_no");
        Assert.assertEquals(fieldSchema.getName().toLowerCase(), "other_no");
        Assert.assertEquals(fieldSchema.getTypeString().toLowerCase(), "varchar(150)");
        Assert.assertEquals(fieldSchema.getDefaultValue().toLowerCase(), "");
        Assert.assertEquals(fieldSchema.getComment(), "comment");
    }

    @After
    public void after() {
        mockRestService.close();
    }
}
