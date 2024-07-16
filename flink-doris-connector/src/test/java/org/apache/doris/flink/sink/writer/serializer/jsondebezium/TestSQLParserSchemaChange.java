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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestSQLParserSchemaChange extends TestJsonDebeziumChangeBase {

    private SQLParserSchemaChange schemaChange;

    @Before
    public void setUp() {
        super.setUp();
        JsonDebeziumChangeContext changeContext =
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
                        "");
        schemaChange = new SQLParserSchemaChange(changeContext);
    }

    @Test
    public void testExtractDDLListMultipleColumns() throws IOException {
        String sql0 = "ALTER TABLE `test`.`t1` DROP COLUMN `c11`";
        String sql1 = "ALTER TABLE `test`.`t1` DROP COLUMN `c3`";
        String sql2 = "ALTER TABLE `test`.`t1` ADD COLUMN `c12` INT DEFAULT '100'";
        List<String> srcSqlList = Arrays.asList(sql0, sql1, sql2);

        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1691033764674,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000029\",\"pos\":23305,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23305,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1691033764,\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23464,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 drop c11, drop column c3, add c12 int default 100\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c555\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c666\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c4\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"555\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c199\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c12\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":7,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = schemaChange.tryParserAlterDDLs(recordRoot);
        for (int i = 0; i < ddlSQLList.size(); i++) {
            String srcSQL = srcSqlList.get(i);
            String targetSQL = ddlSQLList.get(i);
            Assert.assertEquals(srcSQL, targetSQL);
        }
    }

    @Test
    public void testExtractDDLListChangeColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1696945030603,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000043\",\"pos\":6521,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6521,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1696945030,\\\"file\\\":\\\"binlog.000043\\\",\\\"pos\\\":6661,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table test_sink change column c555 c777 bigint\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"test_sink\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c777\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = schemaChange.tryParserAlterDDLs(recordRoot);

        String result = "ALTER TABLE `test`.`t1` RENAME COLUMN `c555` `c777`";
        Assert.assertEquals(result, ddlSQLList.get(0));
    }

    @Test
    public void testExtractDDLListRenameColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1691034519226,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000029\",\"pos\":23752,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23752,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1691034519,\\\"file\\\":\\\"binlog.000029\\\",\\\"pos\\\":23886,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 rename column c22 to c33\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10000\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c2\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c555\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":100,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c666\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c4\\\",\\\"jdbcType\\\":-5,\\\"typeName\\\":\\\"BIGINT\\\",\\\"typeExpression\\\":\\\"BIGINT\\\",\\\"charsetName\\\":null,\\\"position\\\":5,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"555\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c199\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":6,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"c33\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":7,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"100\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordRoot = objectMapper.readTree(record);
        List<String> ddlSQLList = schemaChange.tryParserAlterDDLs(recordRoot);
        Assert.assertEquals("ALTER TABLE `test`.`t1` RENAME COLUMN `c22` `c33`", ddlSQLList.get(0));
    }

    @Test
    public void testExtractDDlListChangeName() throws IOException {
        String columnInfo =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1710925209991,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"mysql-bin.000288\",\"pos\":81654,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"mysql-bin.000288\\\",\\\"pos\\\":81654,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1710925209,\\\"file\\\":\\\"mysql-bin.000288\\\",\\\"pos\\\":81808,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1 change age age1 int\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8\\\",\\\"primaryKeyColumnNames\\\":[\\\"name\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8\\\",\\\"length\\\":256,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":false,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"age1\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"length\\\":11,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode record = objectMapper.readTree(columnInfo);
        List<String> changeNameList = schemaChange.tryParserAlterDDLs(record);
        Assert.assertEquals(
                "ALTER TABLE `test`.`t1` RENAME COLUMN `age` `age1`", changeNameList.get(0));
    }

    @Test
    public void testExtractDDlListChangeNameWithColumn() throws IOException {
        String columnInfo =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1711088321412,\"snapshot\":\"false\",\"db\":\"doris_test\",\"sequence\":null,\"table\":\"t1\",\"server_id\":1,\"gtid\":null,\"file\":\"mysql-bin.000292\",\"pos\":55695,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"mysql-bin.000292\\\",\\\"pos\\\":55695,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1711088321,\\\"file\\\":\\\"mysql-bin.000292\\\",\\\"pos\\\":55891,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table t1\\\\n    change column `key` key_word int default 1 not null\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"t1\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"length\\\":11,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":false,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"key_word\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"length\\\":11,\\\"position\\\":2,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"1\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode record = objectMapper.readTree(columnInfo);
        List<String> changeNameList = schemaChange.tryParserAlterDDLs(record);
        Assert.assertEquals(
                "ALTER TABLE `test`.`t1` RENAME COLUMN `key` `key_word`", changeNameList.get(0));
    }

    @Test
    public void testAddDatetimeColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1720596740767,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink34\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000065\",\"pos\":10192,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000065\\\",\\\"pos\\\":10192,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1720596740,\\\"file\\\":\\\"binlog.000065\\\",\\\"pos\\\":10405,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"alter table test_sink34 add column `create_time` datetime(6) DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'datatime_test'\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"test_sink34\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":50,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"decimal_type\\\",\\\"jdbcType\\\":3,\\\"typeName\\\":\\\"DECIMAL\\\",\\\"typeExpression\\\":\\\"DECIMAL\\\",\\\"charsetName\\\":null,\\\"length\\\":38,\\\"scale\\\":9,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"0.123456789\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"create_time\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"length\\\":6,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"1970-01-01 00:00:00\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordJsonNode = objectMapper.readTree(record);
        List<String> changeNameList = schemaChange.tryParserAlterDDLs(recordJsonNode);
        Assert.assertEquals(
                "ALTER TABLE `test`.`t1` ADD COLUMN `create_time` DATETIMEV2(6) DEFAULT CURRENT_TIMESTAMP COMMENT 'datatime_test'",
                changeNameList.get(0));
    }

    @Test
    public void testDropColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1720599133910,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink34\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000065\",\"pos\":12084,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000065\\\",\\\"pos\\\":12084,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1720599133,\\\"file\\\":\\\"binlog.000065\\\",\\\"pos\\\":12219,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"ALTER TABLE test_sink34 drop column create_time\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"test_sink34\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":50,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"decimal_type\\\",\\\"jdbcType\\\":3,\\\"typeName\\\":\\\"DECIMAL\\\",\\\"typeExpression\\\":\\\"DECIMAL\\\",\\\"charsetName\\\":null,\\\"length\\\":38,\\\"scale\\\":9,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"0.123456789\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordJsonNode = objectMapper.readTree(record);
        List<String> changeNameList = schemaChange.tryParserAlterDDLs(recordJsonNode);
        Assert.assertEquals(
                "ALTER TABLE `test`.`t1` DROP COLUMN `create_time`", changeNameList.get(0));
    }

    @Test
    public void testChangeColumn() throws IOException {
        String record =
                "{\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1720598926291,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink34\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000065\",\"pos\":11804,\"row\":0,\"thread\":null,\"query\":null},\"historyRecord\":\"{\\\"source\\\":{\\\"file\\\":\\\"binlog.000065\\\",\\\"pos\\\":11804,\\\"server_id\\\":1},\\\"position\\\":{\\\"transaction_id\\\":null,\\\"ts_sec\\\":1720598926,\\\"file\\\":\\\"binlog.000065\\\",\\\"pos\\\":12007,\\\"server_id\\\":1},\\\"databaseName\\\":\\\"test\\\",\\\"ddl\\\":\\\"ALTER TABLE test_sink34 CHANGE COLUMN `create_time2` `create_time` datetime(6) DEFAULT CURRENT_TIMESTAMP(6)\\\",\\\"tableChanges\\\":[{\\\"type\\\":\\\"ALTER\\\",\\\"id\\\":\\\"\\\\\\\"test\\\\\\\".\\\\\\\"test_sink34\\\\\\\"\\\",\\\"table\\\":{\\\"defaultCharsetName\\\":\\\"utf8mb4\\\",\\\"primaryKeyColumnNames\\\":[\\\"id\\\"],\\\"columns\\\":[{\\\"name\\\":\\\"id\\\",\\\"jdbcType\\\":4,\\\"typeName\\\":\\\"INT\\\",\\\"typeExpression\\\":\\\"INT\\\",\\\"charsetName\\\":null,\\\"position\\\":1,\\\"optional\\\":false,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"10\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"name\\\",\\\"jdbcType\\\":12,\\\"typeName\\\":\\\"VARCHAR\\\",\\\"typeExpression\\\":\\\"VARCHAR\\\",\\\"charsetName\\\":\\\"utf8mb4\\\",\\\"length\\\":50,\\\"position\\\":2,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"enumValues\\\":[]},{\\\"name\\\":\\\"decimal_type\\\",\\\"jdbcType\\\":3,\\\"typeName\\\":\\\"DECIMAL\\\",\\\"typeExpression\\\":\\\"DECIMAL\\\",\\\"charsetName\\\":null,\\\"length\\\":38,\\\"scale\\\":9,\\\"position\\\":3,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"0.123456789\\\",\\\"enumValues\\\":[]},{\\\"name\\\":\\\"create_time\\\",\\\"jdbcType\\\":93,\\\"typeName\\\":\\\"DATETIME\\\",\\\"typeExpression\\\":\\\"DATETIME\\\",\\\"charsetName\\\":null,\\\"length\\\":6,\\\"position\\\":4,\\\"optional\\\":true,\\\"autoIncremented\\\":false,\\\"generated\\\":false,\\\"comment\\\":null,\\\"hasDefaultValue\\\":true,\\\"defaultValueExpression\\\":\\\"1970-01-01 00:00:00\\\",\\\"enumValues\\\":[]}]},\\\"comment\\\":null}]}\"}";
        JsonNode recordJsonNode = objectMapper.readTree(record);
        List<String> changeNameList = schemaChange.tryParserAlterDDLs(recordJsonNode);
        Assert.assertEquals(
                "ALTER TABLE `test`.`t1` RENAME COLUMN `create_time2` `create_time`",
                changeNameList.get(0));
    }
}
