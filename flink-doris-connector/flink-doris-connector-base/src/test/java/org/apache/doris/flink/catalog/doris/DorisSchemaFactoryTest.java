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

package org.apache.doris.flink.catalog.doris;

import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DorisSchemaFactoryTest {

    @Test
    public void testParseTableSchemaBuckets() {
        Assert.assertNull(DorisSchemaFactory.parseTableSchemaBuckets(null, null));
        Map<String, Integer> map = new HashMap<>();
        Assert.assertNull(DorisSchemaFactory.parseTableSchemaBuckets(map, null));
        map.put("tbl1", 1);
        Assert.assertEquals(DorisSchemaFactory.parseTableSchemaBuckets(map, "tbl1").intValue(), 1);
        map = new HashMap<>();
        map.put("tbl2.*", 1);
        Assert.assertEquals(DorisSchemaFactory.parseTableSchemaBuckets(map, "tbl2").intValue(), 1);
        Assert.assertEquals(DorisSchemaFactory.parseTableSchemaBuckets(map, "tbl21").intValue(), 1);
    }

    @Test
    public void testCreateTableSchema() {
        TableSchema tableSchema = buildCreateTableSchema();
        Assert.assertEquals(
                "TableSchema{database='doris', table='create_tab', tableComment='auto_tab_comment', fields={name=FieldSchema{name='name', typeString='VARVHAR(100)', defaultValue='null', comment='Name_test'}, id=FieldSchema{name='id', typeString='INT', defaultValue='100', comment='int_test'}, age=FieldSchema{name='age', typeString='INT', defaultValue='null', comment='null'}, email=FieldSchema{name='email', typeString='VARCHAR(100)', defaultValue='email@doris.com', comment='e'}}, keys=email, model=UNIQUE, distributeKeys=email, properties={light_schema_change=true}, tableBuckets=null}",
                tableSchema.toString());
    }

    private TableSchema buildCreateTableSchema() {
        String dorisTable = "doris.create_tab";
        String[] dbTable = dorisTable.split("\\.");
        Preconditions.checkArgument(dbTable.length == 2);

        Map<String, FieldSchema> columnFields = new HashMap<>();
        columnFields.put("id", new FieldSchema("id", "INT", "100", "int_test"));
        columnFields.put("name", new FieldSchema("name", "VARVHAR(100)", null, "Name_test"));
        columnFields.put("age", new FieldSchema("age", "INT", null, null));
        columnFields.put("email", new FieldSchema("email", "VARCHAR(100)", "email@doris.com", "e"));
        List<String> pkKeys = Collections.singletonList("email");
        Map<String, String> tableProperties = new HashMap<>();
        String tableComment = "auto_tab_comment";
        return DorisSchemaFactory.createTableSchema(
                dbTable[0],
                dbTable[1],
                columnFields,
                pkKeys,
                new DorisTableConfig(tableProperties),
                tableComment);
    }

    @Test
    public void testCreateTableSchemaTableBuckets() {
        TableSchema tableSchema = buildCreateTableSchemaTableBuckets();
        Assert.assertEquals(
                "TableSchema{database='doris', table='create_tab', tableComment='auto_tab_comment', fields={name=FieldSchema{name='name', typeString='VARVHAR(100)', defaultValue='null', comment='Name_test'}, id=FieldSchema{name='id', typeString='INT', defaultValue='100', comment='int_test'}, age=FieldSchema{name='age', typeString='INT', defaultValue='null', comment='null'}, email=FieldSchema{name='email', typeString='VARCHAR(100)', defaultValue='email@doris.com', comment='e'}}, keys=email, model=UNIQUE, distributeKeys=email, properties={replication_num=2, light_schema_change=true}, tableBuckets=40}",
                tableSchema.toString());
    }

    private TableSchema buildCreateTableSchemaTableBuckets() {
        String dorisTable = "doris.create_tab";
        String[] dbTable = dorisTable.split("\\.");
        Preconditions.checkArgument(dbTable.length == 2);

        Map<String, FieldSchema> columnFields = new HashMap<>();
        columnFields.put("id", new FieldSchema("id", "INT", "100", "int_test"));
        columnFields.put("name", new FieldSchema("name", "VARVHAR(100)", null, "Name_test"));
        columnFields.put("age", new FieldSchema("age", "INT", null, null));
        columnFields.put("email", new FieldSchema("email", "VARCHAR(100)", "email@doris.com", "e"));
        List<String> pkKeys = Collections.singletonList("email");
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("table-buckets", "create_tab:40, create_taba:10, tabs:12");
        tableProperties.put("replication_num", "2");
        String tableComment = "auto_tab_comment";
        return DorisSchemaFactory.createTableSchema(
                dbTable[0],
                dbTable[1],
                columnFields,
                pkKeys,
                new DorisTableConfig(tableProperties),
                tableComment);
    }

    @Test
    public void testCreateDuplicateTableSchema() {
        TableSchema tableSchema = buildCreateDuplicateTableSchema();
        Assert.assertEquals(
                "TableSchema{database='doris', table='dup_tab', tableComment='auto_tab_comment', fields={name=FieldSchema{name='name', typeString='VARVHAR(100)', defaultValue='null', comment='Name_test'}, id=FieldSchema{name='id', typeString='INT', defaultValue='100', comment='int_test'}, age=FieldSchema{name='age', typeString='INT', defaultValue='null', comment='null'}, email=FieldSchema{name='email', typeString='VARCHAR(100)', defaultValue='email@doris.com', comment='e'}}, keys=name, model=DUPLICATE, distributeKeys=name, properties={replication_num=1, light_schema_change=true}, tableBuckets=null}",
                tableSchema.toString());
    }

    private TableSchema buildCreateDuplicateTableSchema() {
        String dorisTable = "doris.dup_tab";
        String[] dbTable = dorisTable.split("\\.");
        Preconditions.checkArgument(dbTable.length == 2);

        Map<String, FieldSchema> columnFields = new HashMap<>();
        columnFields.put("id", new FieldSchema("id", "INT", "100", "int_test"));
        columnFields.put("name", new FieldSchema("name", "VARVHAR(100)", null, "Name_test"));
        columnFields.put("age", new FieldSchema("age", "INT", null, null));
        columnFields.put("email", new FieldSchema("email", "VARCHAR(100)", "email@doris.com", "e"));
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("replication_num", "1");
        String tableComment = "auto_tab_comment";
        return DorisSchemaFactory.createTableSchema(
                dbTable[0],
                dbTable[1],
                columnFields,
                new ArrayList<>(),
                new DorisTableConfig(tableProperties),
                tableComment);
    }

    @Test
    public void testGenerateCreateTableDDL() {
        TableSchema tableSchema = buildCreateTableSchema();
        String ddl = DorisSchemaFactory.generateCreateTableDDL(tableSchema);
        Assert.assertEquals(
                "CREATE TABLE IF NOT EXISTS `doris`.`create_tab`(`email` VARCHAR(100) DEFAULT 'email@doris.com' COMMENT 'e',`name` VARVHAR(100) COMMENT 'Name_test',`id` INT DEFAULT '100' COMMENT 'int_test',`age` INT COMMENT '' ) UNIQUE KEY(`email`) COMMENT 'auto_tab_comment'  DISTRIBUTED BY HASH(`email`) BUCKETS AUTO  PROPERTIES ('light_schema_change'='true')",
                ddl);
    }

    @Test
    public void testGenerateCreateTableDDLBuckets() {
        TableSchema tableSchema = buildCreateTableSchemaTableBuckets();
        String ddl = DorisSchemaFactory.generateCreateTableDDL(tableSchema);
        Assert.assertEquals(
                "CREATE TABLE IF NOT EXISTS `doris`.`create_tab`(`email` VARCHAR(100) DEFAULT 'email@doris.com' COMMENT 'e',`name` VARVHAR(100) COMMENT 'Name_test',`id` INT DEFAULT '100' COMMENT 'int_test',`age` INT COMMENT '' ) UNIQUE KEY(`email`) COMMENT 'auto_tab_comment'  DISTRIBUTED BY HASH(`email`) BUCKETS 40 PROPERTIES ('replication_num'='2','light_schema_change'='true')",
                ddl);
    }

    @Test
    public void testGenerateCreateTableDDLDuplicate() {
        TableSchema tableSchema = buildCreateDuplicateTableSchema();
        String ddl = DorisSchemaFactory.generateCreateTableDDL(tableSchema);
        Assert.assertEquals(
                "CREATE TABLE IF NOT EXISTS `doris`.`dup_tab`(`name` VARVHAR(100) COMMENT 'Name_test',`id` INT DEFAULT '100' COMMENT 'int_test',`age` INT COMMENT '',`email` VARCHAR(100) DEFAULT 'email@doris.com' COMMENT 'e' )  COMMENT 'auto_tab_comment'  DISTRIBUTED BY HASH(`name`) BUCKETS AUTO  PROPERTIES ('replication_num'='1','light_schema_change'='true')",
                ddl);
    }

    private TableSchema buildPartitionTableSchema(List<String> pkKeys) {
        String dorisTable = "doris.part_tab";
        String[] dbTable = dorisTable.split("\\.");
        Preconditions.checkArgument(dbTable.length == 2);

        // field order is fixed by LinkedHashMap so that the generated DDL is deterministic
        Map<String, FieldSchema> columnFields = new LinkedHashMap<>();
        columnFields.put("dt", new FieldSchema("dt", "DATE", null, "dt_cmt"));
        columnFields.put("id", new FieldSchema("id", "INT", "100", "int_test"));
        columnFields.put("name", new FieldSchema("name", "VARVHAR(100)", null, "Name_test"));
        columnFields.put("age", new FieldSchema("age", "INT", null, null));
        columnFields.put("email", new FieldSchema("email", "VARCHAR(100)", "email@doris.com", "e"));
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("table-partitions", "part_tab:dt:day");
        return DorisSchemaFactory.createTableSchema(
                dbTable[0],
                dbTable[1],
                columnFields,
                pkKeys,
                new DorisTableConfig(tableProperties),
                "auto_tab_comment");
    }

    /** The auto partition column is one of the primary keys, it must be declared exactly once. */
    @Test
    public void testGenerateCreateTableDDLPartitionColumnInPrimaryKeys() {
        TableSchema tableSchema = buildPartitionTableSchema(Arrays.asList("dt", "id"));
        String ddl = DorisSchemaFactory.generateCreateTableDDL(tableSchema);
        Assert.assertEquals(
                "CREATE TABLE IF NOT EXISTS `doris`.`part_tab`(`dt` DATE NOT NULL  COMMENT 'dt_cmt',`id` INT DEFAULT '100' COMMENT 'int_test',`name` VARVHAR(100) COMMENT 'Name_test',`age` INT COMMENT '',`email` VARCHAR(100) DEFAULT 'email@doris.com' COMMENT 'e' ) UNIQUE KEY(`dt`,`id`) COMMENT 'auto_tab_comment'  AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))() DISTRIBUTED BY HASH(`dt`,`id`) BUCKETS AUTO  PROPERTIES ('light_schema_change'='true')",
                ddl);
        Assert.assertEquals(1, countOccurrences(ddl, "`dt` DATE NOT NULL"));
        Assert.assertEquals(0, countOccurrences(ddl, "`dt` DATE COMMENT"));
        // column order: keys first (partition column in its key position), then the value fields
        Assert.assertTrue(ddl.indexOf("`dt` DATE") < ddl.indexOf("`id` INT"));
        Assert.assertTrue(ddl.indexOf("`id` INT") < ddl.indexOf("`name` "));
        Assert.assertTrue(ddl.indexOf("`name` ") < ddl.indexOf("`age` INT"));
        Assert.assertTrue(ddl.indexOf("`age` INT") < ddl.indexOf("`email` VARCHAR(100)"));
        Assert.assertEquals("`dt`,`id`", uniqueKeyClause(ddl));
    }

    /** The auto partition column is not a primary key, it must still be added exactly once. */
    @Test
    public void testGenerateCreateTableDDLPartitionColumnOutsideKeys() {
        TableSchema tableSchema = buildPartitionTableSchema(Arrays.asList("id"));
        String ddl = DorisSchemaFactory.generateCreateTableDDL(tableSchema);
        Assert.assertEquals(
                "CREATE TABLE IF NOT EXISTS `doris`.`part_tab`(`id` INT DEFAULT '100' COMMENT 'int_test',`dt` DATE NOT NULL  COMMENT 'dt_cmt',`name` VARVHAR(100) COMMENT 'Name_test',`age` INT COMMENT '',`email` VARCHAR(100) DEFAULT 'email@doris.com' COMMENT 'e' ) UNIQUE KEY(`id`,`dt`) COMMENT 'auto_tab_comment'  AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))() DISTRIBUTED BY HASH(`id`) BUCKETS AUTO  PROPERTIES ('light_schema_change'='true')",
                ddl);
        Assert.assertEquals(1, countOccurrences(ddl, "`dt` DATE NOT NULL"));
        Assert.assertEquals("`id`,`dt`", uniqueKeyClause(ddl));
    }

    /** Generating the DDL must not mutate the schema or the caller's key list. */
    @Test
    public void testGenerateCreateTableDDLDoesNotMutateSchema() {
        List<String> pkKeys = Arrays.asList("dt", "id");
        TableSchema tableSchema = buildPartitionTableSchema(pkKeys);
        Map<String, FieldSchema> fieldsBefore = tableSchema.getFields();
        List<String> fieldsOrderBefore = new ArrayList<>(tableSchema.getFields().keySet());
        Map<String, String> propertiesBefore = new HashMap<>(tableSchema.getProperties());

        String first = DorisSchemaFactory.generateCreateTableDDL(tableSchema);
        String second = DorisSchemaFactory.generateCreateTableDDL(tableSchema);

        Assert.assertEquals(first, second);
        Assert.assertEquals(Arrays.asList("dt", "id"), pkKeys);
        Assert.assertEquals(Arrays.asList("dt", "id"), tableSchema.getKeys());
        Assert.assertEquals(Arrays.asList("dt", "id"), tableSchema.getDistributeKeys());
        Assert.assertEquals(fieldsOrderBefore, new ArrayList<>(tableSchema.getFields().keySet()));
        Assert.assertEquals(fieldsBefore, tableSchema.getFields());
        Assert.assertEquals(propertiesBefore, tableSchema.getProperties());
        Assert.assertEquals("dt", tableSchema.getPartitionInfo().f0);
        Assert.assertEquals("day", tableSchema.getPartitionInfo().f1);
    }

    private static String uniqueKeyClause(String ddl) {
        int start = ddl.indexOf("UNIQUE KEY(");
        Assert.assertTrue("no UNIQUE KEY clause in ddl: " + ddl, start >= 0);
        start += "UNIQUE KEY(".length();
        int end = ddl.indexOf(")", start);
        Assert.assertTrue("no closing parenthesis for UNIQUE KEY in ddl: " + ddl, end > start);
        return ddl.substring(start, end);
    }

    private static int countOccurrences(String text, String token) {
        int count = 0;
        int index = text.indexOf(token);
        while (index >= 0) {
            count++;
            index = text.indexOf(token, index + token.length());
        }
        return count;
    }

    @Test
    public void quoteTableIdentifier() {
        String quoted = DorisSchemaFactory.quoteTableIdentifier("db.tbl");
        Assert.assertEquals("`db`.`tbl`", quoted);
    }

    @Test(expected = IllegalArgumentException.class)
    public void quoteTableIdentifierException() {
        DorisSchemaFactory.quoteTableIdentifier("db.tbl.sc");
    }
}
