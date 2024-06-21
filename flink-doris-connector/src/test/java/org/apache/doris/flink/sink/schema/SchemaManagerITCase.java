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

package org.apache.doris.flink.sink.schema;

import org.apache.doris.flink.DorisTestBase;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.rest.models.Field;
import org.apache.doris.flink.rest.models.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

public class SchemaManagerITCase extends DorisTestBase {

    private static final String DATABASE = "test_sc_db";
    private DorisOptions options;
    private SchemaChangeManager schemaChangeManager;

    @Before
    public void setUp() throws Exception {
        options =
                DorisOptions.builder()
                        .setFenodes(getFenodes())
                        .setTableIdentifier(DATABASE + ".add_column")
                        .setUsername(USERNAME)
                        .setPassword(PASSWORD)
                        .build();
        schemaChangeManager = new SchemaChangeManager(options);
    }

    private void initDorisSchemaChangeTable(String table) throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(
                                String.format(URL, DORIS_CONTAINER.getHost()), USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE));
            statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table));
            statement.execute(
                    String.format(
                            "CREATE TABLE %s.%s ( \n"
                                    + "`id` varchar(32),\n"
                                    + "`age` int\n"
                                    + ") DISTRIBUTED BY HASH(`id`) BUCKETS 1\n"
                                    + "PROPERTIES (\n"
                                    + "\"replication_num\" = \"1\"\n"
                                    + ")\n",
                            DATABASE, table));
        }
    }

    @Test
    public void testAddColumn() throws SQLException, IOException, IllegalArgumentException {
        String addColumnTbls = "add_column";
        initDorisSchemaChangeTable(addColumnTbls);
        FieldSchema field = new FieldSchema("c1", "int", "");
        schemaChangeManager.addColumn(DATABASE, addColumnTbls, field);
        boolean exists = schemaChangeManager.addColumn(DATABASE, addColumnTbls, field);
        Assert.assertTrue(exists);

        exists = schemaChangeManager.checkColumnExists(DATABASE, addColumnTbls, "c1");
        Assert.assertTrue(exists);
    }

    @Test
    public void testAddColumnWithChineseComment()
            throws SQLException, IOException, IllegalArgumentException, InterruptedException {
        String addColumnTbls = "add_column";
        initDorisSchemaChangeTable(addColumnTbls);

        // add a column by UTF-8 encoding
        String addColumnName = "col_with_comment1";
        String chineseComment = "中文注释1";
        addColumnWithChineseCommentAndAssert(addColumnTbls, addColumnName, chineseComment, true);

        // change charset encoding to US-ASCII would cause garbled of Chinese.
        schemaChangeManager = new SchemaChangeManager(options, "US-ASCII");
        addColumnName = "col_with_comment2";
        chineseComment = "中文注释2";
        addColumnWithChineseCommentAndAssert(addColumnTbls, addColumnName, chineseComment, false);
    }

    private void addColumnWithChineseCommentAndAssert(
            String tableName, String addColumnName, String chineseComment, boolean assertFlag)
            throws IOException, IllegalArgumentException, InterruptedException {
        FieldSchema field = new FieldSchema(addColumnName, "string", chineseComment);
        schemaChangeManager.addColumn(DATABASE, tableName, field);
        boolean exists = schemaChangeManager.addColumn(DATABASE, tableName, field);
        Assert.assertTrue(exists);

        exists = schemaChangeManager.checkColumnExists(DATABASE, tableName, addColumnName);
        Assert.assertTrue(exists);

        // check Chinese comment
        Thread.sleep(3_000);
        String comment = getColumnComment(tableName, addColumnName);
        if (assertFlag) {
            Assert.assertEquals(comment, chineseComment);
        } else {
            Assert.assertNotEquals(comment, chineseComment);
        }
    }

    private String getColumnComment(String table, String columnName) {
        Schema schema = schemaChangeManager.getTableSchema(DATABASE, table);
        Optional<Field> first =
                schema.getProperties().stream()
                        .filter(col -> col.getName().equals(columnName))
                        .findFirst();
        return first.map(Field::getComment).orElse(null);
    }

    private String getColumnType(String table, String columnName) {
        Schema schema = schemaChangeManager.getTableSchema(DATABASE, table);
        Optional<Field> first =
                schema.getProperties().stream()
                        .filter(col -> col.getName().equals(columnName))
                        .findFirst();
        return first.map(Field::getType).orElse(null);
    }

    @Test
    public void testDropColumn() throws SQLException, IOException, IllegalArgumentException {
        String dropColumnTbls = "drop_column";
        initDorisSchemaChangeTable(dropColumnTbls);
        schemaChangeManager.dropColumn(DATABASE, dropColumnTbls, "age");
        boolean success = schemaChangeManager.dropColumn(DATABASE, dropColumnTbls, "age");
        Assert.assertTrue(success);

        boolean exists = schemaChangeManager.checkColumnExists(DATABASE, dropColumnTbls, "age");
        Assert.assertFalse(exists);
    }

    @Test
    public void testRenameColumn() throws SQLException, IOException, IllegalArgumentException {
        String renameColumnTbls = "rename_column";
        initDorisSchemaChangeTable(renameColumnTbls);
        schemaChangeManager.renameColumn(DATABASE, renameColumnTbls, "age", "age1");
        boolean exists = schemaChangeManager.checkColumnExists(DATABASE, renameColumnTbls, "age1");
        Assert.assertTrue(exists);

        exists = schemaChangeManager.checkColumnExists(DATABASE, renameColumnTbls, "age");
        Assert.assertFalse(exists);
    }

    @Test
    public void testModifyColumnComment()
            throws SQLException, IOException, IllegalArgumentException {
        String modifyColumnCommentTbls = "modify_column_comment";
        initDorisSchemaChangeTable(modifyColumnCommentTbls);
        String columnName = "age";
        String newComment = "new comment of age";
        schemaChangeManager.modifyColumnComment(
                DATABASE, modifyColumnCommentTbls, columnName, newComment);

        String comment = getColumnComment(modifyColumnCommentTbls, columnName);
        Assert.assertEquals(newComment, comment);
    }

    @Test
    public void testOnlyModifyColumnType()
            throws SQLException, IOException, IllegalArgumentException, InterruptedException {
        String modifyColumnTbls = "modify_column_type";
        String columnName = "age";
        String newColumnType = "bigint";
        initDorisSchemaChangeTable(modifyColumnTbls);
        FieldSchema field = new FieldSchema(columnName, newColumnType, "");
        schemaChangeManager.modifyColumnDataType(DATABASE, modifyColumnTbls, field);

        Thread.sleep(3_000);
        String columnType = getColumnType(modifyColumnTbls, columnName);
        Assert.assertEquals(newColumnType, columnType.toLowerCase());
    }

    @Test
    public void testModifyColumnTypeAndComment()
            throws SQLException, IOException, IllegalArgumentException, InterruptedException {
        String modifyColumnTbls = "modify_column_type_and_comment";
        initDorisSchemaChangeTable(modifyColumnTbls);
        String columnName = "age";
        String newColumnType = "bigint";
        String newComment = "new comment of age";
        FieldSchema field = new FieldSchema(columnName, newColumnType, newComment);
        schemaChangeManager.modifyColumnDataType(DATABASE, modifyColumnTbls, field);

        Thread.sleep(3_000);
        String comment = getColumnComment(modifyColumnTbls, columnName);
        Assert.assertEquals(newComment, comment);

        String columnType = getColumnType(modifyColumnTbls, columnName);
        Assert.assertEquals(newColumnType, columnType.toLowerCase());
    }

    @Test
    public void testCreateTableWhenDatabaseNotExists()
            throws IOException, IllegalArgumentException, InterruptedException {
        String databaseName = DATABASE + "_" + Integer.toUnsignedString(new Random().nextInt(), 36);
        String tableName = "auto_create_database";

        TableSchema tableSchema = new TableSchema();
        tableSchema.setDatabase(databaseName);
        tableSchema.setTable(tableName);
        Map<String, FieldSchema> fields = new HashMap<>();
        fields.put("id", new FieldSchema("id", "varchar(32)", ""));
        fields.put("age", new FieldSchema("age", "int", ""));
        tableSchema.setFields(fields);
        tableSchema.setDistributeKeys(Collections.singletonList("id"));
        tableSchema.setModel(DataModel.DUPLICATE);
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("replication_num", "1");
        tableSchema.setProperties(tableProperties);
        schemaChangeManager.createTable(tableSchema);

        Thread.sleep(3_000);
        Assert.assertNotNull(schemaChangeManager.getTableSchema(databaseName, tableName));
    }
}
