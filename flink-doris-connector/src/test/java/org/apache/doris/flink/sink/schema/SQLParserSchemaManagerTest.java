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

import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SQLParserSchemaManagerTest {
    private SQLParserSchemaManager schemaManager;
    private String dorisTable;

    @Before
    public void init() {
        schemaManager = new SQLParserSchemaManager();
        dorisTable = "doris.tab";
    }

    @Test
    public void testParserAlterDDLs() {
        List<String> expectDDLs = new ArrayList<>();
        expectDDLs.add("ALTER TABLE `doris`.`tab` DROP COLUMN `c1`");
        expectDDLs.add("ALTER TABLE `doris`.`tab` DROP COLUMN `c2`");
        expectDDLs.add("ALTER TABLE `doris`.`tab` ADD COLUMN `c3` INT DEFAULT '100'");
        expectDDLs.add(
                "ALTER TABLE `doris`.`tab` ADD COLUMN `decimal_type` DECIMALV3(38,9) DEFAULT '1.123456789' COMMENT 'decimal_type_comment'");
        expectDDLs.add(
                "ALTER TABLE `doris`.`tab` ADD COLUMN `create_time` DATETIMEV2(3) DEFAULT CURRENT_TIMESTAMP COMMENT 'time_comment'");
        expectDDLs.add("ALTER TABLE `doris`.`tab` RENAME COLUMN `c10` `c11`");
        expectDDLs.add("ALTER TABLE `doris`.`tab` RENAME COLUMN `c12` `c13`");

        SourceConnector mysql = SourceConnector.MYSQL;
        String ddl =
                "alter table t1 drop c1, drop column c2, add c3 int default 100, add column `decimal_type` decimal(38,9) DEFAULT '1.123456789' COMMENT 'decimal_type_comment', add `create_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) comment 'time_comment', rename column c10 to c11, change column c12 c13 varchar(10)";
        List<String> actualDDLs = schemaManager.parserAlterDDLs(mysql, ddl, dorisTable);
        for (String actualDDL : actualDDLs) {
            Assert.assertTrue(expectDDLs.contains(actualDDL));
        }
    }

    @Test
    public void testParserAlterDDLsAdd() {
        List<String> expectDDLs = new ArrayList<>();
        expectDDLs.add("ALTER TABLE `doris`.`tab` ADD COLUMN `phone_number` VARCHAR(60)");
        expectDDLs.add("ALTER TABLE `doris`.`tab` ADD COLUMN `address` VARCHAR(765)");

        SourceConnector mysql = SourceConnector.ORACLE;
        String ddl =
                "ALTER TABLE employees ADD (phone_number VARCHAR2(20), address VARCHAR2(255));";
        List<String> actualDDLs = schemaManager.parserAlterDDLs(mysql, ddl, dorisTable);
        for (String actualDDL : actualDDLs) {
            Assert.assertTrue(expectDDLs.contains(actualDDL));
        }
    }

    @Test
    public void testParserAlterDDLsChange() {
        List<String> expectDDLs = new ArrayList<>();
        expectDDLs.add(
                "ALTER TABLE `doris`.`tab` RENAME COLUMN `old_phone_number` `new_phone_number`");
        expectDDLs.add("ALTER TABLE `doris`.`tab` RENAME COLUMN `old_address` `new_address`");

        SourceConnector mysql = SourceConnector.MYSQL;
        String ddl =
                "ALTER TABLE employees\n"
                        + "CHANGE COLUMN old_phone_number new_phone_number VARCHAR(20) NOT NULL,\n"
                        + "CHANGE COLUMN old_address new_address VARCHAR(255) DEFAULT 'Unknown',\n"
                        + "MODIFY COLUMN hire_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;";
        List<String> actualDDLs = schemaManager.parserAlterDDLs(mysql, ddl, dorisTable);
        for (String actualDDL : actualDDLs) {
            Assert.assertTrue(expectDDLs.contains(actualDDL));
        }
    }

    @Test
    public void testExtractCommentValue() {
        String expectComment = "";
        List<String> columnSpecs = Arrays.asList("default", "'100'", "COMMENT", "''");
        String actualComment = schemaManager.extractComment(columnSpecs);
        Assert.assertEquals(expectComment, actualComment);
    }

    @Test
    public void testExtractCommentValueQuotes() {
        String expectComment = "comment_test";
        List<String> columnSpecs =
                Arrays.asList("Default", "\"100\"", "comment", "\"comment_test\"");
        String actualComment = schemaManager.extractComment(columnSpecs);
        Assert.assertEquals(expectComment, actualComment);
    }

    @Test
    public void testExtractCommentValueNull() {
        List<String> columnSpecs = Arrays.asList("default", null, "CommenT", null);
        String actualComment = schemaManager.extractComment(columnSpecs);
        Assert.assertNull(actualComment);
    }

    @Test
    public void testExtractCommentValueEmpty() {
        List<String> columnSpecs = Arrays.asList("default", null, "comment");
        String actualComment = schemaManager.extractComment(columnSpecs);
        Assert.assertNull(actualComment);
    }

    @Test
    public void testExtractCommentValueA() {
        String expectComment = "test";
        List<String> columnSpecs = Arrays.asList("comment", "test");
        String actualComment = schemaManager.extractComment(columnSpecs);
        Assert.assertEquals(expectComment, actualComment);
    }

    @Test
    public void testExtractDefaultValue() {
        String expectDefault = "100";
        List<String> columnSpecs = Arrays.asList("default", "'100'", "comment", "");
        String actualDefault = schemaManager.extractDefaultValue(columnSpecs);
        Assert.assertEquals(expectDefault, actualDefault);
    }

    @Test
    public void testExtractDefaultValueQuotes() {
        String expectDefault = "100";
        List<String> columnSpecs = Arrays.asList("default", "\"100\"", "comment", "");
        String actualDefault = schemaManager.extractDefaultValue(columnSpecs);
        Assert.assertEquals(expectDefault, actualDefault);
    }

    @Test
    public void testExtractDefaultValueNull() {
        List<String> columnSpecs = Arrays.asList("Default", null, "comment", null);
        String actualDefault = schemaManager.extractDefaultValue(columnSpecs);
        Assert.assertNull(actualDefault);
    }

    @Test
    public void testExtractDefaultValueEmpty() {
        String expectDefault = null;
        List<String> columnSpecs = Arrays.asList("DEFAULT", "comment", null);
        String actualDefault = schemaManager.extractDefaultValue(columnSpecs);
        Assert.assertEquals(expectDefault, actualDefault);
    }

    @Test
    public void testExtractDefaultValueA() {
        String expectDefault = "aaa";
        List<String> columnSpecs = Arrays.asList("default", "aaa");
        String actualDefault = schemaManager.extractDefaultValue(columnSpecs);
        Assert.assertEquals(expectDefault, actualDefault);
    }

    @Test
    public void testExtractDefaultValueNULL() {
        List<String> columnSpecs = Collections.singletonList("default");
        String actualDefault = schemaManager.extractDefaultValue(columnSpecs);
        Assert.assertNull(actualDefault);
    }
}
