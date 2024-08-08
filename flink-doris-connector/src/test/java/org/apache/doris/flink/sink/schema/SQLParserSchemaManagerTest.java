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

import org.apache.doris.flink.catalog.doris.DorisType;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
        List<String> actualDDLs = schemaManager.parseAlterDDLs(mysql, ddl, dorisTable);
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
        List<String> actualDDLs = schemaManager.parseAlterDDLs(mysql, ddl, dorisTable);
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
        List<String> actualDDLs = schemaManager.parseAlterDDLs(mysql, ddl, dorisTable);
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
        String actualDefault = schemaManager.extractDefaultValue(DorisType.INT, columnSpecs);
        Assert.assertEquals(expectDefault, actualDefault);
    }

    @Test
    public void testExtractDefaultValueQuotes() {
        String expectDefault = "100";
        List<String> columnSpecs = Arrays.asList("default", "\"100\"", "comment", "");
        String actualDefault = schemaManager.extractDefaultValue(DorisType.BIGINT, columnSpecs);
        Assert.assertEquals(expectDefault, actualDefault);
    }

    @Test
    public void testExtractDefaultValueNull() {
        List<String> columnSpecs = Arrays.asList("Default", null, "comment", null);
        String actualDefault = schemaManager.extractDefaultValue(DorisType.STRING, columnSpecs);
        Assert.assertNull(actualDefault);
    }

    @Test
    public void testExtractDefaultValueEmpty() {
        String expectDefault = null;
        List<String> columnSpecs = Arrays.asList("DEFAULT", "comment", null);
        String actualDefault = schemaManager.extractDefaultValue(DorisType.STRING, columnSpecs);
        Assert.assertEquals(expectDefault, actualDefault);
    }

    @Test
    public void testExtractDefaultValueA() {
        String expectDefault = "aaa";
        List<String> columnSpecs = Arrays.asList("default", "aaa");
        String actualDefault = schemaManager.extractDefaultValue(DorisType.STRING, columnSpecs);
        Assert.assertEquals(expectDefault, actualDefault);
    }

    @Test
    public void testExtractDefaultValueNULL() {
        List<String> columnSpecs = Collections.singletonList("default");
        String actualDefault = schemaManager.extractDefaultValue(DorisType.STRING, columnSpecs);
        Assert.assertNull(actualDefault);
    }

    @Test
    public void testExtractDefaultValueDateTime() {
        List<String> columnSpecs = Arrays.asList("default", "SYSTIMESTAMP");
        String actualDefault = schemaManager.extractDefaultValue(DorisType.DATETIME, columnSpecs);
        Assert.assertEquals("CURRENT_TIMESTAMP", actualDefault);
    }

    @Test
    public void testExtractDefaultValueDateTimeV2() {
        List<String> columnSpecs = Arrays.asList("default", "GETDATE()");
        String actualDefault =
                schemaManager.extractDefaultValue(DorisType.DATETIME_V2, columnSpecs);
        Assert.assertEquals("CURRENT_TIMESTAMP", actualDefault);
    }

    @Test
    public void testExtractDefaultValueDateTimeV2Time() {
        List<String> columnSpecs = Arrays.asList("default", "2024-03-14 17:50:36.002");
        String actualDefault = schemaManager.extractDefaultValue("DATETIMEV2(3)", columnSpecs);
        Assert.assertEquals("2024-03-14 17:50:36.002", actualDefault);
    }

    @Test
    public void testExtractDefaultValueDateTimeV2CurrentTime() {
        List<String> columnSpecs = Arrays.asList("default", "now()");
        String actualDefault = schemaManager.extractDefaultValue("DATETIMEV2(3)", columnSpecs);
        Assert.assertEquals("CURRENT_TIMESTAMP", actualDefault);
    }

    @Test
    public void testExtractDefaultValueDate() {
        List<String> columnSpecs = Arrays.asList("default", "2024-03-14 17:50:36");
        String actualDefault = schemaManager.extractDefaultValue(DorisType.DATE, columnSpecs);
        Assert.assertEquals("2024-03-14 17:50:36", actualDefault);
    }

    @Test
    public void testRemoveContinuousChar() {
        // Test removing continuous target characters from both ends
        Assert.assertEquals("bc", schemaManager.removeContinuousChar("aaaabcaaa", 'a'));
        Assert.assertEquals("bcde", schemaManager.removeContinuousChar("abcdea", 'a'));

        // Test cases with no target character
        Assert.assertEquals("abc", schemaManager.removeContinuousChar("abc", 'x'));

        // Test cases with only target characters
        Assert.assertEquals("", schemaManager.removeContinuousChar("aaaa", 'a'));
        Assert.assertEquals("", schemaManager.removeContinuousChar("xxxxxxxx", 'x'));

        // Test empty and null strings
        Assert.assertNull(schemaManager.removeContinuousChar(null, 'a'));
        Assert.assertEquals("", schemaManager.removeContinuousChar("", 'a'));

        // Test single character strings
        Assert.assertEquals("b", schemaManager.removeContinuousChar("b", 'a'));

        // Test removing quotes
        Assert.assertEquals("abc", schemaManager.removeContinuousChar("\"abc\"", '\"'));
        Assert.assertEquals("a\"bc\"d", schemaManager.removeContinuousChar("\"a\"bc\"d\"", '\"'));
        Assert.assertEquals("abc", schemaManager.removeContinuousChar("'abc'", '\''));
    }

    @Test
    public void testParseCreateTableStatement() {
        String dorisTable = "doris.auto_tab";
        String ddl =
                "CREATE TABLE `test_sinka` (\n"
                        + "  `id` int NOT NULL DEFAULT '10000' COMMENT 'id_test',\n"
                        + "  `create_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3),\n"
                        + "  `c1` int DEFAULT '999',\n"
                        + "  `decimal_type` decimal(9,3) DEFAULT '1.000' COMMENT 'decimal_tes',\n"
                        + "  `aaa` varchar(100) DEFAULT NULL,\n"
                        + "  `decimal_type3` decimal(38,9) DEFAULT '1.123456789' COMMENT 'comment_test',\n"
                        + "  `create_time3` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'ttime_aaa',\n"
                        + "  PRIMARY KEY (`id`)\n"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
        TableSchema tableSchema =
                schemaManager.parseCreateTableStatement(
                        SourceConnector.MYSQL, ddl, dorisTable, null);

        String expected =
                "TableSchema{database='doris', table='auto_tab', tableComment='null', fields={`id`=FieldSchema{name='`id`', typeString='INT', defaultValue='10000', comment='id_test'}, `create_time`=FieldSchema{name='`create_time`', typeString='DATETIMEV2(3)', defaultValue='CURRENT_TIMESTAMP', comment='null'}, `c1`=FieldSchema{name='`c1`', typeString='INT', defaultValue='999', comment='null'}, `decimal_type`=FieldSchema{name='`decimal_type`', typeString='DECIMALV3(9,3)', defaultValue='1.000', comment='decimal_tes'}, `aaa`=FieldSchema{name='`aaa`', typeString='VARCHAR(300)', defaultValue='NULL', comment='null'}, `decimal_type3`=FieldSchema{name='`decimal_type3`', typeString='DECIMALV3(38,9)', defaultValue='1.123456789', comment='comment_test'}, `create_time3`=FieldSchema{name='`create_time3`', typeString='DATETIMEV2(3)', defaultValue='CURRENT_TIMESTAMP', comment='ttime_aaa'}}, keys=`id`, model=UNIQUE, distributeKeys=`id`, properties={}, tableBuckets=null}";
        Assert.assertEquals(expected, tableSchema.toString());
    }

    @Test
    public void testParseCreateUniqueTableStatement() {
        String dorisTable = "doris.auto_uni_tab";
        String ddl =
                "CREATE TABLE test_sink_unique (     id INT NOT NULL,     name VARCHAR(100) NOT NULL,     age INT,     email VARCHAR(100),     UNIQUE (email) )";
        TableSchema tableSchema =
                schemaManager.parseCreateTableStatement(
                        SourceConnector.MYSQL, ddl, dorisTable, null);

        String expected =
                "TableSchema{database='doris', table='auto_uni_tab', tableComment='null', fields={id=FieldSchema{name='id', typeString='INT', defaultValue='null', comment='null'}, name=FieldSchema{name='name', typeString='VARCHAR(300)', defaultValue='null', comment='null'}, age=FieldSchema{name='age', typeString='INT', defaultValue='null', comment='null'}, email=FieldSchema{name='email', typeString='VARCHAR(300)', defaultValue='null', comment='null'}}, keys=email, model=UNIQUE, distributeKeys=email, properties={}, tableBuckets=null}";
        Assert.assertEquals(expected, tableSchema.toString());
    }

    @Test
    public void testParseCreateDuplicateTableStatement() {
        String dorisTable = "doris.auto_duptab";
        String ddl =
                "CREATE TABLE test_sink_duplicate (\n"
                        + "    id INT,\n"
                        + "    name VARCHAR(50),\n"
                        + "    age INT,\n"
                        + "    address VARCHAR(255)\n"
                        + ")";
        TableSchema tableSchema =
                schemaManager.parseCreateTableStatement(
                        SourceConnector.MYSQL, ddl, dorisTable, null);

        String expected =
                "TableSchema{database='doris', table='auto_duptab', tableComment='null', fields={id=FieldSchema{name='id', typeString='INT', defaultValue='null', comment='null'}, name=FieldSchema{name='name', typeString='VARCHAR(150)', defaultValue='null', comment='null'}, age=FieldSchema{name='age', typeString='INT', defaultValue='null', comment='null'}, address=FieldSchema{name='address', typeString='VARCHAR(765)', defaultValue='null', comment='null'}}, keys=id, model=DUPLICATE, distributeKeys=id, properties={}, tableBuckets=null}";
        Assert.assertEquals(expected, tableSchema.toString());
    }

    @Test
    public void testParseOracleTableStatement() {
        String dorisTable = "doris.auto_tab";
        String ddl =
                "CREATE TABLE employees (\n"
                        + "    employee_id NUMBER(10) NOT NULL,\n"
                        + "    first_name VARCHAR2(50),\n"
                        + "    last_name VARCHAR2(50) NOT NULL,\n"
                        + "    email VARCHAR2(100) UNIQUE,\n"
                        + "    phone_number VARCHAR2(20),\n"
                        + "    hire_date DATE DEFAULT SYSDATE NOT NULL,\n"
                        + "    job_id VARCHAR2(10) NOT NULL,\n"
                        + "    salary NUMBER(8, 2),\n"
                        + "    commission_pct NUMBER(2, 2),\n"
                        + "    manager_id NUMBER(10),\n"
                        + "    department_id NUMBER(10),\n"
                        + "    CONSTRAINT pk_employee PRIMARY KEY (employee_id),\n"
                        + "    CONSTRAINT fk_department FOREIGN KEY (department_id)\n"
                        + "        REFERENCES departments(department_id)\n"
                        + ");";
        TableSchema tableSchema =
                schemaManager.parseCreateTableStatement(
                        SourceConnector.ORACLE, ddl, dorisTable, null);

        String expected =
                "TableSchema{database='doris', table='auto_tab', tableComment='null', fields={employee_id=FieldSchema{name='employee_id', typeString='BIGINT', defaultValue='null', comment='null'}, first_name=FieldSchema{name='first_name', typeString='VARCHAR(150)', defaultValue='null', comment='null'}, last_name=FieldSchema{name='last_name', typeString='VARCHAR(150)', defaultValue='null', comment='null'}, email=FieldSchema{name='email', typeString='VARCHAR(300)', defaultValue='null', comment='null'}, phone_number=FieldSchema{name='phone_number', typeString='VARCHAR(60)', defaultValue='null', comment='null'}, hire_date=FieldSchema{name='hire_date', typeString='DATETIMEV2', defaultValue='CURRENT_TIMESTAMP', comment='null'}, job_id=FieldSchema{name='job_id', typeString='VARCHAR(30)', defaultValue='null', comment='null'}, salary=FieldSchema{name='salary', typeString='DECIMALV3(8,2)', defaultValue='null', comment='null'}, commission_pct=FieldSchema{name='commission_pct', typeString='DECIMALV3(2,2)', defaultValue='null', comment='null'}, manager_id=FieldSchema{name='manager_id', typeString='BIGINT', defaultValue='null', comment='null'}, department_id=FieldSchema{name='department_id', typeString='BIGINT', defaultValue='null', comment='null'}}, keys=employee_id, model=UNIQUE, distributeKeys=employee_id, properties={}, tableBuckets=null}";
        Assert.assertEquals(expected, tableSchema.toString());
    }

    @Test
    public void testParseOraclePrimaryTableStatement() {
        String dorisTable = "doris.auto_tab";
        String ddl =
                "CREATE TABLE employees (\n"
                        + "    employee_id NUMBER(10) PRIMARY KEY,\n"
                        + "    first_name VARCHAR2(50),\n"
                        + "    last_name VARCHAR2(50) NOT NULL,\n"
                        + "    email VARCHAR2(100),\n"
                        + "    phone_number VARCHAR2(20),\n"
                        + "    hire_date DATE DEFAULT SYSDATE NOT NULL,\n"
                        + "    job_id VARCHAR2(10) NOT NULL,\n"
                        + "    salary NUMBER(8, 2),\n"
                        + "    commission_pct NUMBER(2, 2),\n"
                        + "    manager_id NUMBER(10),\n"
                        + "    department_id NUMBER(10)\n"
                        + ");";
        TableSchema tableSchema =
                schemaManager.parseCreateTableStatement(
                        SourceConnector.ORACLE, ddl, dorisTable, null);

        String expected =
                "TableSchema{database='doris', table='auto_tab', tableComment='null', fields={employee_id=FieldSchema{name='employee_id', typeString='BIGINT', defaultValue='null', comment='null'}, first_name=FieldSchema{name='first_name', typeString='VARCHAR(150)', defaultValue='null', comment='null'}, last_name=FieldSchema{name='last_name', typeString='VARCHAR(150)', defaultValue='null', comment='null'}, email=FieldSchema{name='email', typeString='VARCHAR(300)', defaultValue='null', comment='null'}, phone_number=FieldSchema{name='phone_number', typeString='VARCHAR(60)', defaultValue='null', comment='null'}, hire_date=FieldSchema{name='hire_date', typeString='DATETIMEV2', defaultValue='CURRENT_TIMESTAMP', comment='null'}, job_id=FieldSchema{name='job_id', typeString='VARCHAR(30)', defaultValue='null', comment='null'}, salary=FieldSchema{name='salary', typeString='DECIMALV3(8,2)', defaultValue='null', comment='null'}, commission_pct=FieldSchema{name='commission_pct', typeString='DECIMALV3(2,2)', defaultValue='null', comment='null'}, manager_id=FieldSchema{name='manager_id', typeString='BIGINT', defaultValue='null', comment='null'}, department_id=FieldSchema{name='department_id', typeString='BIGINT', defaultValue='null', comment='null'}}, keys=employee_id, model=UNIQUE, distributeKeys=employee_id, properties={}, tableBuckets=null}";
        Assert.assertEquals(expected, tableSchema.toString());
    }

    @Test
    public void testParseOracleDuplicateTableStatement() {
        String dorisTable = "doris.auto_tab";
        String ddl =
                "CREATE TABLE orders (\n"
                        + "    order_id NUMBER(10) NOT NULL,\n"
                        + "    customer_id NUMBER(10) NOT NULL,\n"
                        + "    order_date DATE DEFAULT SYSDATE NOT NULL,\n"
                        + "    status VARCHAR2(20) CHECK (status IN ('PENDING', 'SHIPPED', 'DELIVERED', 'CANCELLED')),\n"
                        + "    total_amount NUMBER(12, 2) NOT NULL,\n"
                        + "    shipping_address VARCHAR2(255),\n"
                        + "    delivery_date DATE,\n"
                        + "    CONSTRAINT fk_customer FOREIGN KEY (customer_id)\n"
                        + "        REFERENCES customers(customer_id),\n"
                        + "    CONSTRAINT chk_total_amount CHECK (total_amount >= 0)\n"
                        + ");";
        TableSchema tableSchema =
                schemaManager.parseCreateTableStatement(
                        SourceConnector.ORACLE,
                        ddl,
                        dorisTable,
                        new DorisTableConfig(new HashMap<>()));
        String expected =
                "TableSchema{database='doris', table='auto_tab', tableComment='null', fields={order_id=FieldSchema{name='order_id', typeString='BIGINT', defaultValue='null', comment='null'}, customer_id=FieldSchema{name='customer_id', typeString='BIGINT', defaultValue='null', comment='null'}, order_date=FieldSchema{name='order_date', typeString='DATETIMEV2', defaultValue='CURRENT_TIMESTAMP', comment='null'}, status=FieldSchema{name='status', typeString='VARCHAR(60)', defaultValue='null', comment='null'}, total_amount=FieldSchema{name='total_amount', typeString='DECIMALV3(12,2)', defaultValue='null', comment='null'}, shipping_address=FieldSchema{name='shipping_address', typeString='VARCHAR(765)', defaultValue='null', comment='null'}, delivery_date=FieldSchema{name='delivery_date', typeString='DATETIMEV2', defaultValue='null', comment='null'}}, keys=order_id, model=DUPLICATE, distributeKeys=order_id, properties={light_schema_change=true}, tableBuckets=null}";
        Assert.assertEquals(expected, tableSchema.toString());
    }
}
