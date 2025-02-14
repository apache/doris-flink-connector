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

import org.apache.doris.flink.exception.CreateTableException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DorisSystemTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void buildCreateTableDDL() {
        TableSchema schema = new TableSchema();
        schema.setTable("table");
        schema.setDatabase("db");
        schema.setKeys(Arrays.asList("name"));
        Map<String, FieldSchema> map = new HashMap<>();
        FieldSchema field = new FieldSchema();
        field.setName("age");
        map.put("age", field);
        schema.setFields(map);
        thrown.expect(CreateTableException.class);
        thrown.expectMessage("key name not found in column list");
        DorisSystem.buildCreateTableDDL(schema);
    }

    @Test
    public void buildCreateTableBuckets() {
        TableSchema schema = new TableSchema();
        schema.setTable("table");
        schema.setDatabase("db");
        schema.setKeys(Arrays.asList("name"));
        Map<String, FieldSchema> map = new HashMap<>();
        FieldSchema field = new FieldSchema();
        field.setName("name");
        map.put("name", field);
        schema.setFields(map);
        schema.setTableBuckets(-1);
        thrown.expect(CreateTableException.class);
        thrown.expectMessage("The number of buckets must be positive");
        DorisSystem.buildCreateTableDDL(schema);
    }

    @Test
    public void buildCreateTableProperties() {
        TableSchema schema = new TableSchema();
        schema.setTable("table");
        schema.setDatabase("db");
        schema.setKeys(Arrays.asList("name"));
        schema.setDistributeKeys(Arrays.asList("name"));
        Map<String, FieldSchema> map = new HashMap<>();
        FieldSchema field = new FieldSchema();
        field.setName("name");
        field.setTypeString("STRING");
        field.setDefaultValue("zhangsan");
        map.put("name", field);
        schema.setFields(map);
        schema.setTableBuckets(1);
        Map<String, String> properties = new HashMap<>();
        properties.put("replication_num", "3");
        schema.setProperties(properties);
        String createTableDDL = DorisSystem.buildCreateTableDDL(schema);
        String except =
                "CREATE TABLE IF NOT EXISTS `db`.`table`(`name` VARCHAR(65533) DEFAULT 'zhangsan' COMMENT '' )  DISTRIBUTED BY HASH(`name`) BUCKETS 1 PROPERTIES ('replication_num'='3')";
        Assert.assertEquals(createTableDDL, except);
    }

    @Test
    public void quoteTableIdentifier() {
        String quoted = DorisSystem.quoteTableIdentifier("db.tbl");
        Assert.assertEquals("`db`.`tbl`", quoted);
        thrown.expect(IllegalArgumentException.class);
        quoted = DorisSystem.quoteTableIdentifier("db.tbl.sc");
    }
}
