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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
        TableSchema tableSchema =
                DorisSchemaFactory.createTableSchema(
                        dbTable[0],
                        dbTable[1],
                        columnFields,
                        pkKeys,
                        tableProperties,
                        tableComment);
        Assert.assertEquals(
                "TableSchema{database='doris', table='create_tab', tableComment='auto_tab_comment', fields={name=FieldSchema{name='name', typeString='VARVHAR(100)', defaultValue='null', comment='Name_test'}, id=FieldSchema{name='id', typeString='INT', defaultValue='100', comment='int_test'}, age=FieldSchema{name='age', typeString='INT', defaultValue='null', comment='null'}, email=FieldSchema{name='email', typeString='VARCHAR(100)', defaultValue='email@doris.com', comment='e'}}, keys=email, model=UNIQUE, distributeKeys=email, properties={}, tableBuckets=null}",
                tableSchema.toString());
    }

    @Test
    public void testCreateDuplicateTableSchema() {
        String dorisTable = "doris.dup_tab";
        String[] dbTable = dorisTable.split("\\.");
        Preconditions.checkArgument(dbTable.length == 2);

        Map<String, FieldSchema> columnFields = new HashMap<>();
        columnFields.put("id", new FieldSchema("id", "INT", "100", "int_test"));
        columnFields.put("name", new FieldSchema("name", "VARVHAR(100)", null, "Name_test"));
        columnFields.put("age", new FieldSchema("age", "INT", null, null));
        columnFields.put("email", new FieldSchema("email", "VARCHAR(100)", "email@doris.com", "e"));
        Map<String, String> tableProperties = new HashMap<>();
        String tableComment = "auto_tab_comment";
        TableSchema tableSchema =
                DorisSchemaFactory.createTableSchema(
                        dbTable[0],
                        dbTable[1],
                        columnFields,
                        new ArrayList<>(),
                        tableProperties,
                        tableComment);
        Assert.assertEquals(
                "TableSchema{database='doris', table='dup_tab', tableComment='auto_tab_comment', fields={name=FieldSchema{name='name', typeString='VARVHAR(100)', defaultValue='null', comment='Name_test'}, id=FieldSchema{name='id', typeString='INT', defaultValue='100', comment='int_test'}, age=FieldSchema{name='age', typeString='INT', defaultValue='null', comment='null'}, email=FieldSchema{name='email', typeString='VARCHAR(100)', defaultValue='email@doris.com', comment='e'}}, keys=name, model=DUPLICATE, distributeKeys=name, properties={}, tableBuckets=null}",
                tableSchema.toString());
    }

    @Test
    public void buildTableBucketMapTest() {
        String tableBuckets = "tbl1:10,tbl2 : 20, a.* :30,b.*:40,.*:50";
        Map<String, Integer> tableBucketsMap = DorisSchemaFactory.buildTableBucketMap(tableBuckets);
        assertEquals(10, tableBucketsMap.get("tbl1").intValue());
        assertEquals(20, tableBucketsMap.get("tbl2").intValue());
        assertEquals(30, tableBucketsMap.get("a.*").intValue());
        assertEquals(40, tableBucketsMap.get("b.*").intValue());
        assertEquals(50, tableBucketsMap.get(".*").intValue());
    }
}
