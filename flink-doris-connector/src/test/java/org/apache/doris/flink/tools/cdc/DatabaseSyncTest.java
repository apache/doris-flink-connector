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

package org.apache.doris.flink.tools.cdc;

import org.apache.flink.configuration.Configuration;

import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/** Unit tests for the {@link DatabaseSync}. */
public class DatabaseSyncTest {
    @Test
    public void multiToOneRulesParserTest() throws Exception {
        String[][] testCase = {
            {"a_.*|b_.*", "a|b"} //  Normal condition
            // ,{"a_.*|b_.*","a|b|c"} // Unequal length
            // ,{"",""} // Null value
            // ,{"***....","a"} // Abnormal regular expression
        };
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        Arrays.stream(testCase)
                .forEach(
                        arr -> {
                            databaseSync.multiToOneRulesParser(arr[0], arr[1]);
                        });
    }

    @Test
    public void getSyncTableListTest() throws Exception {
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        databaseSync.setSingleSink(false);
        databaseSync.setIncludingTables("tbl_1|tbl_2");
        Configuration config = new Configuration();
        config.setString("database-name", "db");
        config.setString("table-name", "tbl.*");
        databaseSync.setConfig(config);
        String syncTableList = databaseSync.getSyncTableList(Arrays.asList("tbl_1", "tbl_2"));
        assertEquals("(db)\\.(tbl_1|tbl_2)", syncTableList);
    }

    @Test
    public void getTableBucketsTest() throws SQLException {
        String tableBuckets = "tbl1:10,tbl2 : 20, a.* :30,b.*:40,.*:50";
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        Map<String, Integer> tableBucketsMap = databaseSync.getTableBuckets(tableBuckets);
        assertEquals(10, tableBucketsMap.get("tbl1").intValue());
        assertEquals(20, tableBucketsMap.get("tbl2").intValue());
        assertEquals(30, tableBucketsMap.get("a.*").intValue());
        assertEquals(40, tableBucketsMap.get("b.*").intValue());
        assertEquals(50, tableBucketsMap.get(".*").intValue());
    }

    @Test
    public void setTableSchemaBucketsTest() throws SQLException {
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        String tableSchemaBuckets = "tbl1:10,tbl2:20,a11.*:30,a1.*:40,b.*:50,b1.*:60,.*:70";
        Map<String, Integer> tableBucketsMap = databaseSync.getTableBuckets(tableSchemaBuckets);
        List<String> tableList =
                Arrays.asList(
                        "tbl1", "tbl2", "tbl3", "a11", "a111", "a12", "a13", "b1", "b11", "b2",
                        "c1", "d1");
        HashMap<String, Integer> matchedTableBucketsMap = mockTableBuckets();
        Set<String> tableSet = new HashSet<>();
        tableList.forEach(
                tableName -> {
                    TableSchema tableSchema = new TableSchema();
                    tableSchema.setTable(tableName);
                    databaseSync.setTableSchemaBuckets(
                            tableBucketsMap, tableSchema, tableName, tableSet);
                    assertEquals(
                            matchedTableBucketsMap.get(tableName), tableSchema.getTableBuckets());
                });
    }

    @Test
    public void setTableSchemaBucketsTest1() throws SQLException {
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        String tableSchemaBuckets = ".*:10,a.*:20,tbl:30,b.*:40";
        Map<String, Integer> tableBucketsMap = databaseSync.getTableBuckets(tableSchemaBuckets);
        List<String> tableList = Arrays.asList("a1", "a2", "a3", "b1", "a");
        HashMap<String, Integer> matchedTableBucketsMap = mockTableBuckets1();
        Set<String> tableSet = new HashSet<>();
        tableList.forEach(
                tableName -> {
                    TableSchema tableSchema = new TableSchema();
                    tableSchema.setTable(tableName);
                    databaseSync.setTableSchemaBuckets(
                            tableBucketsMap, tableSchema, tableName, tableSet);
                    assertEquals(
                            matchedTableBucketsMap.get(tableName), tableSchema.getTableBuckets());
                });
    }

    @NotNull
    private static HashMap<String, Integer> mockTableBuckets() {
        HashMap<String, Integer> matchedTableBucketsMap = new HashMap<>();
        matchedTableBucketsMap.put("tbl1", 10);
        matchedTableBucketsMap.put("tbl2", 20);
        matchedTableBucketsMap.put("a11", 30);
        matchedTableBucketsMap.put("a111", 30);
        matchedTableBucketsMap.put("a12", 40);
        matchedTableBucketsMap.put("a13", 40);
        matchedTableBucketsMap.put("b1", 50);
        matchedTableBucketsMap.put("b11", 50);
        matchedTableBucketsMap.put("b2", 50);
        matchedTableBucketsMap.put("c1", 70);
        matchedTableBucketsMap.put("d1", 70);
        matchedTableBucketsMap.put("tbl3", 70);
        return matchedTableBucketsMap;
    }

    @NotNull
    private static HashMap<String, Integer> mockTableBuckets1() {
        HashMap<String, Integer> matchedTableBucketsMap = new HashMap<>();
        matchedTableBucketsMap.put("a", 10);
        matchedTableBucketsMap.put("a1", 10);
        matchedTableBucketsMap.put("a2", 10);
        matchedTableBucketsMap.put("a3", 10);
        matchedTableBucketsMap.put("b1", 10);
        matchedTableBucketsMap.put("tbl1", 10);
        return matchedTableBucketsMap;
    }
}
