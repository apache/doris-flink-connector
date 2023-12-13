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

import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/** Unit tests for the {@link DatabaseSync}. */
public class DatabaseSyncTest {
    @Test
    public void multiToOneRulesParserTest() throws Exception {
        String[][] testCase = {
            {"a_.*|b_.*", "a|b"} //  Normal condition
            //                ,{"a_.*|b_.*","a|b|c"} // Unequal length
            //                ,{"",""} // Null value
            //                ,{"***....","a"} // Abnormal regular expression
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
        Assert.assertEquals("db\\.tbl_1|db\\.tbl_2", syncTableList);
    }
}
