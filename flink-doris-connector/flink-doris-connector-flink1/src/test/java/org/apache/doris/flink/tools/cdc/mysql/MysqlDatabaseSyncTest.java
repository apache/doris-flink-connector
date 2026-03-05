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

package org.apache.doris.flink.tools.cdc.mysql;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MysqlDatabaseSyncTest {

    @Test
    public void testSkipTableWithoutPrimaryKeyInInitialSnapshot() throws Exception {
        MysqlDatabaseSync sync = new MysqlDatabaseSync();
        Configuration config = new Configuration();
        config.setString(MySqlSourceOptions.SCAN_STARTUP_MODE.key(), "initial");
        sync.setConfig(config);

        boolean shouldSkip =
                sync.shouldSkipTableWithoutPrimaryKey(
                        "test_db", "no_pk_table", true, Collections.emptySet());

        Assert.assertTrue(shouldSkip);
    }

    @Test
    public void testDoNotSkipTableWithoutPrimaryKeyWhenChunkKeyConfigured() throws Exception {
        MysqlDatabaseSync sync = new MysqlDatabaseSync();
        Configuration config = new Configuration();
        config.setString(MySqlSourceOptions.SCAN_STARTUP_MODE.key(), "initial");
        sync.setConfig(config);

        Set<String> chunkKeyTables = new HashSet<>();
        chunkKeyTables.add("test_db.no_pk_table");

        boolean shouldSkip =
                sync.shouldSkipTableWithoutPrimaryKey(
                        "test_db", "no_pk_table", true, chunkKeyTables);

        Assert.assertFalse(shouldSkip);
    }

    @Test
    public void testDoNotSkipTableWithoutPrimaryKeyForNonInitialStartup() throws Exception {
        MysqlDatabaseSync sync = new MysqlDatabaseSync();
        Configuration config = new Configuration();
        config.setString(MySqlSourceOptions.SCAN_STARTUP_MODE.key(), "latest-offset");
        sync.setConfig(config);

        boolean shouldSkip =
                sync.shouldSkipTableWithoutPrimaryKey(
                        "test_db", "no_pk_table", true, Collections.emptySet());

        Assert.assertFalse(shouldSkip);
    }
}
