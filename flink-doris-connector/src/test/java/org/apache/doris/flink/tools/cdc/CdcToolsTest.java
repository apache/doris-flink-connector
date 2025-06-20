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

import org.apache.flink.api.java.utils.MultipleParameterTool;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CdcToolsTest {

    @Test
    public void getConfigMapTest() {
        MultipleParameterTool params =
                MultipleParameterTool.fromArgs(
                        new String[] {
                            "--sink-conf",
                            "fenodes = 127.0.0.1:8030",
                            "--sink-conf",
                            "password=",
                            "--sink-conf",
                            "jdbc-url= jdbc:mysql://127.0.0.1:9030 ",
                            "--sink-conf",
                            "sink.label-prefix  = label "
                        });
        Map<String, String> sinkConf = CdcTools.getConfigMap(params, DatabaseSyncConfig.SINK_CONF);

        Map<String, String> excepted = new HashMap<>();
        excepted.put("password", "");
        excepted.put("fenodes", "127.0.0.1:8030");
        excepted.put("jdbc-url", "jdbc:mysql://127.0.0.1:9030");
        excepted.put("sink.label-prefix", "label");
        Assert.assertEquals(sinkConf, excepted);

        Map<String, String> mysqlConf =
                CdcTools.getConfigMap(params, DatabaseSyncConfig.MYSQL_CONF);
        Assert.assertNull(mysqlConf);

        MultipleParameterTool params2 =
                MultipleParameterTool.fromArgs(new String[] {"--sink-conf", "fenodes"});
        Map<String, String> sinkConf2 =
                CdcTools.getConfigMap(params2, DatabaseSyncConfig.SINGLE_SINK);
        Assert.assertNull(sinkConf2);
    }

    @Test
    public void testGetConfigMap() {
        Map<String, Collection<String>> config = new HashMap<>();
        config.put(
                DatabaseSyncConfig.MYSQL_CONF, Arrays.asList("  hostname=127.0.0.1", " port=3306"));
        config.put(
                DatabaseSyncConfig.POSTGRES_CONF,
                Arrays.asList("hostname=127.0.0.1 ", "port=5432 "));
        config.put(
                DatabaseSyncConfig.SINK_CONF,
                Arrays.asList(" fenodes=127.0.0.1:8030 ", " username=root"));
        config.put(DatabaseSyncConfig.TABLE_CONF, Collections.singletonList("  replication_num=1"));
        MultipleParameterTool parameter = MultipleParameterTool.fromMultiMap(config);
        Map<String, String> mysqlConfigMap =
                CdcTools.getConfigMap(parameter, DatabaseSyncConfig.MYSQL_CONF);
        Map<String, String> postGresConfigMap =
                CdcTools.getConfigMap(parameter, DatabaseSyncConfig.POSTGRES_CONF);
        Map<String, String> sinkConfigMap =
                CdcTools.getConfigMap(parameter, DatabaseSyncConfig.SINK_CONF);
        Map<String, String> tableConfigMap =
                CdcTools.getConfigMap(parameter, DatabaseSyncConfig.TABLE_CONF);

        Set<String> mysqlKeyConf = new HashSet<>(Arrays.asList("hostname", "port"));
        Set<String> mysqlValueConf = new HashSet<>(Arrays.asList("127.0.0.1", "3306"));
        assertEquals(mysqlConfigMap, mysqlKeyConf, mysqlValueConf);

        Set<String> postgresKeyConf = new HashSet<>(Arrays.asList("hostname", "port"));
        Set<String> postgresValueConf = new HashSet<>(Arrays.asList("127.0.0.1", "5432"));
        assertEquals(postGresConfigMap, postgresKeyConf, postgresValueConf);

        Set<String> sinkKeyConf = new HashSet<>(Arrays.asList("fenodes", "username"));
        Set<String> sinkValueConf = new HashSet<>(Arrays.asList("127.0.0.1:8030", "root"));
        assertEquals(sinkConfigMap, sinkKeyConf, sinkValueConf);

        Set<String> tableKeyConf = new HashSet<>(Collections.singletonList("replication_num"));
        Set<String> tableValueConf = new HashSet<>(Collections.singletonList("1"));
        assertEquals(tableConfigMap, tableKeyConf, tableValueConf);
    }

    private void assertEquals(
            Map<String, String> actualMap, Set<String> keyConf, Set<String> valueConf) {
        for (Entry<String, String> entry : actualMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            Assert.assertTrue(keyConf.contains(key));
            Assert.assertTrue(valueConf.contains(value));
        }
    }

    @Test
    public void testGetConfigMapWithEnvironmentVariables() {
        // Test cases for environment variable substitution.
        // We assume these environment variables are NOT set in the test environment,
        // so they should resolve to their placeholder strings.

        // Case 1: Simple env var placeholder
        MultipleParameterTool params1 =
                MultipleParameterTool.fromArgs(
                        new String[] {"--test-conf", "db.user=$DB_USER_UNSET"});
        Map<String, String> expected1 = new HashMap<>();
        expected1.put("db.user", "$DB_USER_UNSET");
        Assert.assertEquals(expected1, CdcTools.getConfigMap(params1, "test-conf"));

        // Case 2: Env var with braces placeholder
        MultipleParameterTool params2 =
                MultipleParameterTool.fromArgs(
                        new String[] {"--test-conf", "db.pass=${DB_PASS_UNSET}"});
        Map<String, String> expected2 = new HashMap<>();
        expected2.put("db.pass", "${DB_PASS_UNSET}");
        Assert.assertEquals(expected2, CdcTools.getConfigMap(params2, "test-conf"));

        // Case 3: Mix of plain string and env var placeholder
        MultipleParameterTool params3 =
                MultipleParameterTool.fromArgs(
                        new String[] {
                            "--test-conf",
                            "db.host=localhost",
                            "--test-conf",
                            "db.port=$DB_PORT_UNSET"
                        });
        Map<String, String> expected3 = new HashMap<>();
        expected3.put("db.host", "localhost");
        expected3.put("db.port", "$DB_PORT_UNSET");
        Assert.assertEquals(expected3, CdcTools.getConfigMap(params3, "test-conf"));

        // Case 4: Env var within a string
        MultipleParameterTool params4 =
                MultipleParameterTool.fromArgs(
                        new String[] {
                            "--test-conf", "conn.string=jdbc:mysql://$DB_HOST_UNSET:3306/mydb"
                        });
        Map<String, String> expected4 = new HashMap<>();
        expected4.put("conn.string", "jdbc:mysql://$DB_HOST_UNSET:3306/mydb");
        Assert.assertEquals(expected4, CdcTools.getConfigMap(params4, "test-conf"));

        // Case 5: Multiple env vars in one string
        MultipleParameterTool params5 =
                MultipleParameterTool.fromArgs(
                        new String[] {
                            "--test-conf", "credentials=user:$USER_UNSET,pass:$PASS_UNSET"
                        });
        Map<String, String> expected5 = new HashMap<>();
        expected5.put("credentials", "user:$USER_UNSET,pass:$PASS_UNSET");
        Assert.assertEquals(expected5, CdcTools.getConfigMap(params5, "test-conf"));

        // Case 6: No env vars (regular behavior)
        MultipleParameterTool params6 =
                MultipleParameterTool.fromArgs(
                        new String[] {"--test-conf", "key1=value1", "--test-conf", "key2=value2"});
        Map<String, String> expected6 = new HashMap<>();
        expected6.put("key1", "value1");
        expected6.put("key2", "value2");
        Assert.assertEquals(expected6, CdcTools.getConfigMap(params6, "test-conf"));

        // Case 7: Env var for a key that allows empty value (e.g. password), resolves to
        // placeholder
        MultipleParameterTool params7 =
                MultipleParameterTool.fromArgs(
                        new String[] {"--test-conf", "password=$PASSWORD_UNSET"});
        Map<String, String> expected7 = new HashMap<>();
        expected7.put(
                "password", "$PASSWORD_UNSET"); // DatabaseSyncConfig.PASSWORD is in EMPTY_KEYS
        Assert.assertEquals(expected7, CdcTools.getConfigMap(params7, "test-conf"));

        // Case 8: Env var that resolves to an empty string (if it were set to empty)
        // For this test, we simulate it by having the placeholder itself, as we can't set it to
        // empty easily here.
        // If $EMPTY_VAR was set to "", the result for "key" would be "".
        // Since it's not set, it remains "$EMPTY_VAR".
        MultipleParameterTool params8 =
                MultipleParameterTool.fromArgs(new String[] {"--test-conf", "key=$EMPTY_VAR"});
        Map<String, String> expected8 = new HashMap<>();
        expected8.put("key", "$EMPTY_VAR");
        Assert.assertEquals(expected8, CdcTools.getConfigMap(params8, "test-conf"));
    }
}
