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

package org.apache.doris.flink.catalog;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.doris.flink.table.DorisConfigOptions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DorisCatalogFactoryTest {

    @Test
    public void testCreateCatalog() {
        final Map<String, String> options = new HashMap<>();
        options.put("type", DorisConfigOptions.IDENTIFIER);
        options.put("jdbc-url", "jdbc:mysql://127.0.0.1:9030");
        options.put("fenodes", "127.0.0.1:8030");
        options.put("default-database", "doris_db");
        options.put("username", "root");
        options.put("password", "");
        options.put("doris.request.query.timeout", "3600s");
        options.put("sink.enable-2pc", "false");
        options.put("sink.properties.format", "json");
        options.put("sink.properties.read_json_by_line", "true");
        options.put("table.properties.replication_num", "1");

        Catalog catalog =
                FactoryUtil.createCatalog(
                        "test_catalog",
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader());
        assertTrue(catalog instanceof DorisCatalog);
        DorisCatalog dorisCatalog = (DorisCatalog) catalog;
        assertEquals("test_catalog", dorisCatalog.getName());
        assertEquals("doris_db", dorisCatalog.getDefaultDatabase());
        assertEquals(
                "jdbc:mysql://127.0.0.1:9030", dorisCatalog.getConnectionOptions().getJdbcUrl());
        assertEquals("127.0.0.1:8030", dorisCatalog.getConnectionOptions().getFenodes());
        assertEquals("root", dorisCatalog.getConnectionOptions().getUsername());
        assertEquals("", dorisCatalog.getConnectionOptions().getPassword());

        Map<String, String> properties = dorisCatalog.getProperties();
        assertEquals(10, properties.size());
        assertEquals("3600s", properties.get("doris.request.query.timeout"));
        assertEquals("false", properties.get("sink.enable-2pc"));
        assertEquals("json", properties.get("sink.properties.format"));
        assertEquals("true", properties.get("sink.properties.read_json_by_line"));
        assertEquals("1", properties.get("table.properties.replication_num"));
    }
}
