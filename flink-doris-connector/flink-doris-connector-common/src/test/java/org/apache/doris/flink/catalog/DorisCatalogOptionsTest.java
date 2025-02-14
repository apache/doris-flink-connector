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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DorisCatalogOptionsTest {

    @Test
    public void testGetCreateTableProps() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("table.properties.light_schema_change", "true");
        tableOptions.put("table.properties.replication_num", "1");
        tableOptions.put("fenodes", "127.0.0.1");

        Map<String, String> tableProps = DorisCatalogOptions.getCreateTableProps(tableOptions);

        assertEquals(2, tableProps.size());
        assertTrue(tableProps.containsKey("light_schema_change"));
        assertEquals("true", tableProps.get("light_schema_change"));
        assertTrue(tableProps.containsKey("replication_num"));
        assertEquals("1", tableProps.get("replication_num"));

        assertTrue(!tableProps.containsKey("fenodes"));
    }

    @Test
    public void testGetCreateTablePropsEmptyInput() {
        Map<String, String> tableOptions = new HashMap<>();
        Map<String, String> tableProps = DorisCatalogOptions.getCreateTableProps(tableOptions);
        assertTrue(tableProps.isEmpty());
    }

    @Test
    public void testGetCreateTablePropsNoMatchingPrefix() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("fenodes", "127.0.0.1:8030");
        tableOptions.put("benodes", "127.0.0.1:8040");

        Map<String, String> tableProps = DorisCatalogOptions.getCreateTableProps(tableOptions);
        assertTrue(tableProps.isEmpty());
    }
}
