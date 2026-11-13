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

package org.apache.doris.flink.tools.cdc.mongodb;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeContext;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.TestJsonDebeziumChangeBase;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.apache.doris.flink.tools.cdc.converter.TableNameConverter;
import org.apache.doris.flink.tools.cdc.mongodb.serializer.MongoJsonDebeziumSchemaChange;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MongoDBCreateTableTest extends TestJsonDebeziumChangeBase {

    private MongoJsonDebeziumSchemaChange schemaChange;

    @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock private DorisSystem mockDorisSystem;

    @Mock private DorisOptions mockDorisOptions;

    @Mock private SchemaChangeManager mockSchemaManager;

    private final String dbName = "test_db";
    private final String prefix = "ods_";
    private final String suffix = "_dt";

    @Before
    public void setUp() {
        super.setUp();

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put(DorisTableConfig.REPLICATION_NUM, "1");
        tableConfig.put(DorisTableConfig.TABLE_BUCKETS, ".*:1");

        JsonDebeziumChangeContext changeContext =
                new JsonDebeziumChangeContext(
                        mockDorisOptions,
                        tableMapping,
                        null,
                        dbName,
                        new DorisTableConfig(tableConfig),
                        objectMapper,
                        null,
                        lineDelimiter,
                        ignoreUpdateBefore,
                        prefix,
                        suffix,
                        true,
                        new TableNameConverter(prefix, suffix));
        schemaChange = new MongoJsonDebeziumSchemaChange(changeContext);
    }

    @Test
    public void testAutoCreateTable() throws IOException {
        String newTableName = "test_table";
        String record =
                "{"
                        + "\"_id\":\"{\\\"_id\\\": {\\\"_id\\\": {\\\"$oid\\\": \\\"67d2d13807fe0c4336070cfd\\\"}}}\","
                        + "\"operationType\":\"insert\","
                        + "\"fullDocument\":\"{\\\"_id\\\": {\\\"$oid\\\": \\\"67d2d13807fe0c4336070cfd\\\"}, \\\"name\\\": \\\"John Doe\\\", \\\"age\\\": 30, \\\"city\\\": \\\"New York\\\"}\","
                        + "\"fullDocumentBeforeChange\":null,"
                        + "\"source\":{\"ts_ms\":1741869368000,\"snapshot\":\"false\"},"
                        + "\"ts_ms\":1741869368365,"
                        + "\"ns\":{\"db\":\"testDB\",\"coll\":\""
                        + newTableName
                        + "\"},"
                        + "\"to\":null,"
                        + "\"documentKey\":\"{\\\"_id\\\": {\\\"$oid\\\": \\\"67d2d13807fe0c4336070cfd\\\"}}\","
                        + "\"updateDescription\":null,"
                        + "\"clusterTime\":\"{\\\"$timestamp\\\": {\\\"t\\\": 1741869368, \\\"i\\\": 2}}\","
                        + "\"txnNumber\":null,"
                        + "\"lsid\":null"
                        + "}";
        schemaChange.setSchemaChangeManager(mockSchemaManager);
        schemaChange.setDorisSystem(mockDorisSystem);

        JsonNode recordRoot = objectMapper.readTree(record);
        boolean result = schemaChange.schemaChange(recordRoot);
        Assert.assertTrue(
                tableMapping.containsValue(
                        dbName
                                + "."
                                + new TableNameConverter(prefix, suffix).convert(newTableName)));
        Assert.assertTrue(result);
    }
}
