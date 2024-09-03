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

package org.apache.doris.flink.table;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_BATCH_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_EXEC_MEM_LIMIT_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_DEFAULT;
import static org.apache.doris.flink.utils.FactoryMocks.SCHEMA;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DorisDynamicTableFactoryTest {

    @Test
    public void testDorisSourceProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("doris.request.query.timeout", "21600s");
        properties.put("doris.request.tablet.size", "1");
        properties.put("doris.batch.size", "4064");
        properties.put("doris.exec.mem.limit", "8192mb");
        properties.put("doris.deserialize.arrow.async", "false");
        properties.put("doris.deserialize.queue.size", "64");

        properties.put("lookup.cache.max-rows", "100");
        properties.put("lookup.cache.ttl", "20s");
        properties.put("lookup.max-retries", "1");
        properties.put("lookup.jdbc.async", "true");
        properties.put("lookup.jdbc.read.batch.size", "16");
        properties.put("lookup.jdbc.read.batch.queue-size", "16");
        properties.put("lookup.jdbc.read.thread-size", "1");
        properties.put("source.use-flight-sql", "false");
        properties.put("source.flight-sql-port", "9040");
        DynamicTableSource actual = createTableSource(SCHEMA, properties);
        DorisOptions options =
                DorisOptions.builder()
                        .setTableIdentifier("db.tbl")
                        .setFenodes("127.0.0.1:8030")
                        .setBenodes("127.0.0.1:8040")
                        .setAutoRedirect(true)
                        .setUsername("root")
                        .setPassword("")
                        .setJdbcUrl("jdbc:mysql://127.0.0.1:9030")
                        .build();
        DorisLookupOptions lookupOptions =
                DorisLookupOptions.builder()
                        .setCacheExpireMs(20000)
                        .setCacheMaxSize(100)
                        .setAsync(true)
                        .setJdbcReadBatchQueueSize(16)
                        .setJdbcReadBatchSize(16)
                        .setJdbcReadThreadSize(1)
                        .setMaxRetryTimes(1)
                        .build();

        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();
        readOptionBuilder
                .setDeserializeArrowAsync(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                .setDeserializeQueueSize(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                .setExecMemLimit(DORIS_EXEC_MEM_LIMIT_DEFAULT)
                .setRequestQueryTimeoutS(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
                .setRequestBatchSize(DORIS_BATCH_SIZE_DEFAULT)
                .setRequestConnectTimeoutMs(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                .setRequestReadTimeoutMs(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                .setRequestRetries(DORIS_REQUEST_RETRIES_DEFAULT)
                .setRequestTabletSize(DORIS_TABLET_SIZE_DEFAULT)
                .setUseFlightSql(false)
                .setFlightSqlPort(9040);
        DorisDynamicTableSource expected =
                new DorisDynamicTableSource(
                        options,
                        readOptionBuilder.build(),
                        lookupOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        SCHEMA.toPhysicalRowDataType());

        assertEquals(actual, expected);
    }

    @Test
    public void testDorisSinkProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("doris.request.query.timeout", "21600s");
        properties.put("doris.request.tablet.size", "1");
        properties.put("doris.batch.size", "4064");
        properties.put("doris.exec.mem.limit", "8192mb");
        properties.put("doris.deserialize.arrow.async", "false");
        properties.put("doris.deserialize.queue.size", "64");

        properties.put("sink.label-prefix", "abc");
        properties.put("sink.properties.format", "json");
        properties.put("sink.properties.read_json_by_line", "true");
        properties.put("sink.enable-delete", "true");
        properties.put("sink.enable-2pc", "true");
        properties.put("sink.buffer-size", "1MB");
        properties.put("sink.buffer-count", "3");
        properties.put("sink.max-retries", "1");
        properties.put("sink.check-interval", "10s");
        properties.put("sink.use-cache", "true");
        properties.put("sink.enable.batch-mode", "true");
        properties.put("sink.flush.queue-size", "2");
        properties.put("sink.buffer-flush.max-rows", "10000");
        properties.put("sink.buffer-flush.max-bytes", "10MB");
        properties.put("sink.buffer-flush.interval", "10s");
        properties.put("sink.ignore.update-before", "true");
        properties.put("sink.ignore.commit-error", "false");
        properties.put("sink.parallelism", "1");

        DynamicTableSink actual = createTableSink(SCHEMA, properties);
        DorisOptions options =
                DorisOptions.builder()
                        .setTableIdentifier("db.tbl")
                        .setFenodes("127.0.0.1:8030")
                        .setBenodes("127.0.0.1:8040")
                        .setAutoRedirect(true)
                        .setUsername("root")
                        .setPassword("")
                        .setJdbcUrl("jdbc:mysql://127.0.0.1:9030")
                        .build();

        Properties prop = new Properties();
        prop.put("format", "json");
        prop.put("read_json_by_line", "true");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setLabelPrefix("abc")
                        .setBufferCount(3)
                        .setBufferSize(1024 * 1024)
                        .setStreamLoadProp(prop)
                        .setMaxRetries(1)
                        .setCheckInterval(10000)
                        .setBatchMode(true)
                        .enable2PC()
                        .setBufferFlushIntervalMs(10000)
                        .setBufferFlushMaxBytes(10 * 1024 * 1024)
                        .setBufferFlushMaxRows(10000)
                        .setFlushQueueSize(2)
                        .setUseCache(true)
                        .setIgnoreCommitError(false)
                        .build();

        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();
        readOptionBuilder
                .setDeserializeArrowAsync(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                .setDeserializeQueueSize(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                .setExecMemLimit(DORIS_EXEC_MEM_LIMIT_DEFAULT)
                .setRequestQueryTimeoutS(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
                .setRequestBatchSize(DORIS_BATCH_SIZE_DEFAULT)
                .setRequestConnectTimeoutMs(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                .setRequestReadTimeoutMs(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                .setRequestRetries(DORIS_REQUEST_RETRIES_DEFAULT)
                .setRequestTabletSize(DORIS_TABLET_SIZE_DEFAULT)
                .setUseFlightSql(false)
                .setFlightSqlPort(9040);
        DorisDynamicTableSink expected =
                new DorisDynamicTableSink(
                        options,
                        readOptionBuilder.build(),
                        executionOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        1);

        assertEquals(expected, expected);
        assertNotEquals(expected, null);
        assertEquals(actual, expected);

        options.setTableIdentifier("xxxxx");
        DorisDynamicTableSink expected2 =
                new DorisDynamicTableSink(
                        options,
                        readOptionBuilder.build(),
                        executionOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        1);
        assertNotEquals(actual, expected2);
        options.setTableIdentifier("db.tbl");

        readOptionBuilder.setExecMemLimit(1L);
        DorisDynamicTableSink expected3 =
                new DorisDynamicTableSink(
                        options,
                        readOptionBuilder.build(),
                        executionOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        1);
        assertNotEquals(actual, expected3);
        readOptionBuilder.setExecMemLimit(DORIS_EXEC_MEM_LIMIT_DEFAULT);

        executionOptions.setEnable2PC(false);
        DorisDynamicTableSink expected4 =
                new DorisDynamicTableSink(
                        options,
                        readOptionBuilder.build(),
                        executionOptions,
                        TableSchema.fromResolvedSchema(SCHEMA),
                        1);
        assertNotEquals(actual, expected4);
        executionOptions.setEnable2PC(true);

        DynamicTableSink actual2 = createTableSink(SCHEMA, new HashMap<>());
        assertNotEquals(actual2, expected);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "doris");
        options.put("fenodes", "127.0.0.1:8030");
        options.put("benodes", "127.0.0.1:8040");
        options.put("jdbc-url", "jdbc:mysql://127.0.0.1:9030");
        options.put("table.identifier", "db.tbl");
        options.put("username", "root");
        options.put("password", "");
        options.put("auto-redirect", "true");
        options.put("doris.request.retries", "3");
        options.put("doris.request.connect.timeout", "30s");
        options.put("doris.request.read.timeout", "30s");
        return options;
    }
}
