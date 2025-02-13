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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.doris.flink.sink.writer.WriteMode;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_BATCH_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_EXEC_MEM_LIMIT_DEFAULT_STR;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_DEFAULT;

/** Options for the Doris connector. */
@PublicEvolving
public class DorisConfigOptions {

    public static final String IDENTIFIER = "doris";
    // common option
    public static final ConfigOption<String> FENODES =
            ConfigOptions.key("fenodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("doris fe http address.");
    public static final ConfigOption<String> BENODES =
            ConfigOptions.key("benodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("doris be http address.");
    public static final ConfigOption<String> TABLE_IDENTIFIER =
            ConfigOptions.key("table.identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris table name.");
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris user name.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris password.");
    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("jdbc-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("doris jdbc url address.");
    public static final ConfigOption<Boolean> AUTO_REDIRECT =
            ConfigOptions.key("auto-redirect")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Use automatic redirection of fe without explicitly obtaining the be list");

    // source config options
    // This is compatible with the previous writing method.
    // Some expressions may not be pushed down by FlinkSQL.
    @Deprecated
    public static final ConfigOption<String> DORIS_FILTER_QUERY =
            ConfigOptions.key("doris.filter.query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering");

    @Deprecated
    public static final ConfigOption<String> DORIS_READ_FIELD =
            ConfigOptions.key("doris.read.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of column names in the Doris table, separated by commas");

    public static final ConfigOption<Integer> DORIS_TABLET_SIZE =
            ConfigOptions.key("doris.request.tablet.size")
                    .intType()
                    .defaultValue(DORIS_TABLET_SIZE_DEFAULT)
                    .withDescription(
                            "The number of Doris Tablets corresponding to a Partition. The smaller this value is set, the more Partitions will be generated. This improves the parallelism on the Flink side, but at the same time puts more pressure on Doris.");
    public static final ConfigOption<Duration> DORIS_REQUEST_CONNECT_TIMEOUT_MS =
            ConfigOptions.key("doris.request.connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT))
                    .withDescription("Connection timeout for sending requests to Doris");
    public static final ConfigOption<Duration> DORIS_REQUEST_READ_TIMEOUT_MS =
            ConfigOptions.key("doris.request.read.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT))
                    .withDescription("Read timeout for sending requests to Doris");
    public static final ConfigOption<Duration> DORIS_REQUEST_QUERY_TIMEOUT_S =
            ConfigOptions.key("doris.request.query.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT))
                    .withDescription(
                            "The timeout time for querying Doris, the default value is 1 hour, -1 means no timeout limit");
    public static final ConfigOption<Integer> DORIS_REQUEST_RETRIES =
            ConfigOptions.key("doris.request.retries")
                    .intType()
                    .defaultValue(DORIS_REQUEST_RETRIES_DEFAULT)
                    .withDescription("Number of retries to send requests to Doris");
    public static final ConfigOption<Boolean> DORIS_DESERIALIZE_ARROW_ASYNC =
            ConfigOptions.key("doris.deserialize.arrow.async")
                    .booleanType()
                    .defaultValue(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                    .withDescription(
                            "Whether to support asynchronous conversion of Arrow format to RowBatch needed for connector iterations");
    public static final ConfigOption<Integer> DORIS_DESERIALIZE_QUEUE_SIZE =
            ConfigOptions.key("doris.deserialize.queue.size")
                    .intType()
                    .defaultValue(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                    .withDescription(
                            "Asynchronous conversion of internal processing queue in Arrow format, effective when doris.deserialize.arrow.async is true");
    public static final ConfigOption<Integer> DORIS_BATCH_SIZE =
            ConfigOptions.key("doris.batch.size")
                    .intType()
                    .defaultValue(DORIS_BATCH_SIZE_DEFAULT)
                    .withDescription(
                            "The maximum number of rows to read data from BE at a time. Increasing this value reduces the number of connections established between Flink and Doris. Thereby reducing the additional time overhead caused by network delay.");
    public static final ConfigOption<MemorySize> DORIS_EXEC_MEM_LIMIT =
            ConfigOptions.key("doris.exec.mem.limit")
                    .memoryType()
                    .defaultValue(MemorySize.parse(DORIS_EXEC_MEM_LIMIT_DEFAULT_STR))
                    .withDescription("Memory limit for a single query. The default is 8192mb.");
    public static final ConfigOption<Boolean> SOURCE_USE_OLD_API =
            ConfigOptions.key("source.use-old-api")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read data using the new interface defined according to the FLIP-27 specification,default false");

    // Lookup options
    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The max number of rows of lookup cache, over this value, the oldest rows will "
                                    + "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is "
                                    + "specified.");

    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("The cache time to live.");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The max retry times if lookup database failed.");

    public static final ConfigOption<Integer> LOOKUP_JDBC_READ_BATCH_SIZE =
            ConfigOptions.key("lookup.jdbc.read.batch.size")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "when dimension table query, save the maximum number of batches.");

    public static final ConfigOption<Integer> LOOKUP_JDBC_READ_BATCH_QUEUE_SIZE =
            ConfigOptions.key("lookup.jdbc.read.batch.queue-size")
                    .intType()
                    .defaultValue(256)
                    .withDescription("dimension table query request buffer queue size.");

    public static final ConfigOption<Integer> LOOKUP_JDBC_READ_THREAD_SIZE =
            ConfigOptions.key("lookup.jdbc.read.thread-size")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "the number of threads for dimension table query, each query occupies a JDBC connection");

    public static final ConfigOption<Boolean> LOOKUP_JDBC_ASYNC =
            ConfigOptions.key("lookup.jdbc.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to set async lookup");

    // sink config options
    public static final ConfigOption<Boolean> SINK_ENABLE_2PC =
            ConfigOptions.key("sink.enable-2pc")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("enable 2PC while loading");

    public static final ConfigOption<Duration> SINK_CHECK_INTERVAL =
            ConfigOptions.key("sink.check-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "check exception with the interval while loading, The default is 0, disabling the checker thread");
    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if writing records to database failed.");
    public static final ConfigOption<MemorySize> SINK_BUFFER_SIZE =
            ConfigOptions.key("sink.buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1mb"))
                    .withDescription("the buffer size to cache data for stream load.");
    public static final ConfigOption<Integer> SINK_BUFFER_COUNT =
            ConfigOptions.key("sink.buffer-count")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the buffer count to cache data for stream load.");
    public static final ConfigOption<String> SINK_LABEL_PREFIX =
            ConfigOptions.key("sink.label-prefix")
                    .stringType()
                    .defaultValue("")
                    .withDescription("the unique label prefix.");
    public static final ConfigOption<Boolean> SINK_ENABLE_DELETE =
            ConfigOptions.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("whether to enable the delete function");

    public static final ConfigOption<String> SINK_WRITE_MODE =
            ConfigOptions.key("sink.write-mode")
                    .stringType()
                    .defaultValue(WriteMode.STREAM_LOAD.name())
                    .withDescription("Write mode, supports stream_load, stream_load_batch");

    public static final ConfigOption<Boolean> SINK_IGNORE_COMMIT_ERROR =
            ConfigOptions.key("sink.ignore.commit-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to ignore commit errors. Usually used when the checkpoint cannot be restored to skip the commit of txn. The default is false.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    public static final ConfigOption<Boolean> SINK_ENABLE_BATCH_MODE =
            ConfigOptions.key("sink.enable.batch-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable batch write mode");

    public static final ConfigOption<Integer> SINK_FLUSH_QUEUE_SIZE =
            ConfigOptions.key("sink.flush.queue-size")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Queue length for async stream load, default is 2");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(500000)
                    .withDescription(
                            "The maximum number of flush items in each batch, the default is 5w");

    public static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_BYTES =
            ConfigOptions.key("sink.buffer-flush.max-bytes")
                    .memoryType()
                    .defaultValue(MemorySize.parse("100mb"))
                    .withDescription(
                            "The maximum number of bytes flushed in each batch, the default is 10MB");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "the flush interval mills, over this time, asynchronous threads will flush data. The "
                                    + "default value is 10s.");

    public static final ConfigOption<Boolean> SINK_IGNORE_UPDATE_BEFORE =
            ConfigOptions.key("sink.ignore.update-before")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "In the CDC scenario, when the primary key of the upstream is inconsistent with that of the downstream, the update-before data needs to be passed to the downstream as deleted data, otherwise the data cannot be deleted.\n"
                                    + "The default is to ignore, that is, perform upsert semantics.");

    public static final ConfigOption<Boolean> SINK_USE_CACHE =
            ConfigOptions.key("sink.use-cache")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use buffer cache for breakpoint resume");
    public static final ConfigOption<Boolean> USE_FLIGHT_SQL =
            ConfigOptions.key("source.use-flight-sql")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription("use flight sql flag");
    public static final ConfigOption<Integer> FLIGHT_SQL_PORT =
            ConfigOptions.key("source.flight-sql-port")
                    .intType()
                    .defaultValue(9040)
                    .withDescription("flight sql port");
    // Prefix for Doris StreamLoad specific properties.
    public static final String STREAM_LOAD_PROP_PREFIX = "sink.properties.";

    public static Properties getStreamLoadProp(Map<String, String> tableOptions) {
        final Properties streamLoadProp = new Properties();

        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (entry.getKey().startsWith(STREAM_LOAD_PROP_PREFIX)) {
                String subKey = entry.getKey().substring(STREAM_LOAD_PROP_PREFIX.length());
                streamLoadProp.put(subKey, entry.getValue());
            }
        }
        return streamLoadProp;
    }
}
