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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.writer.WriteMode;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.doris.flink.table.DorisConfigOptions.AUTO_REDIRECT;
import static org.apache.doris.flink.table.DorisConfigOptions.BENODES;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_BATCH_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_DESERIALIZE_ARROW_ASYNC;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_DESERIALIZE_QUEUE_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_EXEC_MEM_LIMIT;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_FILTER_QUERY;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_READ_FIELD;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_REQUEST_QUERY_TIMEOUT_S;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_REQUEST_READ_TIMEOUT_MS;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_REQUEST_RETRIES;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_TABLET_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.FENODES;
import static org.apache.doris.flink.table.DorisConfigOptions.FLIGHT_SQL_PORT;
import static org.apache.doris.flink.table.DorisConfigOptions.IDENTIFIER;
import static org.apache.doris.flink.table.DorisConfigOptions.JDBC_URL;
import static org.apache.doris.flink.table.DorisConfigOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.doris.flink.table.DorisConfigOptions.LOOKUP_CACHE_TTL;
import static org.apache.doris.flink.table.DorisConfigOptions.LOOKUP_JDBC_ASYNC;
import static org.apache.doris.flink.table.DorisConfigOptions.LOOKUP_JDBC_READ_BATCH_QUEUE_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.LOOKUP_JDBC_READ_BATCH_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.LOOKUP_JDBC_READ_THREAD_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.LOOKUP_MAX_RETRIES;
import static org.apache.doris.flink.table.DorisConfigOptions.PASSWORD;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_BUFFER_COUNT;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_BUFFER_FLUSH_MAX_BYTES;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_BUFFER_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_CHECK_INTERVAL;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_ENABLE_2PC;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_ENABLE_BATCH_MODE;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_ENABLE_DELETE;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_FLUSH_QUEUE_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_IGNORE_COMMIT_ERROR;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_IGNORE_UPDATE_BEFORE;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_LABEL_PREFIX;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_MAX_RETRIES;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_PARALLELISM;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_USE_CACHE;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_WRITE_MODE;
import static org.apache.doris.flink.table.DorisConfigOptions.SOURCE_USE_OLD_API;
import static org.apache.doris.flink.table.DorisConfigOptions.STREAM_LOAD_PROP_PREFIX;
import static org.apache.doris.flink.table.DorisConfigOptions.TABLE_IDENTIFIER;
import static org.apache.doris.flink.table.DorisConfigOptions.USERNAME;
import static org.apache.doris.flink.table.DorisConfigOptions.USE_FLIGHT_SQL;

/**
 * The {@link DorisDynamicTableFactory} translates the catalog table to a table source.
 *
 * <p>Because the table source requires a decoding format, we are discovering the format using the
 * provided {@link FactoryUtil} for convenience.
 */
public final class DorisDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER; // used for matching to `connector = '...'`
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FENODES);
        options.add(TABLE_IDENTIFIER);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FENODES);
        options.add(BENODES);
        options.add(TABLE_IDENTIFIER);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(JDBC_URL);
        options.add(AUTO_REDIRECT);

        options.add(DORIS_READ_FIELD);
        options.add(DORIS_FILTER_QUERY);
        options.add(DORIS_TABLET_SIZE);
        options.add(DORIS_REQUEST_CONNECT_TIMEOUT_MS);
        options.add(DORIS_REQUEST_READ_TIMEOUT_MS);
        options.add(DORIS_REQUEST_QUERY_TIMEOUT_S);
        options.add(DORIS_REQUEST_RETRIES);
        options.add(DORIS_DESERIALIZE_ARROW_ASYNC);
        options.add(DORIS_DESERIALIZE_QUEUE_SIZE);
        options.add(DORIS_BATCH_SIZE);
        options.add(DORIS_EXEC_MEM_LIMIT);
        options.add(LOOKUP_CACHE_MAX_ROWS);
        options.add(LOOKUP_CACHE_TTL);
        options.add(LOOKUP_MAX_RETRIES);
        options.add(LOOKUP_JDBC_ASYNC);
        options.add(LOOKUP_JDBC_READ_BATCH_SIZE);
        options.add(LOOKUP_JDBC_READ_THREAD_SIZE);
        options.add(LOOKUP_JDBC_READ_BATCH_QUEUE_SIZE);

        options.add(SINK_CHECK_INTERVAL);
        options.add(SINK_ENABLE_2PC);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_ENABLE_DELETE);
        options.add(SINK_LABEL_PREFIX);
        options.add(SINK_BUFFER_SIZE);
        options.add(SINK_BUFFER_COUNT);
        options.add(SINK_PARALLELISM);
        options.add(SINK_IGNORE_UPDATE_BEFORE);

        options.add(SINK_ENABLE_BATCH_MODE);
        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(SINK_BUFFER_FLUSH_MAX_BYTES);
        options.add(SINK_FLUSH_QUEUE_SIZE);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);

        options.add(SINK_USE_CACHE);

        options.add(SOURCE_USE_OLD_API);
        options.add(SINK_WRITE_MODE);
        options.add(SINK_IGNORE_COMMIT_ERROR);

        options.add(USE_FLIGHT_SQL);
        options.add(FLIGHT_SQL_PORT);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // validate all options
        helper.validateExcept(STREAM_LOAD_PROP_PREFIX);
        // get the validated options
        final ReadableConfig options = helper.getOptions();
        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        // create and return dynamic table source
        return new DorisDynamicTableSource(
                getDorisOptions(helper.getOptions()),
                getDorisReadOptions(helper.getOptions()),
                getDorisLookupOptions(helper.getOptions()),
                physicalSchema,
                context.getPhysicalRowDataType());
    }

    private DorisOptions getDorisOptions(ReadableConfig readableConfig) {
        final String fenodes = readableConfig.get(FENODES);
        final String benodes = readableConfig.get(BENODES);
        final DorisOptions.Builder builder =
                DorisOptions.builder()
                        .setFenodes(fenodes)
                        .setBenodes(benodes)
                        .setAutoRedirect(readableConfig.get(AUTO_REDIRECT))
                        .setJdbcUrl(readableConfig.get(JDBC_URL))
                        .setTableIdentifier(readableConfig.get(TABLE_IDENTIFIER));

        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    private DorisReadOptions getDorisReadOptions(ReadableConfig readableConfig) {
        final DorisReadOptions.Builder builder = DorisReadOptions.builder();
        builder.setDeserializeArrowAsync(readableConfig.get(DORIS_DESERIALIZE_ARROW_ASYNC))
                .setDeserializeQueueSize(readableConfig.get(DORIS_DESERIALIZE_QUEUE_SIZE))
                .setExecMemLimit(readableConfig.get(DORIS_EXEC_MEM_LIMIT).getBytes())
                .setFilterQuery(readableConfig.get(DORIS_FILTER_QUERY))
                .setReadFields(readableConfig.get(DORIS_READ_FIELD))
                .setRequestQueryTimeoutS(
                        (int) readableConfig.get(DORIS_REQUEST_QUERY_TIMEOUT_S).getSeconds())
                .setRequestBatchSize(readableConfig.get(DORIS_BATCH_SIZE))
                .setRequestConnectTimeoutMs(
                        (int) readableConfig.get(DORIS_REQUEST_CONNECT_TIMEOUT_MS).toMillis())
                .setRequestReadTimeoutMs(
                        (int) readableConfig.get(DORIS_REQUEST_READ_TIMEOUT_MS).toMillis())
                .setRequestRetries(readableConfig.get(DORIS_REQUEST_RETRIES))
                .setRequestTabletSize(readableConfig.get(DORIS_TABLET_SIZE))
                .setUseOldApi(readableConfig.get(SOURCE_USE_OLD_API))
                .setUseFlightSql(readableConfig.get(USE_FLIGHT_SQL))
                .setFlightSqlPort(readableConfig.get(FLIGHT_SQL_PORT));
        return builder.build();
    }

    private DorisExecutionOptions getDorisExecutionOptions(
            ReadableConfig readableConfig, Properties streamLoadProp) {
        final DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder();
        builder.setCheckInterval((int) readableConfig.get(SINK_CHECK_INTERVAL).toMillis());
        builder.setMaxRetries(readableConfig.get(SINK_MAX_RETRIES));
        builder.setBufferSize((int) readableConfig.get(SINK_BUFFER_SIZE).getBytes());
        builder.setBufferCount(readableConfig.get(SINK_BUFFER_COUNT));
        builder.setLabelPrefix(readableConfig.get(SINK_LABEL_PREFIX));
        builder.setStreamLoadProp(streamLoadProp);
        builder.setDeletable(readableConfig.get(SINK_ENABLE_DELETE));
        builder.setIgnoreUpdateBefore(readableConfig.get(SINK_IGNORE_UPDATE_BEFORE));
        builder.setIgnoreCommitError(readableConfig.get(SINK_IGNORE_COMMIT_ERROR));

        if (!readableConfig.get(SINK_ENABLE_2PC)) {
            builder.disable2PC();
        } else if (readableConfig.getOptional(SINK_ENABLE_2PC).isPresent()) {
            // force open 2pc
            builder.enable2PC();
        }

        builder.setWriteMode(WriteMode.of(readableConfig.get(SINK_WRITE_MODE)));
        builder.setBatchMode(readableConfig.get(SINK_ENABLE_BATCH_MODE));
        // Compatible with previous versions
        if (readableConfig.get(SINK_ENABLE_BATCH_MODE)) {
            builder.setWriteMode(WriteMode.STREAM_LOAD_BATCH);
        }
        builder.setFlushQueueSize(readableConfig.get(SINK_FLUSH_QUEUE_SIZE));
        builder.setBufferFlushMaxRows(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        builder.setBufferFlushMaxBytes(
                (int) readableConfig.get(SINK_BUFFER_FLUSH_MAX_BYTES).getBytes());
        builder.setBufferFlushIntervalMs(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        builder.setUseCache(readableConfig.get(SINK_USE_CACHE));
        return builder.build();
    }

    private DorisLookupOptions getDorisLookupOptions(ReadableConfig readableConfig) {
        final DorisLookupOptions.Builder builder = DorisLookupOptions.builder();
        builder.setCacheExpireMs(readableConfig.get(LOOKUP_CACHE_TTL).toMillis());
        builder.setCacheMaxSize(readableConfig.get(LOOKUP_CACHE_MAX_ROWS));
        builder.setMaxRetryTimes(readableConfig.get(LOOKUP_MAX_RETRIES));
        builder.setJdbcReadBatchSize(readableConfig.get(LOOKUP_JDBC_READ_BATCH_SIZE));
        builder.setJdbcReadBatchQueueSize(readableConfig.get(LOOKUP_JDBC_READ_BATCH_QUEUE_SIZE));
        builder.setJdbcReadThreadSize(readableConfig.get(LOOKUP_JDBC_READ_THREAD_SIZE));
        builder.setAsync(readableConfig.get(LOOKUP_JDBC_ASYNC));
        return builder.build();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // validate all options
        helper.validateExcept(STREAM_LOAD_PROP_PREFIX);
        // sink parallelism
        final Integer parallelism = helper.getOptions().get(SINK_PARALLELISM);

        Properties streamLoadProp =
                DorisConfigOptions.getStreamLoadProp(context.getCatalogTable().getOptions());
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        // create and return dynamic table sink
        return new DorisDynamicTableSink(
                getDorisOptions(helper.getOptions()),
                getDorisReadOptions(helper.getOptions()),
                getDorisExecutionOptions(helper.getOptions(), streamLoadProp),
                physicalSchema,
                parallelism);
    }
}
