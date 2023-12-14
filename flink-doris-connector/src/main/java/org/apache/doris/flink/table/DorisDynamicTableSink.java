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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.batch.DorisBatchSink;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.doris.flink.sink.writer.LoadConstants.COLUMNS_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.DORIS_DELETE_SIGN;
import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.FORMAT_KEY;

/** DorisDynamicTableSink. */
public class DorisDynamicTableSink implements DynamicTableSink {
    private static final Logger LOG = LoggerFactory.getLogger(DorisDynamicTableSink.class);
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private final DorisExecutionOptions executionOptions;
    private final TableSchema tableSchema;
    private final Integer sinkParallelism;

    public DorisDynamicTableSink(
            DorisOptions options,
            DorisReadOptions readOptions,
            DorisExecutionOptions executionOptions,
            TableSchema tableSchema,
            Integer sinkParallelism) {
        this.options = options;
        this.readOptions = readOptions;
        this.executionOptions = executionOptions;
        this.tableSchema = tableSchema;
        this.sinkParallelism = sinkParallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        if (executionOptions.getIgnoreUpdateBefore()) {
            return ChangelogMode.upsert();
        } else {
            return ChangelogMode.all();
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        Properties loadProperties = executionOptions.getStreamLoadProp();
        boolean deletable =
                executionOptions.getDeletable()
                        && RestService.isUniqueKeyType(options, readOptions, LOG);
        if (!loadProperties.containsKey(COLUMNS_KEY)) {
            String[] fieldNames = tableSchema.getFieldNames();
            Preconditions.checkState(fieldNames != null && fieldNames.length > 0);
            String columns =
                    String.join(
                            ",",
                            Arrays.stream(fieldNames)
                                    .map(
                                            item ->
                                                    String.format(
                                                            "`%s`", item.trim().replace("`", "")))
                                    .collect(Collectors.toList()));
            if (deletable) {
                columns = String.format("%s,%s", columns, DORIS_DELETE_SIGN);
            }
            loadProperties.put(COLUMNS_KEY, columns);
        }

        RowDataSerializer.Builder serializerBuilder = RowDataSerializer.builder();
        serializerBuilder
                .setFieldNames(tableSchema.getFieldNames())
                .setFieldType(tableSchema.getFieldDataTypes())
                .setType(loadProperties.getProperty(FORMAT_KEY, CSV))
                .enableDelete(deletable)
                .setFieldDelimiter(
                        loadProperties.getProperty(FIELD_DELIMITER_KEY, FIELD_DELIMITER_DEFAULT));

        if (!executionOptions.enableBatchMode()) {
            DorisSink.Builder<RowData> dorisSinkBuilder = DorisSink.builder();
            dorisSinkBuilder
                    .setDorisOptions(options)
                    .setDorisReadOptions(readOptions)
                    .setDorisExecutionOptions(executionOptions)
                    .setSerializer(serializerBuilder.build());
            return SinkV2Provider.of(dorisSinkBuilder.build(), sinkParallelism);
        } else {
            DorisBatchSink.Builder<RowData> dorisBatchSinkBuilder = DorisBatchSink.builder();
            dorisBatchSinkBuilder
                    .setDorisOptions(options)
                    .setDorisReadOptions(readOptions)
                    .setDorisExecutionOptions(executionOptions)
                    .setSerializer(serializerBuilder.build());
            return SinkV2Provider.of(dorisBatchSinkBuilder.build(), sinkParallelism);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new DorisDynamicTableSink(
                options, readOptions, executionOptions, tableSchema, sinkParallelism);
    }

    @Override
    public String asSummaryString() {
        return "Doris Table Sink";
    }
}
