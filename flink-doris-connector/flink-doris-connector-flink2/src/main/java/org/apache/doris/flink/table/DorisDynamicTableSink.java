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

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.overwrite.DorisOverwriteManager;
import org.apache.doris.flink.sink.overwrite.DorisOverwriteOptions;
import org.apache.doris.flink.sink.overwrite.DorisPreparedOverwrite;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.doris.flink.sink.writer.LoadConstants.COLUMNS_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.DORIS_DELETE_SIGN;
import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.FORMAT_KEY;

/** DorisDynamicTableSink. */
public class DorisDynamicTableSink implements DynamicTableSink, SupportsOverwrite {
    private static final Logger LOG = LoggerFactory.getLogger(DorisDynamicTableSink.class);
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private final DorisExecutionOptions executionOptions;
    private final TableSchema tableSchema;
    private final Integer sinkParallelism;
    private boolean overwrite = false;

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
        DorisOptions sinkOptions = options;
        DorisOverwriteOptions overwriteOptions = null;
        Boolean targetUniqueKeyType = null;
        if (overwrite) {
            if (!context.isBounded()) {
                throw new IllegalStateException("Streaming mode not support overwrite.");
            }
            Preconditions.checkArgument(
                    WriteMode.STREAM_LOAD.equals(executionOptions.getWriteMode()),
                    "INSERT OVERWRITE only supports STREAM_LOAD write mode.");
            Preconditions.checkArgument(
                    executionOptions.enabled2PC(),
                    "INSERT OVERWRITE requires sink.enable-2pc=true.");
            targetUniqueKeyType = RestService.isUniqueKeyType(options, readOptions, LOG);
            Preconditions.checkArgument(
                    !targetUniqueKeyType || executionOptions.force2PC(),
                    "INSERT OVERWRITE on unique key table requires explicitly setting sink.enable-2pc=true.");
            DorisPreparedOverwrite preparedOverwrite =
                    DorisOverwriteManager.prepareOverwrite(options, executionOptions);
            sinkOptions = preparedOverwrite.getSinkOptions();
            overwriteOptions = preparedOverwrite.getOverwriteOptions();
        }

        Properties loadProperties = executionOptions.getStreamLoadProp();
        boolean deletable =
                executionOptions.getDeletable()
                        && (targetUniqueKeyType == null
                                ? RestService.isUniqueKeyType(sinkOptions, readOptions, LOG)
                                : targetUniqueKeyType);
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

        DorisSink.Builder<RowData> dorisSinkBuilder = DorisSink.builder();
        dorisSinkBuilder
                .setDorisOptions(sinkOptions)
                .setDorisReadOptions(readOptions)
                .setDorisExecutionOptions(executionOptions)
                .setOverwriteOptions(overwriteOptions)
                .setSerializer(serializerBuilder.build());
        DorisSink<RowData> dorisSink = dorisSinkBuilder.build();
        return SinkV2Provider.of(dorisSink, sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        DorisDynamicTableSink sink =
                new DorisDynamicTableSink(
                        options, readOptions, executionOptions, tableSchema, sinkParallelism);
        sink.overwrite = overwrite;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "Doris Table Sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisDynamicTableSink that = (DorisDynamicTableSink) o;
        return Objects.equals(options, that.options)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(executionOptions, that.executionOptions)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(sinkParallelism, that.sinkParallelism)
                && Objects.equals(overwrite, that.overwrite);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                options, readOptions, executionOptions, tableSchema, sinkParallelism, overwrite);
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }
}
