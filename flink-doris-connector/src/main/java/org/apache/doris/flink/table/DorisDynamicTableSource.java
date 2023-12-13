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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.RowDataDeserializationSchema;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.source.DorisSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The {@link DorisDynamicTableSource} is used during planning.
 *
 * <p>In our example, we don't implement any of the available ability interfaces such as {@link
 * SupportsFilterPushDown} or {@link SupportsProjectionPushDown}. Therefore, the main logic can be
 * found in {@link #getScanRuntimeProvider(ScanContext)} where we instantiate the required {@link
 * SourceFunction} and its {@link DeserializationSchema} for runtime. Both instances are
 * parameterized to return internal data structures (i.e. {@link RowData}).
 */
public final class DorisDynamicTableSource implements ScanTableSource, LookupTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(DorisDynamicTableSource.class);
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private DorisLookupOptions lookupOptions;
    private TableSchema physicalSchema;

    public DorisDynamicTableSource(
            DorisOptions options,
            DorisReadOptions readOptions,
            DorisLookupOptions lookupOptions,
            TableSchema physicalSchema) {
        this.options = options;
        this.lookupOptions = lookupOptions;
        this.readOptions = readOptions;
        this.physicalSchema = physicalSchema;
    }

    public DorisDynamicTableSource(
            DorisOptions options, DorisReadOptions readOptions, TableSchema physicalSchema) {
        this.options = options;
        this.readOptions = readOptions;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // in our example the format decides about the changelog mode
        // but it could also be the source itself
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        readOptions.setReadFields(
                Arrays.stream(physicalSchema.getFieldNames())
                        .map(item -> String.format("`%s`", item.trim().replace("`", "")))
                        .collect(Collectors.joining(", ")));

        if (readOptions.getUseOldApi()) {
            List<PartitionDefinition> dorisPartitions;
            try {
                dorisPartitions = RestService.findPartitions(options, readOptions, LOG);
            } catch (DorisException e) {
                throw new RuntimeException("Failed fetch doris partitions");
            }
            DorisRowDataInputFormat.Builder builder =
                    DorisRowDataInputFormat.builder()
                            .setFenodes(options.getFenodes())
                            .setBenodes(options.getBenodes())
                            .setUsername(options.getUsername())
                            .setPassword(options.getPassword())
                            .setTableIdentifier(options.getTableIdentifier())
                            .setPartitions(dorisPartitions)
                            .setReadOptions(readOptions)
                            .setRowType((RowType) physicalSchema.toRowDataType().getLogicalType());
            return InputFormatProvider.of(builder.build());
        } else {
            // Read data using the interface of the FLIP-27 specification
            DorisSource<RowData> build =
                    DorisSource.<RowData>builder()
                            .setDorisReadOptions(readOptions)
                            .setDorisOptions(options)
                            .setDeserializer(
                                    new RowDataDeserializationSchema(
                                            (RowType)
                                                    physicalSchema
                                                            .toRowDataType()
                                                            .getLogicalType()))
                            .build();
            return SourceProvider.of(build);
        }
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        DataType physicalRowDataType = physicalSchema.toRowDataType();
        String[] keyNames = new String[context.getKeys().length];
        int[] keyIndexs = new int[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            keyNames[i] = DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
            keyIndexs[i] = innerKeyArr[0];
        }
        if (lookupOptions.isAsync()) {
            return AsyncTableFunctionProvider.of(
                    new DorisRowDataAsyncLookupFunction(
                            options,
                            lookupOptions,
                            DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                            DataType.getFieldDataTypes(physicalRowDataType)
                                    .toArray(new DataType[0]),
                            keyNames,
                            keyIndexs));
        } else {
            return TableFunctionProvider.of(
                    new DorisRowDataJdbcLookupFunction(
                            options,
                            lookupOptions,
                            DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                            DataType.getFieldDataTypes(physicalRowDataType)
                                    .toArray(new DataType[0]),
                            keyNames,
                            keyIndexs));
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new DorisDynamicTableSource(options, readOptions, lookupOptions, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "Doris Table Source";
    }
}
