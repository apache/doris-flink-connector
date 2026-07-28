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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;

import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.RowDataDeserializationSchema;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.DorisSourceScanMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** The {@link DorisDynamicTableSource} is used during planning. */
public final class DorisDynamicTableSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsFilterPushDown,
                SupportsProjectionPushDown,
                SupportsLimitPushDown {

    private static final Logger LOG = LoggerFactory.getLogger(DorisDynamicTableSource.class);
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private DorisLookupOptions lookupOptions;
    private TableSchema physicalSchema;
    private List<String> resolvedFilterQuery = new ArrayList<>();
    private DataType physicalRowDataType;

    public DorisDynamicTableSource(
            DorisOptions options,
            DorisReadOptions readOptions,
            DorisLookupOptions lookupOptions,
            TableSchema physicalSchema,
            DataType physicalRowDataType) {
        if (readOptions.getUseOldApi()
                && readOptions.getScanMode() != DorisSourceScanMode.SNAPSHOT) {
            throw new ValidationException(
                    "source.use-old-api=true only supports source.scan.mode=snapshot");
        }
        this.options = options;
        this.lookupOptions = lookupOptions;
        this.readOptions = readOptions;
        this.physicalSchema = physicalSchema;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (!readOptions.getScanMode().hasIncrementalPhase()
                || readOptions.getBinlogIncrementType()
                        == org.apache.doris.flink.source.DorisBinlogIncrementType.APPEND_ONLY) {
            return ChangelogMode.insertOnly();
        }
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        if (!resolvedFilterQuery.isEmpty()) {
            String filterQuery = resolvedFilterQuery.stream().collect(Collectors.joining(" AND "));
            if (!StringUtils.isNullOrWhitespaceOnly(readOptions.getFilterQuery())) {
                filterQuery =
                        String.format("(%s) AND (%s)", readOptions.getFilterQuery(), filterQuery);
            }
            readOptions.setFilterQuery(filterQuery);
        }

        if (StringUtils.isNullOrWhitespaceOnly(readOptions.getReadFields())) {
            String[] selectFields =
                    DataType.getFieldNames(physicalRowDataType).toArray(new String[0]);
            readOptions.setReadFields(
                    Arrays.stream(selectFields)
                            .map(item -> String.format("`%s`", item.trim().replace("`", "")))
                            .collect(Collectors.joining(", ")));
        }

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
                            .setRowType((RowType) physicalRowDataType.getLogicalType());
            return InputFormatProvider.of(builder.build());
        } else {
            // Read data using the interface of the FLIP-27 specification
            DorisSource<RowData> build =
                    DorisSource.<RowData>builder()
                            .setDorisReadOptions(readOptions)
                            .setDorisOptions(options)
                            .setDeserializer(
                                    new RowDataDeserializationSchema(
                                            (RowType) physicalRowDataType.getLogicalType()))
                            .build();
            return SourceProvider.of(build);
        }
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
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
        // filterQuery/readFields of readOption may be overwritten in union all sql
        DorisDynamicTableSource newSource =
                new DorisDynamicTableSource(
                        options,
                        readOptions.copy(),
                        lookupOptions,
                        physicalSchema,
                        physicalRowDataType);
        newSource.resolvedFilterQuery = new ArrayList<>(this.resolvedFilterQuery);
        return newSource;
    }

    @Override
    public String asSummaryString() {
        return "Doris Table Source";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        if (readOptions.getScanMode().hasIncrementalPhase()) {
            return Result.of(Collections.emptyList(), filters);
        }

        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();

        DorisExpressionVisitor expressionVisitor = new DorisExpressionVisitor();
        for (ResolvedExpression filter : filters) {
            String filterQuery = filter.accept(expressionVisitor);
            if (!StringUtils.isNullOrWhitespaceOnly(filterQuery)) {
                acceptedFilters.add(filter);
                this.resolvedFilterQuery.add(filterQuery);
            } else {
                remainingFilters.add(filter);
            }
        }
        return Result.of(acceptedFilters, remainingFilters);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
        String[] selectFields = DataType.getFieldNames(physicalRowDataType).toArray(new String[0]);
        this.readOptions.setReadFields(
                Arrays.stream(selectFields)
                        .map(item -> String.format("`%s`", item.trim().replace("`", "")))
                        .collect(Collectors.joining(", ")));
    }

    @VisibleForTesting
    public List<String> getResolvedFilterQuery() {
        return resolvedFilterQuery;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisDynamicTableSource that = (DorisDynamicTableSource) o;
        return Objects.equals(options, that.options)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(lookupOptions, that.lookupOptions)
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(resolvedFilterQuery, that.resolvedFilterQuery)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                options,
                readOptions,
                lookupOptions,
                physicalSchema,
                resolvedFilterQuery,
                physicalRowDataType);
    }

    @Override
    public void applyLimit(long limit) {
        if (readOptions.getScanMode().hasIncrementalPhase()) {
            return;
        }
        // partial limit push down to reduce the amount of data scanned
        readOptions.setRowLimit(limit);
    }
}
