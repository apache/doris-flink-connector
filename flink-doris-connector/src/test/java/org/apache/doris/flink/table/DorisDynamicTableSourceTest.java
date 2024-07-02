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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.enumerator.PendingSplitsCheckpoint;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.utils.FactoryMocks;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.apache.doris.flink.utils.FactoryMocks.SCHEMA;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

public class DorisDynamicTableSourceTest {

    @Test
    public void testDorisUseNewApi() {
        DorisReadOptions.Builder builder = OptionUtils.dorisReadOptionsBuilder();
        builder.setUseOldApi(false);
        final DorisDynamicTableSource actualDorisSource =
                new DorisDynamicTableSource(
                        OptionUtils.buildDorisOptions(),
                        builder.build(),
                        DorisLookupOptions.builder().build(),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        FactoryMocks.PHYSICAL_DATA_TYPE);
        ScanTableSource.ScanRuntimeProvider provider =
                actualDorisSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertDorisSource(provider);
    }

    @Test
    public void testDorisUseNewApiDefault() {
        final DorisDynamicTableSource actualDorisSource =
                new DorisDynamicTableSource(
                        OptionUtils.buildDorisOptions(),
                        OptionUtils.buildDorisReadOptions(),
                        DorisLookupOptions.builder().build(),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        FactoryMocks.PHYSICAL_DATA_TYPE);
        ScanTableSource.ScanRuntimeProvider provider =
                actualDorisSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertDorisSource(provider);
    }

    @Test
    public void testDorisUseOldApi() {
        DorisReadOptions.Builder builder = OptionUtils.dorisReadOptionsBuilder();
        builder.setUseOldApi(true);
        MockedStatic<RestService> restServiceMockedStatic = mockStatic(RestService.class);
        restServiceMockedStatic
                .when(() -> RestService.findPartitions(any(), any(), any()))
                .thenReturn(
                        Collections.singletonList(
                                new PartitionDefinition("", "", "", new HashSet<>(), "")));
        final DorisDynamicTableSource actualDorisSource =
                new DorisDynamicTableSource(
                        OptionUtils.buildDorisOptions(),
                        builder.build(),
                        DorisLookupOptions.builder().build(),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        FactoryMocks.PHYSICAL_DATA_TYPE);
        ScanTableSource.ScanRuntimeProvider provider =
                actualDorisSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertDorisInputFormat(provider);
        restServiceMockedStatic.close();
    }

    private void assertDorisInputFormat(ScanTableSource.ScanRuntimeProvider provider) {
        assertThat(provider, instanceOf(InputFormatProvider.class));
        final InputFormatProvider inputFormatProvider = (InputFormatProvider) provider;

        InputFormat<RowData, DorisTableInputSplit> inputFormat =
                (InputFormat<RowData, DorisTableInputSplit>)
                        inputFormatProvider.createInputFormat();
        assertThat(inputFormat, instanceOf(DorisRowDataInputFormat.class));
    }

    private void assertDorisSource(ScanTableSource.ScanRuntimeProvider provider) {
        assertThat(provider, instanceOf(SourceProvider.class));
        final SourceProvider sourceProvider = (SourceProvider) provider;

        Source<RowData, DorisSourceSplit, PendingSplitsCheckpoint> source =
                (Source<RowData, DorisSourceSplit, PendingSplitsCheckpoint>)
                        sourceProvider.createSource();
        assertThat(source, instanceOf(DorisSource.class));
    }

    @Test
    public void testApplyProjection() {
        int[][] projectionArray = new int[2][1];
        projectionArray[0][0] = 0;
        projectionArray[1][0] = 2;
        DataType projectionDataType =
                ResolvedSchema.of(
                                Column.physical("a", DataTypes.STRING()),
                                Column.physical("c", DataTypes.BOOLEAN()))
                        .toPhysicalRowDataType();
        DorisReadOptions readOptions = OptionUtils.dorisReadOptionsBuilder().build();
        final DorisDynamicTableSource actualDorisSource =
                new DorisDynamicTableSource(
                        OptionUtils.buildDorisOptions(),
                        readOptions,
                        DorisLookupOptions.builder().build(),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        FactoryMocks.PHYSICAL_DATA_TYPE);
        actualDorisSource.applyProjection(projectionArray, projectionDataType);
        Assert.assertEquals(readOptions.getReadFields(), "`a`, `c`");
    }

    @Test
    public void testFilter() {
        DorisReadOptions readOptions = OptionUtils.dorisReadOptionsBuilder().build();
        final DorisDynamicTableSource actualDorisSource =
                new DorisDynamicTableSource(
                        OptionUtils.buildDorisOptions(),
                        readOptions,
                        DorisLookupOptions.builder().build(),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        FactoryMocks.PHYSICAL_DATA_TYPE);
        ResolvedExpression aRef = new FieldReferenceExpression("a", DataTypes.STRING(), 0, 2);
        ResolvedExpression aRefCharLength =
                new CallExpression(
                        BuiltInFunctionDefinitions.CHAR_LENGTH,
                        Collections.singletonList(aRef),
                        DataTypes.INT());
        ResolvedExpression aExp =
                new CallExpression(
                        BuiltInFunctionDefinitions.LESS_THAN,
                        Arrays.asList(aRefCharLength, valueLiteral(10)),
                        DataTypes.BOOLEAN());
        actualDorisSource.applyFilters(Collections.singletonList(aExp));
        Assert.assertTrue(actualDorisSource.getResolvedFilterQuery().isEmpty());

        ResolvedExpression a1Ref = new FieldReferenceExpression("a", DataTypes.STRING(), 0, 2);
        ResolvedExpression a1Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(a1Ref, valueLiteral("doris")),
                        DataTypes.BOOLEAN());
        actualDorisSource.applyFilters(Arrays.asList(a1Exp));
        assertEquals(Arrays.asList("(a = 'doris')"), actualDorisSource.getResolvedFilterQuery());

        ResolvedExpression b1Ref = new FieldReferenceExpression("b", DataTypes.INT(), 0, 2);
        ResolvedExpression b1Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(b1Ref, valueLiteral(1)),
                        DataTypes.BOOLEAN());
        actualDorisSource.getResolvedFilterQuery().clear();
        actualDorisSource.applyFilters(Arrays.asList(b1Exp));
        assertEquals(Arrays.asList("(b = 1)"), actualDorisSource.getResolvedFilterQuery());

        actualDorisSource.getResolvedFilterQuery().clear();
        actualDorisSource.applyFilters(
                Arrays.asList(
                        b1Exp,
                        new CallExpression(
                                BuiltInFunctionDefinitions.NOT_EQUALS,
                                Arrays.asList(b1Ref, valueLiteral(1)),
                                DataTypes.BOOLEAN()),
                        new CallExpression(
                                BuiltInFunctionDefinitions.GREATER_THAN,
                                Arrays.asList(b1Ref, valueLiteral(1)),
                                DataTypes.BOOLEAN()),
                        new CallExpression(
                                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                Arrays.asList(b1Ref, valueLiteral(1)),
                                DataTypes.BOOLEAN()),
                        new CallExpression(
                                BuiltInFunctionDefinitions.LESS_THAN,
                                Arrays.asList(b1Ref, valueLiteral(1)),
                                DataTypes.BOOLEAN()),
                        new CallExpression(
                                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                Arrays.asList(b1Ref, valueLiteral(1)),
                                DataTypes.BOOLEAN())));
        assertEquals(
                Arrays.asList("(b = 1)", "(b <> 1)", "(b > 1)", "(b >= 1)", "(b < 1)", "(b <= 1)"),
                actualDorisSource.getResolvedFilterQuery());

        actualDorisSource.getResolvedFilterQuery().clear();
        actualDorisSource.applyFilters(
                Arrays.asList(
                        new CallExpression(
                                BuiltInFunctionDefinitions.OR,
                                Arrays.asList(a1Exp, b1Exp),
                                DataTypes.BOOLEAN())));
        assertEquals(
                Arrays.asList("((a = 'doris') OR (b = 1))"),
                actualDorisSource.getResolvedFilterQuery());

        actualDorisSource.getResolvedFilterQuery().clear();
        actualDorisSource.applyFilters(
                Arrays.asList(
                        new CallExpression(
                                BuiltInFunctionDefinitions.AND,
                                Arrays.asList(a1Exp, b1Exp),
                                DataTypes.BOOLEAN())));
        assertEquals(
                Arrays.asList("((a = 'doris') AND (b = 1))"),
                actualDorisSource.getResolvedFilterQuery());

        ResolvedExpression aLikeExp =
                new CallExpression(
                        BuiltInFunctionDefinitions.LIKE,
                        Arrays.asList(a1Ref, valueLiteral("d")),
                        DataTypes.BOOLEAN());
        actualDorisSource.getResolvedFilterQuery().clear();
        actualDorisSource.applyFilters(Collections.singletonList(aLikeExp));
        assertEquals(Arrays.asList("(a LIKE 'd')"), actualDorisSource.getResolvedFilterQuery());

        ResolvedExpression aInExp =
                new CallExpression(
                        BuiltInFunctionDefinitions.IN,
                        Arrays.asList(a1Ref, valueLiteral("doris")),
                        DataTypes.BOOLEAN());
        actualDorisSource.getResolvedFilterQuery().clear();
        actualDorisSource.applyFilters(Collections.singletonList(aInExp));
        assertTrue(actualDorisSource.getResolvedFilterQuery().isEmpty());

        ResolvedExpression aNoNullExp =
                new CallExpression(
                        BuiltInFunctionDefinitions.IS_NOT_NULL,
                        Arrays.asList(a1Ref),
                        DataTypes.BOOLEAN());
        actualDorisSource.getResolvedFilterQuery().clear();
        actualDorisSource.applyFilters(Collections.singletonList(aNoNullExp));
        assertEquals(Arrays.asList("(a IS NOT NULL)"), actualDorisSource.getResolvedFilterQuery());

        ResolvedExpression aNullExp =
                new CallExpression(
                        BuiltInFunctionDefinitions.IS_NULL,
                        Arrays.asList(a1Ref),
                        DataTypes.BOOLEAN());
        actualDorisSource.getResolvedFilterQuery().clear();
        actualDorisSource.applyFilters(Collections.singletonList(aNullExp));
        assertEquals(Arrays.asList("(a IS NULL)"), actualDorisSource.getResolvedFilterQuery());
    }

    @Test
    public void testFilterDate() {
        DorisReadOptions readOptions = OptionUtils.dorisReadOptionsBuilder().build();
        final DorisDynamicTableSource actualDorisSource =
                new DorisDynamicTableSource(
                        OptionUtils.buildDorisOptions(),
                        readOptions,
                        DorisLookupOptions.builder().build(),
                        TableSchema.fromResolvedSchema(FactoryMocks.SCHEMA_DT),
                        FactoryMocks.SCHEMA_DT.toPhysicalRowDataType());
        ResolvedExpression dRef = new FieldReferenceExpression("d", DataTypes.DATE(), 0, 2);
        ResolvedExpression dExp =
                new CallExpression(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(dRef, valueLiteral(LocalDate.of(2021, 1, 1))),
                        DataTypes.BOOLEAN());
        actualDorisSource.applyFilters(Arrays.asList(dExp));
        assertEquals(
                Arrays.asList("(d = '2021-01-01')"), actualDorisSource.getResolvedFilterQuery());

        ResolvedExpression eRef = new FieldReferenceExpression("e", DataTypes.TIMESTAMP(), 0, 2);
        ResolvedExpression eExp =
                new CallExpression(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(eRef, valueLiteral("2021-01-01 11:12:13")),
                        DataTypes.BOOLEAN());
        actualDorisSource.getResolvedFilterQuery().clear();
        actualDorisSource.applyFilters(Arrays.asList(eExp));
        assertEquals(
                Arrays.asList("(e = '2021-01-01 11:12:13')"),
                actualDorisSource.getResolvedFilterQuery());
    }
}
