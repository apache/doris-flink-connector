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

package org.apache.doris.flink.sink.writer;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.arrow.serializers.ArrowSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** test for RowDataSerializer. */
public class TestRowDataSerializer {
    static GenericRowData rowData;
    static DataType[] dataTypes;
    static String[] fieldNames;

    @BeforeClass
    public static void setUp() {
        rowData = new GenericRowData(3);
        rowData.setField(0, 3);
        rowData.setField(1, StringData.fromString("test"));
        rowData.setField(2, 60.2);
        rowData.setRowKind(RowKind.INSERT);
        dataTypes = new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};
        fieldNames = new String[] {"id", "name", "weight"};
    }

    @Test
    public void testSerializeCsv() throws IOException {
        RowDataSerializer.Builder builder = RowDataSerializer.builder();
        builder.setFieldNames(fieldNames)
                .setFieldType(dataTypes)
                .setType("csv")
                .setFieldDelimiter("|")
                .enableDelete(false);
        RowDataSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(rowData).getRow();
        Assert.assertArrayEquals("3|test|60.2".getBytes(StandardCharsets.UTF_8), serializedValue);
    }

    @Test
    public void testSerializeJson() throws IOException {
        RowDataSerializer.Builder builder = RowDataSerializer.builder();
        builder.setFieldNames(fieldNames)
                .setFieldType(dataTypes)
                .setType("json")
                .setFieldDelimiter("|")
                .enableDelete(false);
        RowDataSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(rowData).getRow();
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> valueMap =
                objectMapper.readValue(
                        new String(serializedValue, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, String>>() {});
        Assert.assertEquals("3", valueMap.get("id"));
        Assert.assertEquals("test", valueMap.get("name"));
        Assert.assertEquals("60.2", valueMap.get("weight"));
    }

    @Test
    public void testSerializeCsvWithSign() throws IOException {
        RowDataSerializer.Builder builder = RowDataSerializer.builder();
        builder.setFieldNames(fieldNames)
                .setFieldType(dataTypes)
                .setType("csv")
                .setFieldDelimiter("|")
                .enableDelete(true);
        RowDataSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(rowData).getRow();
        Assert.assertArrayEquals("3|test|60.2|0".getBytes(StandardCharsets.UTF_8), serializedValue);
    }

    @Test
    public void testSerializeJsonWithSign() throws IOException {
        RowDataSerializer.Builder builder = RowDataSerializer.builder();
        builder.setFieldNames(fieldNames)
                .setFieldType(dataTypes)
                .setType("json")
                .setFieldDelimiter("|")
                .enableDelete(true);
        RowDataSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(rowData).getRow();
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> valueMap =
                objectMapper.readValue(
                        new String(serializedValue, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, String>>() {});
        Assert.assertEquals("3", valueMap.get("id"));
        Assert.assertEquals("test", valueMap.get("name"));
        Assert.assertEquals("60.2", valueMap.get("weight"));
        Assert.assertEquals("0", valueMap.get("__DORIS_DELETE_SIGN__"));
    }

    @Test
    public void testParseDeleteSign() {
        RowDataSerializer.Builder builder = RowDataSerializer.builder();
        builder.setFieldNames(fieldNames)
                .setFieldType(dataTypes)
                .setType("json")
                .setFieldDelimiter("|")
                .enableDelete(true);
        RowDataSerializer serializer = builder.build();
        Assert.assertEquals("0", serializer.parseDeleteSign(RowKind.INSERT));
        Assert.assertEquals("0", serializer.parseDeleteSign(RowKind.UPDATE_AFTER));
        Assert.assertEquals("1", serializer.parseDeleteSign(RowKind.DELETE));
        Assert.assertEquals("1", serializer.parseDeleteSign(RowKind.UPDATE_BEFORE));
    }

    @Test
    public void testArrowType() throws Exception {
        RowDataSerializer serializer =
                RowDataSerializer.builder()
                        .setFieldNames(fieldNames)
                        .setFieldType(dataTypes)
                        .setType("arrow")
                        .enableDelete(false)
                        .build();

        // write data to binary
        serializer.initial();
        serializer.serialize(rowData);
        byte[] serializedValue = serializer.flush().getRow();

        // read data from binary
        LogicalType[] logicalTypes = TypeConversions.fromDataToLogicalType(dataTypes);
        RowType rowType = RowType.of(logicalTypes, fieldNames);
        ArrowSerializer arrowSerializer = new ArrowSerializer(rowType, rowType);
        ByteArrayInputStream input = new ByteArrayInputStream(serializedValue);
        arrowSerializer.open(input, new ByteArrayOutputStream(0));
        int cnt = arrowSerializer.load();
        RowData data = arrowSerializer.read(0);

        Assert.assertEquals(1, cnt);
        Assert.assertEquals(3, data.getInt(0));
        Assert.assertEquals("test", data.getString(1).toString());
        Assert.assertEquals(60.2, data.getDouble(2), 0.001);
    }

    @Test(expected = IllegalStateException.class)
    public void testBuild() {
        RowDataSerializer serializer =
                RowDataSerializer.builder()
                        .setFieldNames(fieldNames)
                        .setFieldType(dataTypes)
                        .setType("csv")
                        .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildArrow() {
        RowDataSerializer serializer =
                RowDataSerializer.builder()
                        .setFieldNames(fieldNames)
                        .setFieldType(dataTypes)
                        .setType("arrow")
                        .enableDelete(true)
                        .build();
    }
}
