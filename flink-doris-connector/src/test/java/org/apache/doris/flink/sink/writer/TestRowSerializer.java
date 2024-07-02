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
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.sink.writer.serializer.RowSerializer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** test for RowSerializer. */
public class TestRowSerializer {
    static Row row;
    static DataType[] dataTypes;
    static String[] fieldNames;

    @BeforeClass
    public static void setUp() {
        row = new Row(3);
        row.setField(0, 3);
        row.setField(1, "test");
        row.setField(2, 60.2);
        row.setKind(RowKind.INSERT);
        dataTypes = new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};
        fieldNames = new String[] {"id", "name", "weight"};
    }

    @Test
    public void testSerializeCsv() throws IOException {
        RowSerializer.Builder builder = RowSerializer.builder();
        builder.setFieldNames(fieldNames)
                .setFieldType(dataTypes)
                .setType("csv")
                .setFieldDelimiter("|")
                .enableDelete(false);
        RowSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(row).getRow();
        Assert.assertArrayEquals("3|test|60.2".getBytes(StandardCharsets.UTF_8), serializedValue);
    }

    @Test
    public void testSerializeJson() throws IOException {
        RowSerializer.Builder builder = RowSerializer.builder();
        builder.setFieldNames(fieldNames)
                .setFieldType(dataTypes)
                .setType("json")
                .setFieldDelimiter("|")
                .enableDelete(false);
        RowSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(row).getRow();
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
        RowSerializer.Builder builder = RowSerializer.builder();
        builder.setFieldNames(fieldNames)
                .setFieldType(dataTypes)
                .setType("csv")
                .setFieldDelimiter("|")
                .enableDelete(true);
        RowSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(row).getRow();
        Assert.assertArrayEquals("3|test|60.2|0".getBytes(StandardCharsets.UTF_8), serializedValue);
    }

    @Test
    public void testSerializeJsonWithSign() throws IOException {
        RowSerializer.Builder builder = RowSerializer.builder();
        builder.setFieldNames(fieldNames)
                .setFieldType(dataTypes)
                .setType("json")
                .setFieldDelimiter("|")
                .enableDelete(true);
        RowSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(row).getRow();
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

    @Test(expected = IllegalStateException.class)
    public void testBuild() {
        RowSerializer serializer =
                RowSerializer.builder()
                        .setFieldNames(fieldNames)
                        .setFieldType(dataTypes)
                        .setType("csv")
                        .build();
    }
}
