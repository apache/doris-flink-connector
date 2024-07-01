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

package org.apache.doris.flink.deserialization.convert;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.doris.flink.deserialization.converter.DorisRowConverter;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer.Builder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisRowConverterTest implements Serializable {
    @Test
    public void testConvert() throws IOException {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("f1", DataTypes.NULL()),
                        Column.physical("f2", DataTypes.BOOLEAN()),
                        Column.physical("f3", DataTypes.FLOAT()),
                        Column.physical("f4", DataTypes.DOUBLE()),
                        Column.physical("f5", DataTypes.INTERVAL(DataTypes.YEAR())),
                        Column.physical("f6", DataTypes.INTERVAL(DataTypes.DAY())),
                        Column.physical("f7", DataTypes.TINYINT()),
                        Column.physical("f8", DataTypes.SMALLINT()),
                        Column.physical("f9", DataTypes.INT()),
                        Column.physical("f10", DataTypes.BIGINT()),
                        Column.physical("f11", DataTypes.DECIMAL(10, 2)),
                        Column.physical("f12", DataTypes.TIMESTAMP_WITH_TIME_ZONE()),
                        Column.physical("f13", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
                        Column.physical("f14", DataTypes.DATE()),
                        Column.physical("f15", DataTypes.CHAR(1)),
                        Column.physical("f16", DataTypes.VARCHAR(256)),
                        Column.physical("f17", DataTypes.TIMESTAMP_WITH_TIME_ZONE()),
                        Column.physical("f18", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()));

        DorisRowConverter converter =
                new DorisRowConverter((RowType) schema.toPhysicalRowDataType().getLogicalType());
        // Doris DatetimeV2 supports up to 6 decimal places (microseconds).
        LocalDateTime time1 = LocalDateTime.of(2021, 1, 1, 8, 1, 1, 1000);
        LocalDateTime time2 = LocalDateTime.of(2021, 1, 1, 8, 1, 1, 1000);
        LocalDate date1 = LocalDate.of(2021, 1, 1);
        Timestamp timestamp1 = Timestamp.valueOf(time1);
        Timestamp timestamp2 = Timestamp.valueOf(time2);
        List<Object> record =
                Arrays.asList(
                        null,
                        true,
                        1.2F,
                        1.2345D,
                        24,
                        10,
                        (byte) 1,
                        (short) 32,
                        64,
                        128L,
                        BigDecimal.valueOf(10.123),
                        time1,
                        time2,
                        date1,
                        "a",
                        "doris",
                        timestamp1,
                        timestamp2);
        GenericRowData rowData = converter.convertInternal(record);
        DorisRowConverter converterWithDataType =
                new DorisRowConverter(schema.getColumnDataTypes().toArray(new DataType[0]));
        GenericRowData genericRowData = converterWithDataType.convertInternal(record);
        Assert.assertEquals(rowData, genericRowData);

        RowDataSerializer serializer =
                new Builder()
                        .setFieldType(schema.getColumnDataTypes().toArray(new DataType[0]))
                        .setType("csv")
                        .setFieldDelimiter("|")
                        .setFieldNames(
                                new String[] {
                                    "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
                                    "f11", "f12", "f13", "f14", "f15", "f16", "f17", "f18"
                                })
                        .build();
        String s = new String(serializer.serialize(rowData).getRow());
        Assert.assertEquals(
                "\\N|true|1.2|1.2345|24|10|1|32|64|128|10.12|2021-01-01 08:01:01.000001|2021-01-01 08:01:01.000001|2021-01-01|a|doris|2021-01-01 08:01:01.000001|2021-01-01 08:01:01.000001",
                s);
    }

    @Test
    public void testExternalConvert() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("f1", DataTypes.NULL()),
                        Column.physical("f2", DataTypes.BOOLEAN()),
                        Column.physical("f3", DataTypes.FLOAT()),
                        Column.physical("f4", DataTypes.DOUBLE()),
                        Column.physical("f5", DataTypes.INTERVAL(DataTypes.YEAR())),
                        Column.physical("f6", DataTypes.INTERVAL(DataTypes.DAY())),
                        Column.physical("f7", DataTypes.TINYINT()),
                        Column.physical("f8", DataTypes.SMALLINT()),
                        Column.physical("f9", DataTypes.INT()),
                        Column.physical("f10", DataTypes.BIGINT()),
                        Column.physical("f11", DataTypes.DECIMAL(10, 2)),
                        Column.physical("f12", DataTypes.TIMESTAMP_WITH_TIME_ZONE()),
                        Column.physical("f13", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
                        Column.physical("f14", DataTypes.DATE()),
                        Column.physical("f15", DataTypes.CHAR(1)),
                        Column.physical("f16", DataTypes.VARCHAR(256)),
                        Column.physical("f17", DataTypes.TIMESTAMP_WITH_TIME_ZONE()),
                        Column.physical("f18", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()));
        DorisRowConverter converter =
                new DorisRowConverter((RowType) schema.toPhysicalRowDataType().getLogicalType());
        // Doris DatetimeV2 supports up to 6 decimal places (microseconds).
        LocalDateTime time1 = LocalDateTime.of(2021, 1, 1, 8, 1, 1, 1000);
        LocalDateTime time2 = LocalDateTime.of(2021, 1, 1, 8, 1, 1, 1000);
        Timestamp timestamp1 = Timestamp.valueOf(time1);
        Timestamp timestamp2 = Timestamp.valueOf(time2);
        LocalDate date1 = LocalDate.of(2021, 1, 1);
        GenericRowData rowData =
                GenericRowData.of(
                        null,
                        true,
                        1.2F,
                        1.2345D,
                        24,
                        10,
                        (byte) 1,
                        (short) 32,
                        64,
                        128L,
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(10.123), 5, 3),
                        TimestampData.fromLocalDateTime(time1),
                        TimestampData.fromLocalDateTime(time2),
                        (int) date1.toEpochDay(),
                        StringData.fromString("a"),
                        StringData.fromString("doris"),
                        TimestampData.fromTimestamp(timestamp1),
                        TimestampData.fromTimestamp(timestamp2));
        List<Object> row = new ArrayList<>();
        for (int i = 0; i < rowData.getArity(); i++) {
            row.add(converter.convertExternal(rowData, i));
        }

        Assert.assertEquals(
                "[null, true, 1.2, 1.2345, 24, 10, 1, 32, 64, 128, 10.123, 2021-01-01 08:01:01.000001, 2021-01-01 08:01:01.000001, 2021-01-01, a, doris, 2021-01-01 08:01:01.000001, 2021-01-01 08:01:01.000001]",
                row.toString());
    }

    @Test
    public void testMapInternalConvert() throws IOException {

        ResolvedSchema schema = getRowMapSchema();
        DorisRowConverter converter =
                new DorisRowConverter((RowType) schema.toPhysicalRowDataType().getLogicalType());
        // Doris DatetimeV2 supports up to 6 decimal places (microseconds).
        LocalDateTime time1 = LocalDateTime.of(2021, 1, 1, 8, 1, 1, 1000);
        LocalDateTime time2 = LocalDateTime.of(2021, 1, 1, 8, 1, 1, 1000);
        LocalDateTime time3 = LocalDateTime.of(2021, 1, 1, 8, 1, 1, 1000);
        LocalDate date1 = LocalDate.of(2021, 1, 1);
        Map<Boolean, Boolean> booleanMap = createMapAndPut(new HashMap<>(), true, false);
        Map<Float, Float> floatMap = createMapAndPut(new HashMap<>(), 1.2f, 1.3f);
        Map<Double, Double> doubleMap = createMapAndPut(new HashMap<>(), 1.2345d, 1.2345d);
        Map<Integer, Integer> intervalYearMap = createMapAndPut(new HashMap<>(), 24, 24);
        Map<Integer, Integer> intervalDayMap = createMapAndPut(new HashMap<>(), 10, 10);
        Map<Byte, Byte> tinyIntMap = createMapAndPut(new HashMap<>(), (byte) 1, (byte) 1);
        Map<Short, Short> shortIntMap = createMapAndPut(new HashMap<>(), (short) 32, (short) 32);
        Map<Integer, Integer> intMap = createMapAndPut(new HashMap<>(), 64, 64);
        Map<Long, Long> longMap = createMapAndPut(new HashMap<>(), 128L, 128L);
        Map<BigDecimal, BigDecimal> decimalMap =
                createMapAndPut(
                        new HashMap<>(), BigDecimal.valueOf(10.123), BigDecimal.valueOf(10.123));
        Map<LocalDateTime, LocalDateTime> timestampWithZoneMap =
                createMapAndPut(new HashMap<>(), time1, time1);
        Map<LocalDateTime, LocalDateTime> timestampWithLocalZoneMap =
                createMapAndPut(new HashMap<>(), time2, time2);
        Map<LocalDateTime, LocalDateTime> timestampNoLTZ =
                createMapAndPut(new HashMap<>(), time3, time3);
        Map<LocalDate, LocalDate> dateMap = createMapAndPut(new HashMap<>(), date1, date1);
        Map<Character, Character> charMap = createMapAndPut(new HashMap<>(), 'a', 'a');
        Map<String, String> stringMap = createMapAndPut(new HashMap<>(), "doris", "doris");

        List<Object> record =
                Arrays.asList(
                        booleanMap,
                        floatMap,
                        doubleMap,
                        intervalYearMap,
                        intervalDayMap,
                        tinyIntMap,
                        shortIntMap,
                        intMap,
                        longMap,
                        decimalMap,
                        timestampWithZoneMap,
                        timestampWithLocalZoneMap,
                        timestampNoLTZ,
                        dateMap,
                        charMap,
                        stringMap);
        GenericRowData rowData = converter.convertInternal(record);

        RowDataSerializer serializer =
                new Builder()
                        .setFieldType(schema.getColumnDataTypes().toArray(new DataType[0]))
                        .setType("csv")
                        .setFieldDelimiter("|")
                        .setFieldNames(
                                new String[] {
                                    "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
                                    "f11", "f12", "f13", "f14", "f15", "f16"
                                })
                        .build();
        String s = new String(serializer.serialize(rowData).getRow());
        Assert.assertEquals(
                "{\"true\":\"false\"}|{\"1.2\":\"1.3\"}|{\"1.2345\":\"1.2345\"}|{\"24\":\"24\"}|{\"10\":\"10\"}|{\"1\":\"1\"}|{\"32\":\"32\"}|{\"64\":\"64\"}|{\"128\":\"128\"}|{\"10.12\":\"10.12\"}|{\"2021-01-01 08:01:01.000001\":\"2021-01-01 08:01:01.000001\"}|{\"2021-01-01 08:01:01.000001\":\"2021-01-01 08:01:01.000001\"}|{\"2021-01-01 08:01:01.000001\":\"2021-01-01 08:01:01.000001\"}|{\"2021-01-01\":\"2021-01-01\"}|{\"a\":\"a\"}|{\"doris\":\"doris\"}",
                s);
    }

    @Test
    public void testMapExternalConvert() {

        ResolvedSchema schema = getRowMapSchema();
        DorisRowConverter converter =
                new DorisRowConverter((RowType) schema.toPhysicalRowDataType().getLogicalType());
        // Doris DatetimeV2 supports up to 6 decimal places (microseconds).
        LocalDateTime time1 = LocalDateTime.of(2021, 1, 1, 8, 1, 1, 1000);
        LocalDateTime time2 = LocalDateTime.of(2021, 1, 1, 8, 1, 1, 1000);
        LocalDateTime time3 = LocalDateTime.of(2021, 1, 1, 8, 1, 1, 1000);
        LocalDate date1 = LocalDate.of(2021, 1, 1);

        Map<Boolean, Boolean> booleanMap = createMapAndPut(new HashMap<>(), true, false);
        Map<Float, Float> floatMap = createMapAndPut(new HashMap<>(), 1.2f, 1.3f);
        Map<Double, Double> doubleMap = createMapAndPut(new HashMap<>(), 1.2345d, 1.2345d);
        Map<Integer, Integer> intervalYearMap = createMapAndPut(new HashMap<>(), 24, 24);
        Map<Integer, Integer> intervalDayMap = createMapAndPut(new HashMap<>(), 10, 10);
        Map<Byte, Byte> tinyIntMap = createMapAndPut(new HashMap<>(), (byte) 1, (byte) 1);
        Map<Short, Short> shortIntMap = createMapAndPut(new HashMap<>(), (short) 32, (short) 32);
        Map<Integer, Integer> intMap = createMapAndPut(new HashMap<>(), 64, 64);
        Map<Long, Long> longMap = createMapAndPut(new HashMap<>(), 128L, 128L);
        Map<BigDecimal, BigDecimal> decimalMap =
                createMapAndPut(
                        new HashMap<>(), BigDecimal.valueOf(10.123), BigDecimal.valueOf(10.123));
        Map<TimestampData, TimestampData> timestampWithZoneMap =
                createMapAndPut(
                        new HashMap<>(),
                        TimestampData.fromLocalDateTime(time1),
                        TimestampData.fromLocalDateTime(time1));
        Map<TimestampData, TimestampData> timestampWithLocalZoneMap =
                createMapAndPut(
                        new HashMap<>(),
                        TimestampData.fromLocalDateTime(time2),
                        TimestampData.fromLocalDateTime(time2));
        Map<TimestampData, TimestampData> timestampNoLTZ =
                createMapAndPut(
                        new HashMap<>(),
                        TimestampData.fromLocalDateTime(time3),
                        TimestampData.fromLocalDateTime(time3));
        Map<Integer, Integer> dateMap =
                createMapAndPut(
                        new HashMap<>(), (int) date1.toEpochDay(), (int) date1.toEpochDay());
        Map<Character, Character> charMap = createMapAndPut(new HashMap<>(), 'a', 'a');
        Map<String, String> stringMap = createMapAndPut(new HashMap<>(), "doris", "doris");
        GenericRowData rowData =
                GenericRowData.of(
                        new GenericMapData(booleanMap),
                        new GenericMapData(floatMap),
                        new GenericMapData(doubleMap),
                        new GenericMapData(intervalYearMap),
                        new GenericMapData(intervalDayMap),
                        new GenericMapData(tinyIntMap),
                        new GenericMapData(shortIntMap),
                        new GenericMapData(intMap),
                        new GenericMapData(longMap),
                        new GenericMapData(decimalMap),
                        new GenericMapData(timestampWithZoneMap),
                        new GenericMapData(timestampWithLocalZoneMap),
                        new GenericMapData(timestampNoLTZ),
                        new GenericMapData(dateMap),
                        new GenericMapData(charMap),
                        new GenericMapData(stringMap));

        List<Object> row = new ArrayList<>();
        for (int i = 0; i < rowData.getArity(); i++) {
            row.add(converter.convertExternal(rowData, i));
        }
        Assert.assertEquals(
                "[{\"true\":\"false\"}, {\"1.2\":\"1.3\"}, {\"1.2345\":\"1.2345\"}, {\"24\":\"24\"}, {\"10\":\"10\"}, {\"1\":\"1\"}, {\"32\":\"32\"}, {\"64\":\"64\"}, {\"128\":\"128\"}, {\"10.123\":\"10.123\"}, {\"2021-01-01 08:01:01.000001\":\"2021-01-01 08:01:01.000001\"}, {\"2021-01-01 08:01:01.000001\":\"2021-01-01 08:01:01.000001\"}, {\"2021-01-01 08:01:01.000001\":\"2021-01-01 08:01:01.000001\"}, {\"2021-01-01\":\"2021-01-01\"}, {\"a\":\"a\"}, {\"doris\":\"doris\"}]",
                row.toString());
    }

    /** generate map data. */
    public static <K, V> Map<K, V> createMapAndPut(Map<K, V> map, K key, V value) {
        map.put(key, value);
        return map;
    }

    public static ResolvedSchema getRowMapSchema() {
        return ResolvedSchema.of(
                Column.physical("f1", DataTypes.MAP(DataTypes.BOOLEAN(), DataTypes.BOOLEAN())),
                Column.physical("f2", DataTypes.MAP(DataTypes.FLOAT(), DataTypes.FLOAT())),
                Column.physical("f3", DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.DOUBLE())),
                Column.physical(
                        "f4",
                        DataTypes.MAP(
                                DataTypes.INTERVAL(DataTypes.YEAR()),
                                DataTypes.INTERVAL(DataTypes.YEAR()))),
                Column.physical(
                        "f5",
                        DataTypes.MAP(
                                DataTypes.INTERVAL(DataTypes.DAY()),
                                DataTypes.INTERVAL(DataTypes.DAY()))),
                Column.physical("f6", DataTypes.MAP(DataTypes.TINYINT(), DataTypes.TINYINT())),
                Column.physical("f7", DataTypes.MAP(DataTypes.SMALLINT(), DataTypes.SMALLINT())),
                Column.physical("f8", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                Column.physical("f9", DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT())),
                Column.physical(
                        "f10", DataTypes.MAP(DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(10, 2))),
                Column.physical(
                        "f11",
                        DataTypes.MAP(
                                DataTypes.TIMESTAMP_WITH_TIME_ZONE(),
                                DataTypes.TIMESTAMP_WITH_TIME_ZONE())),
                Column.physical(
                        "f12",
                        DataTypes.MAP(
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())),
                Column.physical("f13", DataTypes.MAP(DataTypes.TIMESTAMP(), DataTypes.TIMESTAMP())),
                Column.physical("f14", DataTypes.MAP(DataTypes.DATE(), DataTypes.DATE())),
                Column.physical("f15", DataTypes.MAP(DataTypes.CHAR(1), DataTypes.CHAR(1))),
                Column.physical(
                        "f16", DataTypes.MAP(DataTypes.VARCHAR(256), DataTypes.VARCHAR(256))));
    }
}
