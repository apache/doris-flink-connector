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

import org.apache.doris.flink.deserialization.converter.DorisRowConverter;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer.Builder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DorisRowConverterTest implements Serializable {

    @Test
    public void testConvert() throws IOException {
        ResolvedSchema SCHEMA =
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
                        Column.physical("f11", DataTypes.DECIMAL(10,2)),
                        Column.physical("f12", DataTypes.TIMESTAMP_WITH_TIME_ZONE()),
                        Column.physical("f13", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
                        Column.physical("f14", DataTypes.DATE()),
                        Column.physical("f15", DataTypes.CHAR(1)),
                        Column.physical("f16", DataTypes.VARCHAR(256)));

        DorisRowConverter converter = new DorisRowConverter((RowType) SCHEMA.toPhysicalRowDataType().getLogicalType());

        LocalDateTime time1 = LocalDateTime.of(2021, 1, 1, 8, 0, 0);
        LocalDateTime time2 = LocalDateTime.of(2021, 1, 1, 8, 0, 0);
        LocalDate date1 = LocalDate.of(2021, 1, 1);
        List record = Arrays.asList(null,true,1.2F,1.2345D,24,10,(byte) 1, (short) 32,64,128L, BigDecimal.valueOf(10.123),time1,time2, date1,"a","doris");
        GenericRowData rowData = converter.convertInternal(record);

        RowDataSerializer serializer = new Builder()
                .setFieldType(SCHEMA.getColumnDataTypes().toArray(new DataType[0]))
                .setType("csv")
                .setFieldDelimiter("|")
                .setFieldNames(new String[]{"f1","f2","f3","f4","f5","f6","f7","f8","f9","f10","f11","f12","f13","f14","f15","f16"})
                .build();
        String s = new String(serializer.serialize(rowData).getRow());
        Assert.assertEquals("\\N|true|1.2|1.2345|24|10|1|32|64|128|10.12|2021-01-01 08:00:00.0|2021-01-01 08:00:00.0|2021-01-01|a|doris", s);
    }

    @Test
    public void testExternalConvert() {
        ResolvedSchema SCHEMA =
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
                        Column.physical("f11", DataTypes.DECIMAL(10,2)),
                        Column.physical("f12", DataTypes.TIMESTAMP_WITH_TIME_ZONE()),
                        Column.physical("f13", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
                        Column.physical("f14", DataTypes.DATE()),
                        Column.physical("f15", DataTypes.CHAR(1)),
                        Column.physical("f16", DataTypes.VARCHAR(256)));
        DorisRowConverter converter = new DorisRowConverter((RowType) SCHEMA.toPhysicalRowDataType().getLogicalType());
        LocalDateTime time1 = LocalDateTime.of(2021, 1, 1, 8, 0, 0);
        LocalDateTime time2 = LocalDateTime.of(2021, 1, 1, 8, 0, 0);
        LocalDate date1 = LocalDate.of(2021, 1, 1);
        GenericRowData rowData = GenericRowData.of(null, true, 1.2F, 1.2345D, 24, 10, (byte) 1, (short) 32, 64, 128L,
                DecimalData.fromBigDecimal(BigDecimal.valueOf(10.123), 5, 3),
                TimestampData.fromLocalDateTime(time1), TimestampData.fromLocalDateTime(time2),
                (int) date1.toEpochDay(), StringData.fromString("a"), StringData.fromString("doris"));
        List row = new ArrayList();
        for (int i = 0; i < rowData.getArity(); i++) {
            row.add(converter.convertExternal(rowData, i));
        }
        Assert.assertEquals("[null, true, 1.2, 1.2345, 24, 10, 1, 32, 64, 128, 10.123, 2021-01-01 08:00:00.0, 2021-01-01 08:00:00.0, 2021-01-01, a, doris]", row.toString());
    }
}
