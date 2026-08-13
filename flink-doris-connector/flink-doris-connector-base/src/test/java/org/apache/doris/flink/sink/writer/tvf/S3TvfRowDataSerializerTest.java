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

package org.apache.doris.flink.sink.writer.tvf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class S3TvfRowDataSerializerTest {

    @Test
    public void testSerializeSelectedColumnsInConfiguredOrder() throws Exception {
        S3TvfRowDataSerializer serializer = createSerializer(false);
        GenericRowData row = createRow(RowKind.INSERT);

        String json = new String(serializer.serialize(row).getRow(), StandardCharsets.UTF_8);

        Assert.assertEquals("{\"weight\":\"60.2\",\"id\":\"3\"}", json);
    }

    @Test
    public void testSerializeDeleteSign() throws Exception {
        S3TvfRowDataSerializer serializer = createSerializer(true);

        String insertJson =
                new String(
                        serializer.serialize(createRow(RowKind.UPDATE_AFTER)).getRow(),
                        StandardCharsets.UTF_8);
        String deleteJson =
                new String(
                        serializer.serialize(createRow(RowKind.DELETE)).getRow(),
                        StandardCharsets.UTF_8);

        Assert.assertEquals(
                "{\"weight\":\"60.2\",\"id\":\"3\",\"__DORIS_DELETE_SIGN__\":\"0\"}", insertJson);
        Assert.assertEquals(
                "{\"weight\":\"60.2\",\"id\":\"3\",\"__DORIS_DELETE_SIGN__\":\"1\"}", deleteJson);
    }

    private static S3TvfRowDataSerializer createSerializer(boolean deletable) {
        return new S3TvfRowDataSerializer(
                new String[] {"id", "name", "weight"},
                new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()},
                Arrays.asList("weight", "id"),
                deletable);
    }

    private static GenericRowData createRow(RowKind rowKind) {
        GenericRowData row = new GenericRowData(3);
        row.setField(0, 3);
        row.setField(1, StringData.fromString("test"));
        row.setField(2, 60.2);
        row.setRowKind(rowKind);
        return row;
    }
}
