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

package org.apache.doris.flink.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DorisTypeMapperTest {

    @Test
    public void testBooleanType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "BOOLEAN", 0, 0);
        assertEquals(DataTypes.BOOLEAN(), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.BOOLEAN());
        assertEquals("BOOLEAN", dorisType);
    }

    @Test
    public void testTinyIntTypeWithPrecisionZero() {
        DataType result = DorisTypeMapper.toFlinkType("col", "TINYINT", 0, 0);
        assertEquals(DataTypes.BOOLEAN(), result);
    }

    @Test
    public void testTinyIntTypeWithNonZeroPrecision() {
        DataType result = DorisTypeMapper.toFlinkType("col", "TINYINT", 1, 0);
        assertEquals(DataTypes.TINYINT(), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.TINYINT());
        assertEquals("TINYINT", dorisType);
    }

    @Test
    public void testSmallIntType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "SMALLINT", 0, 0);
        assertEquals(DataTypes.SMALLINT(), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.SMALLINT());
        assertEquals("SMALLINT", dorisType);
    }

    @Test
    public void testIntType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "INT", 0, 0);
        assertEquals(DataTypes.INT(), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.INT());
        assertEquals("INT", dorisType);
    }

    @Test
    public void testBigIntType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "BIGINT", 0, 0);
        assertEquals(DataTypes.BIGINT(), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.BIGINT());
        assertEquals("BIGINT", dorisType);
    }

    @Test
    public void testDecimalType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "DECIMAL", 10, 2);
        assertEquals(DataTypes.DECIMAL(10, 2), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.DECIMAL(10, 2));
        assertEquals("DECIMALV3(10,2)", dorisType);
    }

    @Test
    public void testDecimalV3Type() {
        DataType result = DorisTypeMapper.toFlinkType("col", "DECIMALV3", 10, 2);
        assertEquals(DataTypes.DECIMAL(10, 2), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.DECIMAL(10, 2));
        assertEquals("DECIMALV3(10,2)", dorisType);
    }

    @Test
    public void testFloatType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "FLOAT", 0, 0);
        assertEquals(DataTypes.FLOAT(), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.FLOAT());
        assertEquals("FLOAT", dorisType);
    }

    @Test
    public void testDoubleType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "DOUBLE", 0, 0);
        assertEquals(DataTypes.DOUBLE(), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.DOUBLE());
        assertEquals("DOUBLE", dorisType);
    }

    @Test
    public void testCharType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "CHAR", 10, 0);
        assertEquals(DataTypes.CHAR(10), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.CHAR(10));
        assertEquals("CHAR(30)", dorisType);
        dorisType = DorisTypeMapper.toDorisType(DataTypes.CHAR(100));
        assertEquals("VARCHAR(300)", dorisType);
    }

    @Test
    public void testVarcharType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "VARCHAR", 50, 0);
        assertEquals(DataTypes.VARCHAR(50), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.VARCHAR(50));
        assertEquals("VARCHAR(150)", dorisType);
    }

    @Test
    public void testStringType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "STRING", 0, 0);
        assertEquals(DataTypes.STRING(), result);
    }

    @Test
    public void testDateType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "DATE", 0, 0);
        assertEquals(DataTypes.DATE(), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.DATE());
        assertEquals("DATEV2", dorisType);
    }

    @Test
    public void testDatetimeType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "DATETIME", 0, 3);
        assertEquals(DataTypes.TIMESTAMP(3), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.TIMESTAMP(3));
        assertEquals("DATETIMEV2(3)", dorisType);
    }

    @Test
    public void testDatetimeTypeWithTimezoneType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "DATETIME", 0, 3);
        assertEquals(DataTypes.TIMESTAMP(3), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.TIMESTAMP_WITH_TIME_ZONE(3));
        assertEquals("DATETIMEV2(3)", dorisType);
    }

    @Test
    public void testDatetimeTypeWithLocalTimezoneType() {
        DataType result = DorisTypeMapper.toFlinkType("col", "DATETIME", 0, 3);
        assertEquals(DataTypes.TIMESTAMP(3), result);
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        assertEquals("DATETIMEV2(3)", dorisType);
    }

    @Test
    public void testTimeTypeWithLocalTimezoneType() {
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.TIME());
        assertEquals("STRING", dorisType);
    }

    @Test
    public void testTimeType() {
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.TIME(3));
        assertEquals("STRING", dorisType);
    }

    @Test
    public void testMultisetType() {
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.MULTISET(DataTypes.INT()));
        assertEquals("STRING", dorisType);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedType() {
        DorisTypeMapper.toFlinkType("col", "UNSUPPORTED", 0, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedDorisType() {
        DorisTypeMapper.toDorisType(DataTypes.NULL());
    }

    @Test
    public void testVarBinaryType() {
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.BYTES());
        assertEquals("STRING", dorisType);
    }

    @Test
    public void testArrayType() {
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.ARRAY(DataTypes.INT()));
        assertEquals("STRING", dorisType);
    }

    @Test
    public void testMapType() {
        String dorisType =
                DorisTypeMapper.toDorisType(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        assertEquals("STRING", dorisType);
    }

    @Test
    public void testRowType() {
        String dorisType =
                DorisTypeMapper.toDorisType(
                        DataTypes.ROW(DataTypes.FIELD("field", DataTypes.INT())));
        assertEquals("STRING", dorisType);
    }

    @Test
    public void testBinaryType() {
        String dorisType = DorisTypeMapper.toDorisType(DataTypes.BINARY(1));
        assertEquals("STRING", dorisType);
    }
}
