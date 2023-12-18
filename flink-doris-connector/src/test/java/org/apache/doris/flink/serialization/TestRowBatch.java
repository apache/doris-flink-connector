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

package org.apache.doris.flink.serialization;

import org.apache.flink.table.data.DecimalData;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TStatus;
import org.apache.doris.sdk.thrift.TStatusCode;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import static org.hamcrest.core.StringStartsWith.startsWith;

public class TestRowBatch {
    private static Logger logger = LoggerFactory.getLogger(TestRowBatch.class);

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testRowBatch() throws Exception {
        // schema
        List<Field> childrenBuilder = new ArrayList<>();
        childrenBuilder.add(new Field("k0", FieldType.nullable(new ArrowType.Bool()), null));
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Int(8, true)), null));
        childrenBuilder.add(new Field("k2", FieldType.nullable(new ArrowType.Int(16, true)), null));
        childrenBuilder.add(new Field("k3", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(new Field("k4", FieldType.nullable(new ArrowType.Int(64, true)), null));
        childrenBuilder.add(
                new Field(
                        "k9",
                        FieldType.nullable(
                                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                        null));
        childrenBuilder.add(
                new Field(
                        "k8",
                        FieldType.nullable(
                                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                        null));
        childrenBuilder.add(new Field("k10", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k11", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k5", FieldType.nullable(new ArrowType.Decimal(9, 2)), null));
        childrenBuilder.add(new Field("k6", FieldType.nullable(new ArrowType.Utf8()), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder, null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k0");
        BitVector bitVector = (BitVector) vector;
        bitVector.setInitialCapacity(3);
        bitVector.allocateNew(3);
        bitVector.setSafe(0, 1);
        bitVector.setSafe(1, 0);
        bitVector.setSafe(2, 1);
        vector.setValueCount(3);

        vector = root.getVector("k1");
        TinyIntVector tinyIntVector = (TinyIntVector) vector;
        tinyIntVector.setInitialCapacity(3);
        tinyIntVector.allocateNew(3);
        tinyIntVector.setSafe(0, 1);
        tinyIntVector.setSafe(1, 2);
        tinyIntVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k2");
        SmallIntVector smallIntVector = (SmallIntVector) vector;
        smallIntVector.setInitialCapacity(3);
        smallIntVector.allocateNew(3);
        smallIntVector.setSafe(0, 1);
        smallIntVector.setSafe(1, 2);
        smallIntVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k3");
        IntVector intVector = (IntVector) vector;
        intVector.setInitialCapacity(3);
        intVector.allocateNew(3);
        intVector.setSafe(0, 1);
        intVector.setNull(1);
        intVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k4");
        BigIntVector bigIntVector = (BigIntVector) vector;
        bigIntVector.setInitialCapacity(3);
        bigIntVector.allocateNew(3);
        bigIntVector.setSafe(0, 1);
        bigIntVector.setSafe(1, 2);
        bigIntVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k5");
        DecimalVector decimalVector = (DecimalVector) vector;
        decimalVector.setInitialCapacity(3);
        decimalVector.allocateNew();
        decimalVector.setIndexDefined(0);
        decimalVector.setSafe(0, new BigDecimal("12.34"));
        decimalVector.setIndexDefined(1);
        decimalVector.setSafe(1, new BigDecimal("88.88"));
        decimalVector.setIndexDefined(2);
        decimalVector.setSafe(2, new BigDecimal("10.22"));
        vector.setValueCount(3);

        vector = root.getVector("k6");
        VarCharVector charVector = (VarCharVector) vector;
        charVector.setInitialCapacity(3);
        charVector.allocateNew();
        charVector.setIndexDefined(0);
        charVector.setValueLengthSafe(0, 5);
        charVector.setSafe(0, "char1".getBytes());
        charVector.setIndexDefined(1);
        charVector.setValueLengthSafe(1, 5);
        charVector.setSafe(1, "char2".getBytes());
        charVector.setIndexDefined(2);
        charVector.setValueLengthSafe(2, 5);
        charVector.setSafe(2, "char3".getBytes());
        vector.setValueCount(3);

        vector = root.getVector("k8");
        Float8Vector float8Vector = (Float8Vector) vector;
        float8Vector.setInitialCapacity(3);
        float8Vector.allocateNew(3);
        float8Vector.setSafe(0, 1.1);
        float8Vector.setSafe(1, 2.2);
        float8Vector.setSafe(2, 3.3);
        vector.setValueCount(3);

        vector = root.getVector("k9");
        Float4Vector float4Vector = (Float4Vector) vector;
        float4Vector.setInitialCapacity(3);
        float4Vector.allocateNew(3);
        float4Vector.setSafe(0, 1.1f);
        float4Vector.setSafe(1, 2.2f);
        float4Vector.setSafe(2, 3.3f);
        vector.setValueCount(3);

        vector = root.getVector("k10");
        VarCharVector datecharVector = (VarCharVector) vector;
        datecharVector.setInitialCapacity(3);
        datecharVector.allocateNew();
        datecharVector.setIndexDefined(0);
        datecharVector.setValueLengthSafe(0, 5);
        datecharVector.setSafe(0, "2008-08-08".getBytes());
        datecharVector.setIndexDefined(1);
        datecharVector.setValueLengthSafe(1, 5);
        datecharVector.setSafe(1, "1900-08-08".getBytes());
        datecharVector.setIndexDefined(2);
        datecharVector.setValueLengthSafe(2, 5);
        datecharVector.setSafe(2, "2100-08-08".getBytes());
        vector.setValueCount(3);

        vector = root.getVector("k11");
        VarCharVector timecharVector = (VarCharVector) vector;
        timecharVector.setInitialCapacity(3);
        timecharVector.allocateNew();
        timecharVector.setIndexDefined(0);
        timecharVector.setValueLengthSafe(0, 5);
        timecharVector.setSafe(0, "2008-08-08 00:00:00".getBytes());
        timecharVector.setIndexDefined(1);
        timecharVector.setValueLengthSafe(1, 5);
        timecharVector.setSafe(1, "1900-08-08 00:00:00".getBytes());
        timecharVector.setIndexDefined(2);
        timecharVector.setValueLengthSafe(2, 5);
        timecharVector.setSafe(2, "2100-08-08 00:00:00".getBytes());
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr =
                "{\"properties\":[{\"type\":\"BOOLEAN\",\"name\":\"k0\",\"comment\":\"\"},"
                        + "{\"type\":\"TINYINT\",\"name\":\"k1\",\"comment\":\"\"},{\"type\":\"SMALLINT\",\"name\":\"k2\","
                        + "\"comment\":\"\"},{\"type\":\"INT\",\"name\":\"k3\",\"comment\":\"\"},{\"type\":\"BIGINT\","
                        + "\"name\":\"k4\",\"comment\":\"\"},{\"type\":\"FLOAT\",\"name\":\"k9\",\"comment\":\"\"},"
                        + "{\"type\":\"DOUBLE\",\"name\":\"k8\",\"comment\":\"\"},{\"type\":\"DATE\",\"name\":\"k10\","
                        + "\"comment\":\"\"},{\"type\":\"DATETIME\",\"name\":\"k11\",\"comment\":\"\"},"
                        + "{\"name\":\"k5\",\"scale\":\"0\",\"comment\":\"\","
                        + "\"type\":\"DECIMAL\",\"precision\":\"9\",\"aggregation_type\":\"\"},{\"type\":\"CHAR\",\"name\":\"k6\",\"comment\":\"\",\"aggregation_type\":\"REPLACE_IF_NOT_NULL\"}],"
                        + "\"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();

        List<Object> expectedRow1 =
                Arrays.asList(
                        Boolean.TRUE,
                        (byte) 1,
                        (short) 1,
                        1,
                        1L,
                        (float) 1.1,
                        (double) 1.1,
                        LocalDate.of(2008, 8, 8),
                        LocalDateTime.of(2008, 8, 8, 0, 0, 0),
                        DecimalData.fromBigDecimal(new BigDecimal(12.34), 4, 2),
                        "char1");

        List<Object> expectedRow2 =
                Arrays.asList(
                        Boolean.FALSE,
                        (byte) 2,
                        (short) 2,
                        null,
                        2L,
                        (float) 2.2,
                        (double) 2.2,
                        LocalDate.of(1900, 8, 8),
                        LocalDateTime.of(1900, 8, 8, 0, 0, 0),
                        DecimalData.fromBigDecimal(new BigDecimal(88.88), 4, 2),
                        "char2");

        List<Object> expectedRow3 =
                Arrays.asList(
                        Boolean.TRUE,
                        (byte) 3,
                        (short) 3,
                        3,
                        3L,
                        (float) 3.3,
                        (double) 3.3,
                        LocalDate.of(2100, 8, 8),
                        LocalDateTime.of(2100, 8, 8, 0, 0, 0),
                        DecimalData.fromBigDecimal(new BigDecimal(10.22), 4, 2),
                        "char3");

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow1 = rowBatch.next();
        actualRow1.set(9, DecimalData.fromBigDecimal((BigDecimal) actualRow1.get(9), 4, 2));
        Assert.assertEquals(expectedRow1, actualRow1);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        actualRow2.set(9, DecimalData.fromBigDecimal((BigDecimal) actualRow2.get(9), 4, 2));
        Assert.assertEquals(expectedRow2, actualRow2);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow3 = rowBatch.next();
        actualRow3.set(9, DecimalData.fromBigDecimal((BigDecimal) actualRow3.get(9), 4, 2));
        Assert.assertEquals(expectedRow3, actualRow3);

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testBinary() throws Exception {
        byte[] binaryRow0 = {'a', 'b', 'c'};
        byte[] binaryRow1 = {'d', 'e', 'f'};
        byte[] binaryRow2 = {'g', 'h', 'i'};

        List<Field> childrenBuilder = new ArrayList<>();
        childrenBuilder.add(new Field("k7", FieldType.nullable(new ArrowType.Binary()), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder, null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k7");
        VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
        varBinaryVector.setInitialCapacity(3);
        varBinaryVector.allocateNew();
        varBinaryVector.setIndexDefined(0);
        varBinaryVector.setValueLengthSafe(0, 3);
        varBinaryVector.setSafe(0, binaryRow0);
        varBinaryVector.setIndexDefined(1);
        varBinaryVector.setValueLengthSafe(1, 3);
        varBinaryVector.setSafe(1, binaryRow1);
        varBinaryVector.setIndexDefined(2);
        varBinaryVector.setValueLengthSafe(2, 3);
        varBinaryVector.setSafe(2, binaryRow2);
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr =
                "{\"properties\":[{\"type\":\"BINARY\",\"name\":\"k7\",\"comment\":\"\"}], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertArrayEquals(binaryRow0, (byte[]) actualRow0.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow1 = rowBatch.next();
        Assert.assertArrayEquals(binaryRow1, (byte[]) actualRow1.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        Assert.assertArrayEquals(binaryRow2, (byte[]) actualRow2.get(0));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testDecimalV2() throws Exception {
        List<Field> childrenBuilder = new ArrayList<>();
        childrenBuilder.add(
                new Field("k7", FieldType.nullable(new ArrowType.Decimal(27, 9)), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder, null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k7");
        DecimalVector decimalVector = (DecimalVector) vector;
        decimalVector.setInitialCapacity(3);
        decimalVector.allocateNew(3);
        decimalVector.setSafe(0, new BigDecimal("12.340000000"));
        decimalVector.setSafe(1, new BigDecimal("88.880000000"));
        decimalVector.setSafe(2, new BigDecimal("10.000000000"));
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr =
                "{\"properties\":[{\"type\":\"DECIMALV2\",\"scale\": 0,"
                        + "\"precision\": 9, \"name\":\"k7\",\"comment\":\"\"}], "
                        + "\"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertEquals(
                DecimalData.fromBigDecimal(new BigDecimal(12.340000000), 11, 9),
                DecimalData.fromBigDecimal((BigDecimal) actualRow0.get(0), 11, 9));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow1 = rowBatch.next();
        Assert.assertEquals(
                DecimalData.fromBigDecimal(new BigDecimal(88.880000000), 11, 9),
                DecimalData.fromBigDecimal((BigDecimal) actualRow1.get(0), 11, 9));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        Assert.assertEquals(
                DecimalData.fromBigDecimal(new BigDecimal(10.000000000), 11, 9),
                DecimalData.fromBigDecimal((BigDecimal) actualRow2.get(0), 11, 9));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testMap() throws IOException, DorisException {

        ImmutableList<Field> mapChildren =
                ImmutableList.of(
                        new Field(
                                "child",
                                new FieldType(false, new ArrowType.Struct(), null),
                                ImmutableList.of(
                                        new Field(
                                                "key",
                                                new FieldType(false, new ArrowType.Utf8(), null),
                                                null),
                                        new Field(
                                                "value",
                                                new FieldType(
                                                        false, new ArrowType.Int(32, true), null),
                                                null))));

        ImmutableList<Field> fields =
                ImmutableList.of(
                        new Field(
                                "col_map",
                                new FieldType(false, new ArrowType.Map(false), null),
                                mapChildren));

        RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(fields, null), allocator);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        MapVector mapVector = (MapVector) root.getVector("col_map");
        mapVector.allocateNew();
        UnionMapWriter mapWriter = mapVector.getWriter();
        for (int i = 0; i < 3; i++) {
            mapWriter.setPosition(i);
            mapWriter.startMap();
            mapWriter.startEntry();
            String key = "k" + (i + 1);
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            ArrowBuf buffer = allocator.buffer(bytes.length);
            buffer.setBytes(0, bytes);
            mapWriter.key().varChar().writeVarChar(0, bytes.length, buffer);
            buffer.close();
            mapWriter.value().integer().writeInt(i);
            mapWriter.endEntry();
            mapWriter.endMap();
        }
        mapWriter.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr =
                "{\"properties\":[{\"type\":\"MAP\",\"name\":\"col_map\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();
        Assert.assertTrue(rowBatch.hasNext());
        Assert.assertTrue(ImmutableMap.of("k1", 0).equals(rowBatch.next().get(0)));
        Assert.assertTrue(rowBatch.hasNext());
        Assert.assertTrue(ImmutableMap.of("k2", 1).equals(rowBatch.next().get(0)));
        Assert.assertTrue(rowBatch.hasNext());
        Assert.assertTrue(ImmutableMap.of("k3", 2).equals(rowBatch.next().get(0)));
        Assert.assertFalse(rowBatch.hasNext());
    }

    @Test
    public void testStruct() throws IOException, DorisException {

        ImmutableList<Field> fields =
                ImmutableList.of(
                        new Field(
                                "col_struct",
                                new FieldType(false, new ArrowType.Struct(), null),
                                ImmutableList.of(
                                        new Field(
                                                "a",
                                                new FieldType(false, new ArrowType.Utf8(), null),
                                                null),
                                        new Field(
                                                "b",
                                                new FieldType(
                                                        false, new ArrowType.Int(32, true), null),
                                                null))));

        RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(fields, null), allocator);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        StructVector structVector = (StructVector) root.getVector("col_struct");
        structVector.allocateNew();
        NullableStructWriter writer = structVector.getWriter();
        writer.setPosition(0);
        writer.start();
        byte[] bytes = "a1".getBytes(StandardCharsets.UTF_8);
        ArrowBuf buffer = allocator.buffer(bytes.length);
        buffer.setBytes(0, bytes);
        writer.varChar("a").writeVarChar(0, bytes.length, buffer);
        buffer.close();
        writer.integer("b").writeInt(1);
        writer.end();
        writer.setValueCount(1);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr =
                "{\"properties\":[{\"type\":\"STRUCT\",\"name\":\"col_struct\",\"comment\":\"\"}"
                        + "], \"status\":200}";
        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();
        Assert.assertTrue(rowBatch.hasNext());
        Assert.assertTrue(
                ImmutableMap.of("a", new Text("a1"), "b", 1).equals(rowBatch.next().get(0)));
    }
}
