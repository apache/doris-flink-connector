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
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.ArrayUtils;
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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;

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

    @Test
    public void testDate() throws DorisException, IOException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k2", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(
                new Field("k3", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(
                                childrenBuilder.build(), null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        FieldVector vector = root.getVector("k1");
        VarCharVector dateVector = (VarCharVector) vector;
        dateVector.setInitialCapacity(1);
        dateVector.allocateNew();
        dateVector.setIndexDefined(0);
        dateVector.setValueLengthSafe(0, 10);
        dateVector.setSafe(0, "2023-08-09".getBytes());
        vector.setValueCount(1);

        vector = root.getVector("k2");
        VarCharVector dateV2Vector = (VarCharVector) vector;
        dateV2Vector.setInitialCapacity(1);
        dateV2Vector.allocateNew();
        dateV2Vector.setIndexDefined(0);
        dateV2Vector.setValueLengthSafe(0, 10);
        dateV2Vector.setSafe(0, "2023-08-10".getBytes());
        vector.setValueCount(1);

        vector = root.getVector("k3");
        DateDayVector dateNewVector = (DateDayVector) vector;
        dateNewVector.setInitialCapacity(1);
        dateNewVector.allocateNew();
        dateNewVector.setIndexDefined(0);
        dateNewVector.setSafe(0, 19802);
        vector.setValueCount(1);

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
                "{\"properties\":["
                        + "{\"type\":\"DATE\",\"name\":\"k1\",\"comment\":\"\"}, "
                        + "{\"type\":\"DATEV2\",\"name\":\"k2\",\"comment\":\"\"}, "
                        + "{\"type\":\"DATEV2\",\"name\":\"k3\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertEquals(LocalDate.of(2023, 8, 9), actualRow0.get(0));
        Assert.assertEquals(LocalDate.of(2023, 8, 10), actualRow0.get(1));
        Assert.assertEquals(LocalDate.of(2024, 3, 20), actualRow0.get(2));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testDateTime() throws IOException, DorisException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(
                new Field(
                        "k2",
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                        null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(
                                childrenBuilder.build(), null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k1");
        VarCharVector datetimeVector = (VarCharVector) vector;
        datetimeVector.setInitialCapacity(3);
        datetimeVector.allocateNew();
        datetimeVector.setIndexDefined(0);
        datetimeVector.setValueLengthSafe(0, 20);
        datetimeVector.setSafe(0, "2024-03-20 00:00:00".getBytes());
        datetimeVector.setIndexDefined(1);
        datetimeVector.setValueLengthSafe(1, 20);
        datetimeVector.setSafe(1, "2024-03-20 00:00:01".getBytes());
        datetimeVector.setIndexDefined(2);
        datetimeVector.setValueLengthSafe(2, 20);
        datetimeVector.setSafe(2, "2024-03-20 00:00:02".getBytes());
        vector.setValueCount(3);

        LocalDateTime localDateTime = LocalDateTime.of(2024, 3, 20, 0, 0, 0, 123456000);
        long second = localDateTime.atZone(ZoneId.systemDefault()).toEpochSecond();
        int nano = localDateTime.getNano();

        vector = root.getVector("k2");
        TimeStampMicroVector datetimeV2Vector = (TimeStampMicroVector) vector;
        datetimeV2Vector.setInitialCapacity(3);
        datetimeV2Vector.allocateNew();
        datetimeV2Vector.setIndexDefined(0);
        datetimeV2Vector.setSafe(0, second);
        datetimeV2Vector.setIndexDefined(1);
        datetimeV2Vector.setSafe(1, second * 1000 + nano / 1000000);
        datetimeV2Vector.setIndexDefined(2);
        datetimeV2Vector.setSafe(2, second * 1000000 + nano / 1000);
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
                "{\"properties\":["
                        + "{\"type\":\"DATETIME\",\"name\":\"k1\",\"comment\":\"\"}, "
                        + "{\"type\":\"DATETIMEV2\",\"name\":\"k2\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertEquals(LocalDateTime.of(2024, 3, 20, 0, 0, 0), actualRow0.get(0));
        Assert.assertEquals(LocalDateTime.of(2024, 3, 20, 0, 0, 0), actualRow0.get(1));

        List<Object> actualRow1 = rowBatch.next();
        Assert.assertEquals(LocalDateTime.of(2024, 3, 20, 0, 0, 1), actualRow1.get(0));
        Assert.assertEquals(LocalDateTime.of(2024, 3, 20, 0, 0, 0, 123000000), actualRow1.get(1));

        List<Object> actualRow2 = rowBatch.next();
        Assert.assertEquals(LocalDateTime.of(2024, 3, 20, 0, 0, 2), actualRow2.get(0));
        Assert.assertEquals(LocalDateTime.of(2024, 3, 20, 0, 0, 0, 123456000), actualRow2.get(1));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testLargeInt() throws DorisException, IOException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(
                new Field("k2", FieldType.nullable(new ArrowType.FixedSizeBinary(16)), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(
                                childrenBuilder.build(), null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        FieldVector vector = root.getVector("k1");
        VarCharVector lageIntVector = (VarCharVector) vector;
        lageIntVector.setInitialCapacity(1);
        lageIntVector.allocateNew();
        lageIntVector.setIndexDefined(0);
        lageIntVector.setValueLengthSafe(0, 19);
        lageIntVector.setSafe(0, "9223372036854775808".getBytes());
        vector.setValueCount(1);

        vector = root.getVector("k2");
        FixedSizeBinaryVector lageIntVector1 = (FixedSizeBinaryVector) vector;
        lageIntVector1.setInitialCapacity(1);
        lageIntVector1.allocateNew();
        lageIntVector1.setIndexDefined(0);
        byte[] bytes = new BigInteger("9223372036854775809").toByteArray();
        byte[] fixedBytes = new byte[16];
        System.arraycopy(bytes, 0, fixedBytes, 16 - bytes.length, bytes.length);
        ArrayUtils.reverse(fixedBytes);
        lageIntVector1.setSafe(0, fixedBytes);
        vector.setValueCount(1);

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
                "{\"properties\":["
                        + "{\"type\":\"LARGEINT\",\"name\":\"k1\",\"comment\":\"\"}, "
                        + "{\"type\":\"LARGEINT\",\"name\":\"k2\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();

        Assert.assertEquals(new BigInteger("9223372036854775808"), actualRow0.get(0));
        Assert.assertEquals(new BigInteger("9223372036854775809"), actualRow0.get(1));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testVariant() throws DorisException, IOException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Utf8()), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(
                                childrenBuilder.build(), null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k1");
        VarCharVector datetimeVector = (VarCharVector) vector;
        datetimeVector.setInitialCapacity(3);
        datetimeVector.allocateNew();
        datetimeVector.setIndexDefined(0);
        datetimeVector.setValueLengthSafe(0, 20);
        datetimeVector.setSafe(0, "{\"id\":\"a\"}".getBytes());
        datetimeVector.setIndexDefined(1);
        datetimeVector.setValueLengthSafe(1, 20);
        datetimeVector.setSafe(1, "1000".getBytes());
        datetimeVector.setIndexDefined(2);
        datetimeVector.setValueLengthSafe(2, 20);
        datetimeVector.setSafe(2, "123.456".getBytes());
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
                "{\"properties\":[{\"type\":\"VARIANT\",\"name\":\"k\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();
        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertEquals("{\"id\":\"a\"}", actualRow0.get(0));

        List<Object> actualRow1 = rowBatch.next();
        Assert.assertEquals("1000", actualRow1.get(0));

        List<Object> actualRow2 = rowBatch.next();
        Assert.assertEquals("123.456", actualRow2.get(0));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testIPV6() throws DorisException, IOException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Utf8()), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(
                                childrenBuilder.build(), null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(13);

        FieldVector vector = root.getVector("k1");
        VarCharVector ipv6Vector = (VarCharVector) vector;
        ipv6Vector.setInitialCapacity(13);
        ipv6Vector.allocateNew();
        ipv6Vector.setIndexDefined(0);
        ipv6Vector.setValueLengthSafe(0, 1);
        ipv6Vector.setSafe(0, "0".getBytes());

        ipv6Vector.setIndexDefined(1);
        ipv6Vector.setValueLengthSafe(0, 1);
        ipv6Vector.setSafe(1, "1".getBytes());

        ipv6Vector.setIndexDefined(2);
        ipv6Vector.setSafe(2, "65535".getBytes());

        ipv6Vector.setIndexDefined(3);
        ipv6Vector.setSafe(3, "65536".getBytes());

        ipv6Vector.setIndexDefined(4);
        ipv6Vector.setSafe(4, "4294967295".getBytes());

        ipv6Vector.setIndexDefined(5);
        ipv6Vector.setSafe(5, "4294967296".getBytes());

        ipv6Vector.setIndexDefined(6);
        ipv6Vector.setSafe(6, "8589934591".getBytes());

        ipv6Vector.setIndexDefined(7);
        ipv6Vector.setSafe(7, "281470681743359".getBytes());

        ipv6Vector.setIndexDefined(8);
        ipv6Vector.setSafe(8, "281470681743360".getBytes());

        ipv6Vector.setIndexDefined(9);
        ipv6Vector.setSafe(9, "281474976710655".getBytes());

        ipv6Vector.setIndexDefined(10);
        ipv6Vector.setSafe(10, "281474976710656".getBytes());

        ipv6Vector.setIndexDefined(11);
        ipv6Vector.setSafe(11, "340277174624079928635746639885392347137".getBytes());

        ipv6Vector.setIndexDefined(12);
        ipv6Vector.setSafe(12, "340282366920938463463374607431768211455".getBytes());

        vector.setValueCount(13);
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
                "{\"properties\":["
                        + "{\"type\":\"IPV6\",\"name\":\"k1\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();
        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        assertEquals("::", actualRow0.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow1 = rowBatch.next();
        assertEquals("::1", actualRow1.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        assertEquals("::ffff", actualRow2.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow3 = rowBatch.next();
        assertEquals("::0.1.0.0", actualRow3.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow4 = rowBatch.next();
        assertEquals("::255.255.255.255", actualRow4.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow5 = rowBatch.next();
        assertEquals("::1:0:0", actualRow5.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow6 = rowBatch.next();
        assertEquals("::1:ffff:ffff", actualRow6.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow7 = rowBatch.next();
        assertEquals("::fffe:ffff:ffff", actualRow7.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow8 = rowBatch.next();
        assertEquals("::ffff:0.0.0.0", actualRow8.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow9 = rowBatch.next();
        assertEquals("::ffff:255.255.255.255", actualRow9.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow10 = rowBatch.next();
        assertEquals("::1:0:0:0", actualRow10.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow11 = rowBatch.next();
        assertEquals("ffff::1:ffff:ffff:1", actualRow11.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow12 = rowBatch.next();
        assertEquals("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", actualRow12.get(0));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testIPV4() throws DorisException, IOException {

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(
                new Field("k1", FieldType.nullable(new ArrowType.Int(32, false)), null),
                new Field("k2", FieldType.nullable(new ArrowType.Int(32, true)), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(
                                childrenBuilder.build(), null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(5);

        FieldVector vector = root.getVector("k1");
        UInt4Vector uInt4Vector = (UInt4Vector) vector;
        uInt4Vector.setInitialCapacity(5);
        uInt4Vector.allocateNew(4);
        uInt4Vector.setIndexDefined(0);
        uInt4Vector.setSafe(0, 0);
        uInt4Vector.setIndexDefined(1);
        uInt4Vector.setSafe(1, 255);
        uInt4Vector.setIndexDefined(2);
        uInt4Vector.setSafe(2, 65535);
        uInt4Vector.setIndexDefined(3);
        uInt4Vector.setSafe(3, 16777215);
        uInt4Vector.setIndexDefined(4);
        uInt4Vector.setWithPossibleTruncate(4, 4294967295L);

        FieldVector vector1 = root.getVector("k2");
        IntVector intVector = (IntVector) vector1;
        intVector.setInitialCapacity(5);
        intVector.allocateNew(4);
        intVector.setIndexDefined(0);
        intVector.setSafe(0, 0);
        intVector.setIndexDefined(1);
        intVector.setSafe(1, 255);
        intVector.setIndexDefined(2);
        intVector.setSafe(2, 65535);
        intVector.setIndexDefined(3);
        intVector.setSafe(3, 16777215);
        intVector.setIndexDefined(4);
        intVector.setWithPossibleTruncate(4, 4294967295L);
        vector.setValueCount(5);
        vector1.setValueCount(5);
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
                "{\"properties\":["
                        + "{\"type\":\"IPV4\",\"name\":\"k1\",\"comment\":\"\"},"
                        + "{\"type\":\"IPV4\",\"name\":\"k2\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();
        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        assertEquals("0.0.0.0", actualRow0.get(0));
        assertEquals("0.0.0.0", actualRow0.get(1));
        List<Object> actualRow1 = rowBatch.next();
        assertEquals("0.0.0.255", actualRow1.get(0));
        assertEquals("0.0.0.255", actualRow1.get(1));
        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        assertEquals("0.0.255.255", actualRow2.get(0));
        assertEquals("0.0.255.255", actualRow2.get(1));
        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow3 = rowBatch.next();
        assertEquals("0.255.255.255", actualRow3.get(0));
        assertEquals("0.255.255.255", actualRow3.get(1));
        List<Object> actualRow4 = rowBatch.next();
        assertEquals("255.255.255.255", actualRow4.get(0));
        assertEquals("255.255.255.255", actualRow4.get(1));
        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testCompleteMilliseconds() {
        String dt = RowBatch.completeMilliseconds("2021-01-01 10:01:02.123456");
        Assert.assertEquals(dt, "2021-01-01 10:01:02.123456");
        dt = RowBatch.completeMilliseconds("2021-01-01 10:01:02");
        Assert.assertEquals(dt, "2021-01-01 10:01:02.000000");
        dt = RowBatch.completeMilliseconds("2021-01-01 10:01:02.1");
        Assert.assertEquals(dt, "2021-01-01 10:01:02.100000");
        dt = RowBatch.completeMilliseconds("2021-01-01 10:01:02.12");
        Assert.assertEquals(dt, "2021-01-01 10:01:02.120000");
        dt = RowBatch.completeMilliseconds("2021-01-01 10:01:02.123");
        Assert.assertEquals(dt, "2021-01-01 10:01:02.123000");
        dt = RowBatch.completeMilliseconds("2021-01-01 10:01:02.1234");
        Assert.assertEquals(dt, "2021-01-01 10:01:02.123400");
        dt = RowBatch.completeMilliseconds("2021-01-01 10:01:02.12345");
        Assert.assertEquals(dt, "2021-01-01 10:01:02.123450");
        dt = RowBatch.completeMilliseconds("2021-01-01 10:01:02.123456");
        Assert.assertEquals(dt, "2021-01-01 10:01:02.123456");

        dt = RowBatch.completeMilliseconds("2021-01-01");
        Assert.assertEquals(dt, "2021-01-01");
    }

    @Test
    public void testDoConvert() throws Exception {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(
                new Field("k1", FieldType.nullable(new ArrowType.Int(32, false)), null),
                new Field("k2", FieldType.nullable(new ArrowType.Int(32, true)), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(
                                childrenBuilder.build(), null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(5);

        FieldVector vector = root.getVector("k1");
        UInt4Vector uInt4Vector = (UInt4Vector) vector;
        uInt4Vector.setInitialCapacity(5);
        uInt4Vector.allocateNew(4);
        uInt4Vector.setIndexDefined(0);
        uInt4Vector.setSafe(0, 0);
        uInt4Vector.setIndexDefined(1);
        uInt4Vector.setSafe(1, 255);
        uInt4Vector.setIndexDefined(2);
        uInt4Vector.setSafe(2, 65535);
        uInt4Vector.setIndexDefined(3);
        uInt4Vector.setSafe(3, 16777215);
        uInt4Vector.setIndexDefined(4);
        uInt4Vector.setWithPossibleTruncate(4, 4294967295L);

        FieldVector vector1 = root.getVector("k2");
        IntVector intVector = (IntVector) vector1;
        intVector.setInitialCapacity(5);
        intVector.allocateNew(4);
        intVector.setIndexDefined(0);
        intVector.setSafe(0, 0);
        intVector.setIndexDefined(1);
        intVector.setSafe(1, 255);
        intVector.setIndexDefined(2);
        intVector.setSafe(2, 65535);
        intVector.setIndexDefined(3);
        intVector.setSafe(3, 16777215);
        intVector.setIndexDefined(4);
        intVector.setWithPossibleTruncate(4, 4294967295L);
        vector.setValueCount(5);
        vector1.setValueCount(5);
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
                "{\"properties\":["
                        + "{\"type\":\"IPV4\",\"name\":\"k1\",\"comment\":\"\"},"
                        + "{\"type\":\"IPV4\",\"name\":\"k2\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();
        boolean flag = rowBatch.doConvert(1, 1, Types.MinorType.TINYINT, "BOOLEAN", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.SMALLINT, "TINYINT", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "SMALLINT", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.VARCHAR, "INT", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.VARCHAR, "IPV4", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "BIGINT", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "FLOAT", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "DOUBLE", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "BINARY", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "DECIMAL", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "DATE", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "DATETIME", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.TIMESTAMPSEC, "DATETIME", null);
        Assert.assertFalse(flag);

        IntVector intVector1 = new IntVector("test", new RootAllocator(Integer.MAX_VALUE));
        intVector1.setNull(0);
        flag = rowBatch.doConvert(1, 1, Types.MinorType.TIMESTAMPSEC, "DATETIME", intVector1);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "DATETIMEV2", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.TIMESTAMPSEC, "DATETIMEV2", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.TIMESTAMPSEC, "DATETIMEV2", intVector1);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "LARGEINT", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "JSONB", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "IPV6", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "ARRAY", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "MAP", null);
        Assert.assertFalse(flag);

        flag = rowBatch.doConvert(1, 1, Types.MinorType.INT, "STRUCT", null);
        Assert.assertFalse(flag);

        thrown.expect(DorisException.class);
        thrown.expectMessage(startsWith("Unsupported type"));
        rowBatch.doConvert(1, 1, Types.MinorType.TINYINT, "UnsupportType", null);
    }

    @Test
    public void testDoConvertNull() throws Exception {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(
                new Field("k1", FieldType.nullable(new ArrowType.Int(32, false)), null),
                new Field("k2", FieldType.nullable(new ArrowType.Int(32, true)), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(
                                childrenBuilder.build(), null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(5);

        FieldVector vector = root.getVector("k1");
        UInt4Vector uInt4Vector = (UInt4Vector) vector;
        uInt4Vector.setInitialCapacity(5);
        uInt4Vector.allocateNew(4);
        uInt4Vector.setIndexDefined(0);
        uInt4Vector.setSafe(0, 0);
        uInt4Vector.setIndexDefined(1);
        uInt4Vector.setSafe(1, 255);
        uInt4Vector.setIndexDefined(2);
        uInt4Vector.setSafe(2, 65535);
        uInt4Vector.setIndexDefined(3);
        uInt4Vector.setSafe(3, 16777215);
        uInt4Vector.setIndexDefined(4);
        uInt4Vector.setWithPossibleTruncate(4, 4294967295L);

        FieldVector vector1 = root.getVector("k2");
        IntVector intVector = (IntVector) vector1;
        intVector.setInitialCapacity(5);
        intVector.allocateNew(4);
        intVector.setIndexDefined(0);
        intVector.setSafe(0, 0);
        intVector.setIndexDefined(1);
        intVector.setSafe(1, 255);
        intVector.setIndexDefined(2);
        intVector.setSafe(2, 65535);
        intVector.setIndexDefined(3);
        intVector.setSafe(3, 16777215);
        intVector.setIndexDefined(4);
        intVector.setWithPossibleTruncate(4, 4294967295L);
        vector.setValueCount(5);
        vector1.setValueCount(5);
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
                "{\"properties\":["
                        + "{\"type\":\"IPV4\",\"name\":\"k1\",\"comment\":\"\"},"
                        + "{\"type\":\"IPV4\",\"name\":\"k2\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema); // .readArrow();
        for (int i = 0; i < 1; ++i) {
            rowBatch.getRowBatch().add(new RowBatch.Row(1));
        }
        RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        BitVector vectorbit = new BitVector("test", allocator);
        vectorbit.setNull(0);
        boolean flag = rowBatch.doConvert(0, 0, Types.MinorType.BIT, "BOOLEAN", vectorbit);
        Assert.assertTrue(flag);

        SmallIntVector vectorSmallint = new SmallIntVector("test", allocator);
        vectorSmallint.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.SMALLINT, "SMALLINT", vectorSmallint);
        Assert.assertTrue(flag);

        IntVector vectorint = new IntVector("test", allocator);
        vectorint.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.INT, "INT", vectorint);
        Assert.assertTrue(flag);

        // IPV4 Vector
        IntVector ipv4Vector = new IntVector("testIpv4", allocator);
        ipv4Vector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.INT, "IPV4", ipv4Vector);
        Assert.assertTrue(flag);

        // BIGINT Vector
        BigIntVector bigintVector = new BigIntVector("testBigint", allocator);
        bigintVector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.BIGINT, "BIGINT", bigintVector);
        Assert.assertTrue(flag);

        // FLOAT4 Vector
        Float4Vector float4Vector = new Float4Vector("testFloat", allocator);
        float4Vector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.FLOAT4, "FLOAT", float4Vector);
        Assert.assertTrue(flag);

        // FLOAT8 (DOUBLE) Vector
        Float8Vector float8Vector = new Float8Vector("testDouble", allocator);
        float8Vector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.FLOAT8, "DOUBLE", float8Vector);
        Assert.assertTrue(flag);

        // VARBINARY Vector
        VarBinaryVector varbinaryVector = new VarBinaryVector("testBinary", allocator);
        varbinaryVector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.VARBINARY, "BINARY", varbinaryVector);
        Assert.assertTrue(flag);

        // DECIMAL Vector
        DecimalVector decimalVector = new DecimalVector("testDecimal", allocator, 38, 18);
        decimalVector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.DECIMAL, "DECIMAL", decimalVector);
        Assert.assertTrue(flag);

        // DATEDAY Vector
        DateDayVector dateDayVector = new DateDayVector("testDate", allocator);
        dateDayVector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.DATEDAY, "DATE", dateDayVector);
        Assert.assertTrue(flag);

        // TIMESTAMPMICRO Vector
        TimeStampMicroVector timeStampMicroVector =
                new TimeStampMicroVector("testDatetime", allocator);
        timeStampMicroVector.setNull(0);
        flag =
                rowBatch.doConvert(
                        0, 0, Types.MinorType.TIMESTAMPMICRO, "DATETIME", timeStampMicroVector);
        Assert.assertTrue(flag);

        // TIMESTAMPMICRO for DATETIMEV2
        TimeStampMicroVector timeStampMicroVectorV2 =
                new TimeStampMicroVector("testDatetimeV2", allocator);
        timeStampMicroVectorV2.setNull(0);
        flag =
                rowBatch.doConvert(
                        0, 0, Types.MinorType.TIMESTAMPMICRO, "DATETIMEV2", timeStampMicroVectorV2);
        Assert.assertTrue(flag);

        // FIXEDSIZEBINARY for LARGEINT
        FixedSizeBinaryVector largeIntVector =
                new FixedSizeBinaryVector("testLargeInt", allocator, 16);
        largeIntVector.setNull(0);
        flag =
                rowBatch.doConvert(
                        0, 0, Types.MinorType.FIXEDSIZEBINARY, "LARGEINT", largeIntVector);
        Assert.assertTrue(flag);

        // VARCHAR for JSONB
        VarCharVector jsonbVector = new VarCharVector("testJsonb", allocator);
        jsonbVector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.VARCHAR, "JSONB", jsonbVector);
        Assert.assertTrue(flag);

        // VARCHAR for IPV6
        VarCharVector ipv6Vector = new VarCharVector("testIpv6", allocator);
        ipv6Vector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.VARCHAR, "IPV6", ipv6Vector);
        Assert.assertTrue(flag);

        // LIST Vector
        ListVector listVector = ListVector.empty("testArray", allocator);
        listVector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.LIST, "ARRAY", listVector);
        Assert.assertTrue(flag);

        // MAP Vector
        MapVector mapVector = MapVector.empty("testMap", allocator, false);
        mapVector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.MAP, "MAP", mapVector);
        Assert.assertTrue(flag);

        // STRUCT Vector
        StructVector structVector = StructVector.empty("testStruct", allocator);
        structVector.setNull(0);
        flag = rowBatch.doConvert(0, 0, Types.MinorType.STRUCT, "STRUCT", structVector);
        Assert.assertTrue(flag);
    }

    @Test
    public void getDateTimeNull() throws IOException, DorisException {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(
                new Field("k1", FieldType.nullable(new ArrowType.Int(32, false)), null),
                new Field("k2", FieldType.nullable(new ArrowType.Int(32, true)), null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(
                                childrenBuilder.build(), null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(5);

        FieldVector vector = root.getVector("k1");
        UInt4Vector uInt4Vector = (UInt4Vector) vector;
        uInt4Vector.setInitialCapacity(5);
        uInt4Vector.allocateNew(4);
        uInt4Vector.setIndexDefined(0);
        uInt4Vector.setSafe(0, 0);
        uInt4Vector.setIndexDefined(1);
        uInt4Vector.setSafe(1, 255);
        uInt4Vector.setIndexDefined(2);
        uInt4Vector.setSafe(2, 65535);
        uInt4Vector.setIndexDefined(3);
        uInt4Vector.setSafe(3, 16777215);
        uInt4Vector.setIndexDefined(4);
        uInt4Vector.setWithPossibleTruncate(4, 4294967295L);

        FieldVector vector1 = root.getVector("k2");
        IntVector intVector = (IntVector) vector1;
        intVector.setInitialCapacity(5);
        intVector.allocateNew(4);
        intVector.setIndexDefined(0);
        intVector.setSafe(0, 0);
        intVector.setIndexDefined(1);
        intVector.setSafe(1, 255);
        intVector.setIndexDefined(2);
        intVector.setSafe(2, 65535);
        intVector.setIndexDefined(3);
        intVector.setSafe(3, 16777215);
        intVector.setIndexDefined(4);
        intVector.setWithPossibleTruncate(4, 4294967295L);
        vector.setValueCount(5);
        vector1.setValueCount(5);
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
                "{\"properties\":["
                        + "{\"type\":\"IPV4\",\"name\":\"k1\",\"comment\":\"\"},"
                        + "{\"type\":\"IPV4\",\"name\":\"k2\",\"comment\":\"\"}"
                        + "], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();
        TimeStampMicroVector timeStampMicroVectorV2 =
                new TimeStampMicroVector("testDatetimeV2", new RootAllocator(Integer.MAX_VALUE));
        timeStampMicroVectorV2.setNull(0);
        LocalDateTime dateTime = rowBatch.getDateTime(0, timeStampMicroVectorV2);
        Assert.assertNull(dateTime);

        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset"));
        rowBatch.addValueToRow(10, null);
    }

    @Test
    public void longToLocalDateTimeTest() {
        ZoneId defaultZoneId = ZoneId.systemDefault();
        LocalDateTime now = LocalDateTime.now(defaultZoneId).truncatedTo(ChronoUnit.MICROS);

        long secondTimestamp = now.toEpochSecond(defaultZoneId.getRules().getOffset(now));
        long milliTimestamp = now.atZone(defaultZoneId).toInstant().toEpochMilli();
        long microTimestamp =
                now.toInstant(defaultZoneId.getRules().getOffset(now)).getEpochSecond() * 1_000_000
                        + now.getNano() / 1_000;

        LocalDateTime dateTime1 = RowBatch.longToLocalDateTime(secondTimestamp);
        LocalDateTime dateTime2 = RowBatch.longToLocalDateTime(milliTimestamp);
        LocalDateTime dateTime3 = RowBatch.longToLocalDateTime(microTimestamp);

        long result1 = dateTime1.atZone(defaultZoneId).toInstant().getEpochSecond();
        long result2 = dateTime2.atZone(defaultZoneId).toInstant().toEpochMilli();
        long result3 =
                dateTime3.toInstant(defaultZoneId.getRules().getOffset(dateTime3)).getEpochSecond()
                                * 1_000_000
                        + dateTime3.getNano() / 1_000;

        long[] expectArray = {secondTimestamp, milliTimestamp, microTimestamp};
        long[] resultArray = {result1, result2, result3};

        Assert.assertArrayEquals(expectArray, resultArray);
    }

    @Test
    public void timestampVector() throws IOException, DorisException {
        List<Field> childrenBuilder = new ArrayList<>();
        childrenBuilder.add(
                new Field(
                        "k0",
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                        null));
        childrenBuilder.add(
                new Field(
                        "k1",
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        null));
        childrenBuilder.add(
                new Field(
                        "k2",
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)),
                        null));

        VectorSchemaRoot root =
                VectorSchemaRoot.create(
                        new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder, null),
                        new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter =
                new ArrowStreamWriter(
                        root, new DictionaryProvider.MapDictionaryProvider(), outputStream);

        arrowStreamWriter.start();
        root.setRowCount(1);

        FieldVector vector = root.getVector("k0");
        TimeStampMicroVector mircoVec = (TimeStampMicroVector) vector;
        mircoVec.allocateNew(1);
        mircoVec.setIndexDefined(0);
        mircoVec.setSafe(0, 1721892143586123L);
        vector.setValueCount(1);

        vector = root.getVector("k1");
        TimeStampMilliVector milliVector = (TimeStampMilliVector) vector;
        milliVector.allocateNew(1);
        milliVector.setIndexDefined(0);
        milliVector.setSafe(0, 1721892143586L);
        vector.setValueCount(1);

        vector = root.getVector("k2");
        TimeStampSecVector secVector = (TimeStampSecVector) vector;
        secVector.allocateNew(1);
        secVector.setIndexDefined(0);
        secVector.setSafe(0, 1721892143L);
        vector.setValueCount(1);

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
                "{\"properties\":[{\"type\":\"DATETIME\",\"name\":\"k0\",\"comment\":\"\"}, {\"type\":\"DATETIME\",\"name\":\"k1\",\"comment\":\"\"}, {\"type\":\"DATETIME\",\"name\":\"k2\",\"comment\":\"\"}],"
                        + "\"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema).readArrow();
        List<Object> next = rowBatch.next();
        Assert.assertEquals(next.size(), 3);
        Assert.assertEquals(
                next.get(0),
                LocalDateTime.of(2024, 7, 25, 15, 22, 23, 586123000)
                        .atZone(ZoneId.of("UTC+8"))
                        .withZoneSameInstant(ZoneId.systemDefault())
                        .toLocalDateTime());
        Assert.assertEquals(
                next.get(1),
                LocalDateTime.of(2024, 7, 25, 15, 22, 23, 586000000)
                        .atZone(ZoneId.of("UTC+8"))
                        .withZoneSameInstant(ZoneId.systemDefault())
                        .toLocalDateTime());
        Assert.assertEquals(
                next.get(2),
                LocalDateTime.of(2024, 7, 25, 15, 22, 23, 0)
                        .atZone(ZoneId.of("UTC+8"))
                        .withZoneSameInstant(ZoneId.systemDefault())
                        .toLocalDateTime());
    }
}
