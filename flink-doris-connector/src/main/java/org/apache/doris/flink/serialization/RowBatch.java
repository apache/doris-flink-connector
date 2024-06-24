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

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseIntVector;
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
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.util.IPUtils;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.doris.flink.util.IPUtils.convertLongToIPv4Address;

/** row batch data container. */
public class RowBatch {
    private static final Logger logger = LoggerFactory.getLogger(RowBatch.class);

    public static class Row {
        private final List<Object> cols;

        Row(int colCount) {
            this.cols = new ArrayList<>(colCount);
        }

        public List<Object> getCols() {
            return cols;
        }

        public void put(Object o) {
            cols.add(o);
        }
    }

    // offset for iterate the rowBatch
    private int offsetInRowBatch;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private final List<Row> rowBatch = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;
    private final Schema schema;
    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private final DateTimeFormatter dateTimeFormatter =
            DateTimeFormatter.ofPattern(DATETIME_PATTERN);
    private final DateTimeFormatter dateTimeV2Formatter =
            DateTimeFormatter.ofPattern(DATETIMEV2_PATTERN);
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public List<Row> getRowBatch() {
        return rowBatch;
    }

    public RowBatch(TScanBatchResult nextResult, Schema schema) {
        this.schema = schema;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader =
                new ArrowStreamReader(
                        new ByteArrayInputStream(nextResult.getRows()), rootAllocator);
        this.offsetInRowBatch = 0;
    }

    public RowBatch readArrow() {
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                fieldVectors = root.getFieldVectors();
                if (fieldVectors.size() > schema.size()) {
                    logger.error(
                            "Data schema size '{}' should not be bigger than arrow field size '{}'",
                            schema.size(),
                            fieldVectors.size());
                    throw new DorisException(
                            "Load Doris data failed, schema size of fetch data is wrong.");
                }
                if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                    logger.debug("One batch in arrow has no data.");
                    continue;
                }
                rowCountInOneBatch = root.getRowCount();
                // init the rowBatch
                for (int i = 0; i < rowCountInOneBatch; ++i) {
                    rowBatch.add(new Row(fieldVectors.size()));
                }
                convertArrowToRowBatch();
                readRowCount += root.getRowCount();
            }
            return this;
        } catch (Exception e) {
            logger.error("Read Doris Data failed because: ", e);
            throw new DorisRuntimeException(e.getMessage());
        } finally {
            close();
        }
    }

    public boolean hasNext() {
        return offsetInRowBatch < readRowCount;
    }

    @VisibleForTesting
    public void addValueToRow(int rowIndex, Object obj) {
        if (rowIndex > rowCountInOneBatch) {
            String errMsg =
                    "Get row offset: " + rowIndex + " larger than row size: " + rowCountInOneBatch;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        rowBatch.get(readRowCount + rowIndex).put(obj);
    }

    public void convertArrowToRowBatch() throws DorisException {
        try {
            for (int col = 0; col < fieldVectors.size(); col++) {
                FieldVector fieldVector = fieldVectors.get(col);
                Types.MinorType minorType = fieldVector.getMinorType();
                final String currentType = schema.get(col).getType();
                for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                    boolean passed = doConvert(col, rowIndex, minorType, currentType, fieldVector);
                    if (!passed) {
                        throw new java.lang.IllegalArgumentException(
                                "FLINK type is "
                                        + currentType
                                        + ", but arrow type is "
                                        + minorType.name()
                                        + ".");
                    }
                }
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    @VisibleForTesting
    public boolean doConvert(
            int col,
            int rowIndex,
            Types.MinorType minorType,
            String currentType,
            FieldVector fieldVector)
            throws DorisException {
        switch (currentType) {
            case "NULL_TYPE":
                break;
            case "BOOLEAN":
                if (!minorType.equals(Types.MinorType.BIT)) {
                    return false;
                }
                BitVector bitVector = (BitVector) fieldVector;
                Object fieldValue =
                        bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                addValueToRow(rowIndex, fieldValue);
                break;
            case "TINYINT":
                if (!minorType.equals(Types.MinorType.TINYINT)) {
                    return false;
                }
                TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
                fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "SMALLINT":
                if (!minorType.equals(Types.MinorType.SMALLINT)) {
                    return false;
                }
                SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
                fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "INT":
                if (!minorType.equals(Types.MinorType.INT)) {
                    return false;
                }
                IntVector intVector = (IntVector) fieldVector;
                fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "IPV4":
                if (!minorType.equals(Types.MinorType.UINT4)
                        && !minorType.equals(Types.MinorType.INT)) {
                    return false;
                }
                BaseIntVector ipv4Vector;
                if (minorType.equals(Types.MinorType.INT)) {
                    ipv4Vector = (IntVector) fieldVector;

                } else {
                    ipv4Vector = (UInt4Vector) fieldVector;
                }
                fieldValue =
                        ipv4Vector.isNull(rowIndex)
                                ? null
                                : convertLongToIPv4Address(ipv4Vector.getValueAsLong(rowIndex));
                addValueToRow(rowIndex, fieldValue);
                break;
            case "BIGINT":
                if (!minorType.equals(Types.MinorType.BIGINT)) {
                    return false;
                }
                BigIntVector bigIntVector = (BigIntVector) fieldVector;
                fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "FLOAT":
                if (!minorType.equals(Types.MinorType.FLOAT4)) {
                    return false;
                }
                Float4Vector float4Vector = (Float4Vector) fieldVector;
                fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "TIME":
            case "DOUBLE":
                if (!minorType.equals(Types.MinorType.FLOAT8)) {
                    return false;
                }
                Float8Vector float8Vector = (Float8Vector) fieldVector;
                fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "BINARY":
                if (!minorType.equals(Types.MinorType.VARBINARY)) {
                    return false;
                }
                VarBinaryVector varBinaryVector = (VarBinaryVector) fieldVector;
                fieldValue =
                        varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128I":
                if (!minorType.equals(Types.MinorType.DECIMAL)) {
                    return false;
                }
                DecimalVector decimalVector = (DecimalVector) fieldVector;
                if (decimalVector.isNull(rowIndex)) {
                    addValueToRow(rowIndex, null);
                    break;
                }
                BigDecimal value = decimalVector.getObject(rowIndex).stripTrailingZeros();
                addValueToRow(rowIndex, value);
                break;
            case "DATE":
            case "DATEV2":
                if (!minorType.equals(Types.MinorType.DATEDAY)
                        && !minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                if (minorType.equals(Types.MinorType.VARCHAR)) {
                    VarCharVector date = (VarCharVector) fieldVector;
                    if (date.isNull(rowIndex)) {
                        addValueToRow(rowIndex, null);
                        break;
                    }
                    String stringValue = new String(date.get(rowIndex));
                    LocalDate localDate = LocalDate.parse(stringValue, dateFormatter);
                    addValueToRow(rowIndex, localDate);
                } else {
                    DateDayVector date = (DateDayVector) fieldVector;
                    if (date.isNull(rowIndex)) {
                        addValueToRow(rowIndex, null);
                        break;
                    }
                    LocalDate localDate = LocalDate.ofEpochDay(date.get(rowIndex));
                    addValueToRow(rowIndex, localDate);
                }
                break;
            case "DATETIME":
                if (!minorType.equals(Types.MinorType.TIMESTAMPMICRO)
                        && !minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                if (minorType.equals(Types.MinorType.VARCHAR)) {
                    VarCharVector varCharVector = (VarCharVector) fieldVector;
                    if (varCharVector.isNull(rowIndex)) {
                        addValueToRow(rowIndex, null);
                        break;
                    }
                    String stringValue = new String(varCharVector.get(rowIndex));
                    LocalDateTime parse = LocalDateTime.parse(stringValue, dateTimeFormatter);
                    addValueToRow(rowIndex, parse);
                } else {
                    LocalDateTime dateTime = getDateTime(rowIndex, fieldVector);
                    addValueToRow(rowIndex, dateTime);
                }
                break;
            case "DATETIMEV2":
                if (!minorType.equals(Types.MinorType.TIMESTAMPMICRO)
                        && !minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                if (minorType.equals(Types.MinorType.VARCHAR)) {
                    VarCharVector varCharVector = (VarCharVector) fieldVector;
                    if (varCharVector.isNull(rowIndex)) {
                        addValueToRow(rowIndex, null);
                        break;
                    }
                    String stringValue = new String(varCharVector.get(rowIndex));
                    stringValue = completeMilliseconds(stringValue);
                    LocalDateTime parse = LocalDateTime.parse(stringValue, dateTimeV2Formatter);
                    addValueToRow(rowIndex, parse);
                } else {
                    LocalDateTime dateTime = getDateTime(rowIndex, fieldVector);
                    addValueToRow(rowIndex, dateTime);
                }
                break;
            case "LARGEINT":
                if (!minorType.equals(Types.MinorType.FIXEDSIZEBINARY)
                        && !minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                if (minorType.equals(Types.MinorType.FIXEDSIZEBINARY)) {
                    FixedSizeBinaryVector largeIntVector = (FixedSizeBinaryVector) fieldVector;
                    if (largeIntVector.isNull(rowIndex)) {
                        addValueToRow(rowIndex, null);
                        break;
                    }
                    byte[] bytes = largeIntVector.get(rowIndex);
                    int left = 0, right = bytes.length - 1;
                    while (left < right) {
                        byte temp = bytes[left];
                        bytes[left] = bytes[right];
                        bytes[right] = temp;
                        left++;
                        right--;
                    }
                    BigInteger largeInt = new BigInteger(bytes);
                    addValueToRow(rowIndex, largeInt);
                    break;
                } else {
                    VarCharVector largeIntVector = (VarCharVector) fieldVector;
                    if (largeIntVector.isNull(rowIndex)) {
                        addValueToRow(rowIndex, null);
                        break;
                    }
                    String stringValue = new String(largeIntVector.get(rowIndex));
                    BigInteger largeInt = new BigInteger(stringValue);
                    addValueToRow(rowIndex, largeInt);
                    break;
                }
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "JSONB":
            case "VARIANT":
                if (!minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                VarCharVector varCharVector = (VarCharVector) fieldVector;
                if (varCharVector.isNull(rowIndex)) {
                    addValueToRow(rowIndex, null);
                    break;
                }
                String stringValue = new String(varCharVector.get(rowIndex));
                addValueToRow(rowIndex, stringValue);
                break;
            case "IPV6":
                if (!minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                VarCharVector ipv6VarcharVector = (VarCharVector) fieldVector;
                if (ipv6VarcharVector.isNull(rowIndex)) {
                    addValueToRow(rowIndex, null);
                    break;
                }
                String ipv6Str = new String(ipv6VarcharVector.get(rowIndex));
                String ipv6Address = IPUtils.fromBigInteger(new BigInteger(ipv6Str));
                addValueToRow(rowIndex, ipv6Address);
                break;
            case "ARRAY":
                if (!minorType.equals(Types.MinorType.LIST)) {
                    return false;
                }
                ListVector listVector = (ListVector) fieldVector;
                Object listValue =
                        listVector.isNull(rowIndex) ? null : listVector.getObject(rowIndex);
                // todo: when the subtype of array is date, conversion is required
                addValueToRow(rowIndex, listValue);
                break;
            case "MAP":
                if (!minorType.equals(Types.MinorType.MAP)) {
                    return false;
                }
                MapVector mapVector = (MapVector) fieldVector;
                UnionMapReader reader = mapVector.getReader();
                if (mapVector.isNull(rowIndex)) {
                    addValueToRow(rowIndex, null);
                    break;
                }
                reader.setPosition(rowIndex);
                Map<String, Object> mapValue = new HashMap<>();
                while (reader.next()) {
                    mapValue.put(reader.key().readObject().toString(), reader.value().readObject());
                }
                addValueToRow(rowIndex, mapValue);
                break;
            case "STRUCT":
                if (!minorType.equals(Types.MinorType.STRUCT)) {
                    return false;
                }
                StructVector structVector = (StructVector) fieldVector;
                if (structVector.isNull(rowIndex)) {
                    addValueToRow(rowIndex, null);
                    break;
                }
                Map<String, ?> structValue = structVector.getObject(rowIndex);
                addValueToRow(rowIndex, structValue);
                break;
            default:
                String errMsg = "Unsupported type " + schema.get(col).getType();
                logger.error(errMsg);
                throw new DorisException(errMsg);
        }
        return true;
    }

    @VisibleForTesting
    public LocalDateTime getDateTime(int rowIndex, FieldVector fieldVector) {
        TimeStampMicroVector vector = (TimeStampMicroVector) fieldVector;
        if (vector.isNull(rowIndex)) {
            return null;
        }
        long time = vector.get(rowIndex);
        Instant instant;
        if (time / 10000000000L == 0) { // datetime(0)
            instant = Instant.ofEpochSecond(time);
        } else if (time / 10000000000000L == 0) { // datetime(3)
            instant = Instant.ofEpochMilli(time);
        } else { // datetime(6)
            instant = Instant.ofEpochSecond(time / 1000000, time % 1000000 * 1000);
        }
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return dateTime;
    }

    @VisibleForTesting
    public static String completeMilliseconds(String stringValue) {
        if (stringValue.length() == DATETIMEV2_PATTERN.length()) {
            return stringValue;
        }

        if (stringValue.length() < DATETIME_PATTERN.length()) {
            return stringValue;
        }

        StringBuilder sb = new StringBuilder(stringValue);
        if (stringValue.length() == DATETIME_PATTERN.length()) {
            sb.append(".");
        }
        while (sb.toString().length() < DATETIMEV2_PATTERN.length()) {
            sb.append(0);
        }
        return sb.toString();
    }

    public List<Object> next() {
        if (!hasNext()) {
            String errMsg =
                    "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        return rowBatch.get(offsetInRowBatch++).getCols();
    }

    private String typeMismatchMessage(final String flinkType, final Types.MinorType arrowType) {
        final String messageTemplate = "FLINK type is %1$s, but arrow type is %2$s.";
        return String.format(messageTemplate, flinkType, arrowType.name());
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }
}
