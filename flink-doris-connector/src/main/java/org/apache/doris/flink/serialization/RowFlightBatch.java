package org.apache.doris.flink.serialization;

import org.apache.flink.annotation.VisibleForTesting;

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
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.util.FastDateUtil;
import org.apache.doris.flink.util.IPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.doris.flink.util.IPUtils.convertLongToIPv4Address;

public class RowFlightBatch {
    private static final Logger logger = LoggerFactory.getLogger(RowFlightBatch.class);

    public static class Row {
        private List<Object> cols;

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
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private List<Row> rowBatch = new ArrayList<>();
    private ArrowReader arrowStreamReader;
    private VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private final Schema schema;
    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";

    public RowFlightBatch(ArrowReader nextResult, Schema schema) {
        this.schema = schema;
        this.arrowStreamReader = nextResult;
        this.offsetInRowBatch = 0;
    }

    public RowFlightBatch readArrow() {
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            fieldVectors = root.getFieldVectors();
            if (fieldVectors.size() > schema.size()) {
                logger.error(
                        "Schema size '{}' is not equal to arrow field size '{}'.",
                        fieldVectors.size(),
                        schema.size());
                throw new DorisException(
                        "Load Doris data failed, schema size of fetch data is wrong.");
            }
            if (fieldVectors.isEmpty() || root.getRowCount() == 0) {
                logger.debug("One batch in arrow has no data.");
                return null;
            }
            rowCountInOneBatch = root.getRowCount();
            for (int i = 0; i < rowCountInOneBatch; ++i) {
                rowBatch.add(new Row(fieldVectors.size()));
            }
            convertArrowToRowBatch();
            readRowCount += root.getRowCount();
            return this;
        } catch (DorisException e) {
            logger.error("Read Doris Data failed because: ", e);
            throw new DorisRuntimeException(e.getMessage());
        } catch (IOException e) {
            return this;
        }
    }

    public boolean hasNext() {
        if (offsetInRowBatch < readRowCount) {
            return true;
        }
        return false;
    }

    private void addValueToRow(int rowIndex, Object obj) {
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
                        throw new IllegalArgumentException(
                                "FLINK type is "
                                        + currentType
                                        + ", but arrow type is "
                                        + minorType.name()
                                        + ".");
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            fieldVectors.clear();
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
                Object fieldValueTiny =
                        tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValueTiny);
                break;
            case "SMALLINT":
                if (!minorType.equals(Types.MinorType.SMALLINT)) {
                    return false;
                }
                SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
                Object fieldValue_smallint =
                        smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue_smallint);
                break;
            case "INT":
                if (!minorType.equals(Types.MinorType.INT)) {
                    return false;
                }
                IntVector intVector = (IntVector) fieldVector;
                Object fieldValueIntVector =
                        intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValueIntVector);
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
                Object fieldValue_ipv4Vector =
                        ipv4Vector.isNull(rowIndex)
                                ? null
                                : convertLongToIPv4Address(ipv4Vector.getValueAsLong(rowIndex));
                addValueToRow(rowIndex, fieldValue_ipv4Vector);
                break;
            case "BIGINT":
                if (!minorType.equals(Types.MinorType.BIGINT)) {
                    return false;
                }
                BigIntVector bigIntVector = (BigIntVector) fieldVector;
                Object fieldValueBigIntVector =
                        bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValueBigIntVector);
                break;
            case "FLOAT":
                if (!minorType.equals(Types.MinorType.FLOAT4)) {
                    return false;
                }
                Float4Vector float4Vector = (Float4Vector) fieldVector;
                Object fieldValueFloat4Vector =
                        float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                addValueToRow(rowIndex, fieldValueFloat4Vector);
                break;
            case "TIME":
            case "DOUBLE":
                if (!minorType.equals(Types.MinorType.FLOAT8)) {
                    return false;
                }
                Float8Vector float8Vector = (Float8Vector) fieldVector;
                Object fieldValueFloat8Vector =
                        float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                addValueToRow(rowIndex, fieldValueFloat8Vector);
                break;
            case "BINARY":
                if (!minorType.equals(Types.MinorType.VARBINARY)) {
                    return false;
                }
                VarBinaryVector varBinaryVector = (VarBinaryVector) fieldVector;
                Object fieldValueVarBinaryVector =
                        varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValueVarBinaryVector);
                break;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128I":
            case "DECIMAL128":
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
                    String stringValue = new String(date.get(rowIndex), StandardCharsets.UTF_8);
                    LocalDate localDate = FastDateUtil.fastParseDate(stringValue, "yyyy-MM-dd");
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
            case "DATETIMEV2":
                if (!minorType.equals(Types.MinorType.TIMESTAMPMICRO)
                        && !minorType.equals(Types.MinorType.TIMESTAMPSEC)
                        && !minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                if (minorType.equals(Types.MinorType.VARCHAR)) {
                    VarCharVector varCharVector = (VarCharVector) fieldVector;
                    if (varCharVector.isNull(rowIndex)) {
                        addValueToRow(rowIndex, null);
                        break;
                    }
                    String stringValue =
                            new String(varCharVector.get(rowIndex), StandardCharsets.UTF_8);
                    stringValue = completeMilliseconds(stringValue);
                    LocalDateTime parse =
                            FastDateUtil.fastParseDateTime(
                                    stringValue, "yyyy-MM-dd HH:mm:ss.SSSSSS");
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
                    String stringValue =
                            new String(largeIntVector.get(rowIndex), StandardCharsets.UTF_8);
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
                String stringValue =
                        new String(varCharVector.get(rowIndex), StandardCharsets.UTF_8);
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
                String ipv6Str =
                        new String(ipv6VarcharVector.get(rowIndex), StandardCharsets.UTF_8);
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
        if (fieldVector instanceof TimeStampMicroVector) {
            return getDateTimeFromTimeStampMicro(rowIndex, fieldVector);
        } else {
            return getDateTimeFromTimeStampSec(rowIndex, fieldVector);
        }
    }

    private LocalDateTime getDateTimeFromTimeStampMicro(int rowIndex, FieldVector fieldVector) {
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

    private LocalDateTime getDateTimeFromTimeStampSec(int rowIndex, FieldVector fieldVector) {
        TimeStampSecVector vector = (TimeStampSecVector) fieldVector;
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

        if (stringValue.length() == DATETIME_PATTERN.length()) {
            stringValue += ".";
        }
        int s = DATETIMEV2_PATTERN.length() - stringValue.length();
        switch (s) {
            case 1:
                return stringValue + "0";
            case 2:
                return stringValue + "00";
            case 3:
                return stringValue + "000";
            case 4:
                return stringValue + "0000";
            case 5:
                return stringValue + "00000";
            case 6:
                return stringValue + "000000";
            default:
                return stringValue;
        }
        //        while (sb.toString().length() < DATETIMEV2_PATTERN.length()) {
        //            sb.add("0");
        //        }
        //        return sb.toString();
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

    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (root != null) {
                root.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }
}
