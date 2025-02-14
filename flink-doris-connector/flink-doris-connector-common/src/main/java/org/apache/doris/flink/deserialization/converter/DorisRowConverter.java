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

package org.apache.doris.flink.deserialization.converter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.serialization.RowBatch;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class DorisRowConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private DeserializationConverter[] deserializationConverters;
    private SerializationConverter[] serializationConverters;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public DorisRowConverter() {}

    public DorisRowConverter(RowType rowType) {
        checkNotNull(rowType);
        this.deserializationConverters = new DeserializationConverter[rowType.getFieldCount()];
        this.serializationConverters = new SerializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            deserializationConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
            serializationConverters[i] = createNullableExternalConverter(rowType.getTypeAt(i));
        }
    }

    public DorisRowConverter(DataType[] dataTypes) {
        checkNotNull(dataTypes);
        this.deserializationConverters = new DeserializationConverter[dataTypes.length];
        this.serializationConverters = new SerializationConverter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            LogicalType logicalType = dataTypes[i].getLogicalType();
            deserializationConverters[i] = createNullableInternalConverter(logicalType);
            serializationConverters[i] = createNullableExternalConverter(logicalType);
        }
    }

    public DorisRowConverter setExternalConverter(DataType[] dataTypes) {
        checkNotNull(dataTypes);
        this.serializationConverters = new SerializationConverter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            LogicalType logicalType = dataTypes[i].getLogicalType();
            serializationConverters[i] = createNullableExternalConverter(logicalType);
        }
        return this;
    }

    /**
     * Convert data retrieved from {@link RowBatch} to {@link RowData}.
     *
     * @param record from rowBatch
     */
    public GenericRowData convertInternal(List record) {
        GenericRowData rowData = new GenericRowData(deserializationConverters.length);
        for (int i = 0; i < deserializationConverters.length; i++) {
            rowData.setField(i, deserializationConverters[i].deserialize(record.get(i)));
        }
        return rowData;
    }

    /**
     * Convert data from {@link RowData} to {@link RowBatch}.
     *
     * @param rowData record from flink rowdata
     * @param index the field index
     * @return java type value.
     */
    public Object convertExternal(RowData rowData, int index) {
        return serializationConverters[index].serialize(index, rowData);
    }

    /**
     * Create a nullable runtime {@link DeserializationConverter} from given {@link LogicalType}.
     */
    public DeserializationConverter createNullableInternalConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createInternalConverter(type));
    }

    public DeserializationConverter wrapIntoNullableInternalConverter(
            DeserializationConverter deserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return deserializationConverter.deserialize(val);
            }
        };
    }

    public static SerializationConverter createNullableExternalConverter(LogicalType type) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type));
    }

    public static SerializationConverter wrapIntoNullableExternalConverter(
            SerializationConverter serializationConverter) {
        return (index, val) -> {
            if (val == null || val.isNullAt(index)) {
                return null;
            } else {
                return serializationConverter.serialize(index, val);
            }
        };
    }

    /** Runtime converter to convert doris field to {@link RowData} type object. */
    @FunctionalInterface
    public interface DeserializationConverter extends Serializable {
        /**
         * Convert a doris field object of {@link RowBatch } to the data structure object.
         *
         * @param field
         */
        Object deserialize(Object field);
    }

    /** Runtime converter to convert {@link RowData} type object to doris field. */
    @FunctionalInterface
    public interface SerializationConverter extends Serializable {
        Object serialize(int index, RowData field);
    }

    public DeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return val -> val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val -> DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val -> {
                    if (val instanceof LocalDateTime) {
                        return TimestampData.fromLocalDateTime((LocalDateTime) val);
                    } else if (val instanceof Timestamp) {
                        return TimestampData.fromTimestamp((Timestamp) val);
                    } else {
                        throw new UnsupportedOperationException(
                                "timestamp type must be java.time.LocalDateTime or java.sql.Timestamp, the actual type is: "
                                        + val.getClass().getName());
                    }
                };
            case DATE:
                return val -> {
                    if (val instanceof LocalDate) {
                        // doris source
                        return (int) ((LocalDate) val).toEpochDay();
                    } else if (val instanceof Date) {
                        // doris lookup
                        return (int) ((Date) val).toLocalDate().toEpochDay();
                    } else {
                        throw new UnsupportedOperationException(
                                "timestamp type must be java.time.LocalDate, the actual type is: "
                                        + val.getClass());
                    }
                };
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case TIME_WITHOUT_TIME_ZONE:
            case BINARY:
            case VARBINARY:
            case ARRAY:
                return val -> convertArrayData(((List<?>) val).toArray(), type);
            case ROW:
                return val -> convertRowData((Map<String, ?>) val, type);
            case MAP:
                return val -> convertMapData((Map) val, type);
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static SerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return ((index, val) -> null);
            case CHAR:
            case VARCHAR:
                return (index, val) -> val.getString(index).toString();
            case BOOLEAN:
                return (index, val) -> val.getBoolean(index);
            case BINARY:
            case VARBINARY:
                return (index, val) -> val.getBinary(index);
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (index, val) ->
                        val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal();
            case TINYINT:
                return (index, val) -> val.getByte(index);
            case SMALLINT:
                return (index, val) -> val.getShort(index);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return (index, val) -> val.getInt(index);
            case BIGINT:
                return (index, val) -> val.getLong(index);
            case FLOAT:
                return (index, val) -> val.getFloat(index);
            case DOUBLE:
                return (index, val) -> val.getDouble(index);
            case DATE:
                return (index, val) -> Date.valueOf(LocalDate.ofEpochDay(val.getInt(index)));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (index, val) -> val.getTimestamp(index, timestampPrecision).toTimestamp();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int localP = ((LocalZonedTimestampType) type).getPrecision();
                return (index, val) -> val.getTimestamp(index, localP).toTimestamp();
            case TIMESTAMP_WITH_TIME_ZONE:
                final int zonedP = ((ZonedTimestampType) type).getPrecision();
                return (index, val) -> val.getTimestamp(index, zonedP).toTimestamp();
            case ARRAY:
                return (index, val) -> convertArrayData(val.getArray(index), type);
            case MAP:
                return (index, val) -> writeValueAsString(convertMapData(val.getMap(index), type));
            case ROW:
                return (index, val) -> writeValueAsString(convertRowData(val, index, type));
            case MULTISET:
            case STRUCTURED_TYPE:
            case DISTINCT_TYPE:
            case RAW:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private ArrayData convertArrayData(Object[] array, LogicalType type) {
        List<LogicalType> children = type.getChildren();
        Preconditions.checkState(children.size() > 0, "Failed to obtain the item type of array");
        DeserializationConverter converter = createNullableInternalConverter(children.get(0));
        for (int i = 0; i < array.length; i++) {
            Object result = converter.deserialize(array[i]);
            array[i] = result;
        }
        GenericArrayData arrayData = new GenericArrayData(array);
        return arrayData;
    }

    private MapData convertMapData(Map<Object, Object> map, LogicalType type) {
        MapType mapType = (MapType) type;
        DeserializationConverter keyConverter =
                createNullableInternalConverter(mapType.getKeyType());
        DeserializationConverter valueConverter =
                createNullableInternalConverter(mapType.getValueType());
        Map<Object, Object> result = new HashMap<>();
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            Object key = keyConverter.deserialize(entry.getKey());
            Object value = valueConverter.deserialize(entry.getValue());
            result.put(key, value);
        }
        GenericMapData mapData = new GenericMapData(result);
        return mapData;
    }

    private RowData convertRowData(Map<String, ?> row, LogicalType type) {
        RowType rowType = (RowType) type;
        GenericRowData rowData = new GenericRowData(row.size());
        int index = 0;
        for (Map.Entry<String, ?> entry : row.entrySet()) {
            DeserializationConverter converter =
                    createNullableInternalConverter(rowType.getTypeAt(index));
            Object value = converter.deserialize(entry.getValue());
            rowData.setField(index, value);
            index++;
        }
        return rowData;
    }

    private static List<Object> convertArrayData(ArrayData array, LogicalType type) {
        if (array instanceof GenericArrayData) {
            return Arrays.asList(((GenericArrayData) array).toObjectArray());
        }
        if (array instanceof BinaryArrayData) {
            LogicalType elementType = ((ArrayType) type).getElementType();
            List<Object> values =
                    Arrays.asList(((BinaryArrayData) array).toObjectArray(elementType));
            if (LogicalTypeRoot.DATE.equals(elementType.getTypeRoot())) {
                return values.stream()
                        .map(date -> Date.valueOf(LocalDate.ofEpochDay((Integer) date)))
                        .collect(Collectors.toList());
            }
            if (LogicalTypeRoot.ARRAY.equals(elementType.getTypeRoot())) {
                return values.stream()
                        .map(arr -> convertArrayData((ArrayData) arr, elementType))
                        .collect(Collectors.toList());
            }
            return values;
        }
        throw new UnsupportedOperationException("Unsupported array data: " + array.getClass());
    }

    private static Object convertMapData(MapData map, LogicalType type) {
        Map<Object, Object> result = new HashMap<>();
        LogicalType valueType = ((MapType) type).getValueType();
        LogicalType keyType = ((MapType) type).getKeyType();
        if (map instanceof GenericMapData) {
            GenericMapData gMap = (GenericMapData) map;
            for (Object key : ((GenericArrayData) gMap.keyArray()).toObjectArray()) {

                Object convertedKey = convertMapEntry(key, keyType);
                Object convertedValue = convertMapEntry(gMap.get(key), valueType);
                result.put(convertedKey, convertedValue);
            }
            return result;
        } else if (map instanceof BinaryMapData) {
            BinaryMapData bMap = (BinaryMapData) map;
            Map<?, ?> javaMap = bMap.toJavaMap(((MapType) type).getKeyType(), valueType);
            for (Map.Entry<?, ?> entry : javaMap.entrySet()) {
                Object convertedKey = convertMapEntry(entry.getKey(), keyType);
                Object convertedValue = convertMapEntry(entry.getValue(), valueType);
                result.put(convertedKey, convertedValue);
            }
            return result;
        }
        throw new UnsupportedOperationException("Unsupported map data: " + map.getClass());
    }

    /**
     * Converts the key-value pair of MAP to the actual type.
     *
     * @param originValue the original value of key-value pair
     * @param logicalType key or value logical type
     */
    private static Object convertMapEntry(Object originValue, LogicalType logicalType) {
        if (LogicalTypeRoot.MAP.equals(logicalType.getTypeRoot())) {
            return convertMapData((MapData) originValue, logicalType);
        } else if (LogicalTypeRoot.DATE.equals(logicalType.getTypeRoot())) {
            return Date.valueOf(LocalDate.ofEpochDay((Integer) originValue)).toString();
        } else if (LogicalTypeRoot.ARRAY.equals(logicalType.getTypeRoot())) {
            return convertArrayData((ArrayData) originValue, logicalType);
        } else if (originValue instanceof TimestampData) {
            return ((TimestampData) originValue).toTimestamp().toString();
        } else {
            return originValue.toString();
        }
    }

    private static Object convertRowData(RowData val, int index, LogicalType type) {
        RowType rowType = (RowType) type;
        Map<String, Object> value = new HashMap<>();
        RowData row = val.getRow(index, rowType.getFieldCount());

        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField rowField = fields.get(i);
            SerializationConverter converter = createNullableExternalConverter(rowField.getType());
            Object valTmp = converter.serialize(i, row);
            value.put(rowField.getName(), valTmp.toString());
        }
        return value;
    }

    private static String writeValueAsString(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
