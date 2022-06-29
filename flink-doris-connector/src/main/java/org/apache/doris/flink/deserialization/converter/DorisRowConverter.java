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

import org.apache.doris.flink.serialization.RowBatch;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class DorisRowConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private final DeserializationConverter[] deserializationConverters;
    private final SerializationConverter[] serializationConverters;

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

    /**
     * Convert data retrieved from {@link RowBatch} to  {@link RowData}.
     *
     * @param record from rowBatch
     */
    public GenericRowData convertInternal(List record) {
        GenericRowData rowData = new GenericRowData(deserializationConverters.length);
        for (int i = 0; i < deserializationConverters.length ; i++) {
            rowData.setField(i, deserializationConverters[i].deserialize(record.get(i)));
        }
        return rowData;
    }

    /**
     * Convert data from {@link RowData} to {@link RowBatch}
     * @param rowData record from flink rowdata
     * @param index the field index
     * @return java type value.
     */
    public Object convertExternal(RowData rowData, int index) {
        return serializationConverters[index].serialize(index, rowData);
    }


    /**
     * Create a nullable runtime {@link DeserializationConverter} from given {@link
     * LogicalType}.
     */
    protected DeserializationConverter createNullableInternalConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createInternalConverter(type));
    }

    protected DeserializationConverter wrapIntoNullableInternalConverter(
            DeserializationConverter deserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return deserializationConverter.deserialize(val);
            }
        };
    }

    protected SerializationConverter createNullableExternalConverter(LogicalType type) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type));
    }

    protected SerializationConverter wrapIntoNullableExternalConverter(SerializationConverter serializationConverter) {
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
    interface DeserializationConverter extends Serializable {
        /**
         * Convert a doris field object of {@link RowBatch } to the  data structure object.
         *
         * @param field
         */
        Object deserialize(Object field);
    }

    /**
     * Runtime converter to convert {@link RowData} type object to doris field.
     */
    @FunctionalInterface
    interface SerializationConverter extends Serializable {
        Object serialize(int index, RowData field);
    }

    protected DeserializationConverter createInternalConverter(LogicalType type) {
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
                    } else {
                        throw new UnsupportedOperationException("timestamp type must be java.time.LocalDateTime, the actual type is: " + val.getClass().getName());
                    }
                };
            case DATE:
                return val -> {
                    if (val instanceof LocalDate) {
                        return (int) ((LocalDate) val).toEpochDay();
                    } else {
                        throw new UnsupportedOperationException("timestamp type must be java.time.LocalDate, the actual type is: " + val.getClass());
                    }
                };
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString((String) val);
            case TIME_WITHOUT_TIME_ZONE:
            case BINARY:
            case VARBINARY:
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    protected SerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return ((index, val) -> null);
            case CHAR:
            case VARCHAR:
                return (index, val) -> val.getString(index);
            case BOOLEAN:
                return (index, val) -> val.getBoolean(index);
            case BINARY:
            case VARBINARY:
                return (index, val) -> val.getBinary(index);
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (index, val) -> val.getDecimal(index, decimalPrecision, decimalScale);
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
            case MULTISET:
            case MAP:
            case ROW:
            case STRUCTURED_TYPE:
            case DISTINCT_TYPE:
            case RAW:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
