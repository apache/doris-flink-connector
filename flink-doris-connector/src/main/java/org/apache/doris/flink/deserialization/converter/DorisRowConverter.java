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
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class DorisRowConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private final DeserializationConverter[] deserializationConverters;

    public DorisRowConverter(RowType rowType) {
        checkNotNull(rowType);
        this.deserializationConverters = new DeserializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            deserializationConverters[i] = createNullableConverter(rowType.getTypeAt(i));
        }
    }

    /**
     * Convert data retrieved from {@link RowBatch} to  {@link RowData}.
     *
     * @param record from rowBatch
     */
    public GenericRowData convert(List record){
        GenericRowData rowData = new GenericRowData(deserializationConverters.length);
        for (int i = 0; i < deserializationConverters.length ; i++) {
            rowData.setField(i, deserializationConverters[i].deserialize(record.get(i)));
        }
        return rowData;
    }


    /**
     * Create a nullable runtime {@link DeserializationConverter} from given {@link
     * LogicalType}.
     */
    protected DeserializationConverter createNullableConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createConverter(type));
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

    protected DeserializationConverter createConverter(LogicalType type) {
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
            case DATE:
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
}
