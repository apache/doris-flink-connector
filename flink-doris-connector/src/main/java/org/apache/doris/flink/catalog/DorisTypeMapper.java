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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import org.apache.doris.flink.catalog.doris.DorisType;

import static org.apache.doris.flink.catalog.doris.DorisType.ARRAY;
import static org.apache.doris.flink.catalog.doris.DorisType.BIGINT;
import static org.apache.doris.flink.catalog.doris.DorisType.BOOLEAN;
import static org.apache.doris.flink.catalog.doris.DorisType.CHAR;
import static org.apache.doris.flink.catalog.doris.DorisType.DATE;
import static org.apache.doris.flink.catalog.doris.DorisType.DATETIME;
import static org.apache.doris.flink.catalog.doris.DorisType.DATETIME_V2;
import static org.apache.doris.flink.catalog.doris.DorisType.DATE_V2;
import static org.apache.doris.flink.catalog.doris.DorisType.DECIMAL;
import static org.apache.doris.flink.catalog.doris.DorisType.DECIMAL_V3;
import static org.apache.doris.flink.catalog.doris.DorisType.DOUBLE;
import static org.apache.doris.flink.catalog.doris.DorisType.FLOAT;
import static org.apache.doris.flink.catalog.doris.DorisType.INT;
import static org.apache.doris.flink.catalog.doris.DorisType.IPV4;
import static org.apache.doris.flink.catalog.doris.DorisType.IPV6;
import static org.apache.doris.flink.catalog.doris.DorisType.JSON;
import static org.apache.doris.flink.catalog.doris.DorisType.JSONB;
import static org.apache.doris.flink.catalog.doris.DorisType.LARGEINT;
import static org.apache.doris.flink.catalog.doris.DorisType.MAP;
import static org.apache.doris.flink.catalog.doris.DorisType.SMALLINT;
import static org.apache.doris.flink.catalog.doris.DorisType.STRING;
import static org.apache.doris.flink.catalog.doris.DorisType.STRUCT;
import static org.apache.doris.flink.catalog.doris.DorisType.TINYINT;
import static org.apache.doris.flink.catalog.doris.DorisType.VARCHAR;
import static org.apache.doris.flink.catalog.doris.DorisType.VARIANT;

public class DorisTypeMapper {

    /** Max size of char type of Doris. */
    public static final int MAX_CHAR_SIZE = 255;

    /** Max size of varchar type of Doris. */
    public static final int MAX_VARCHAR_SIZE = 65533;
    /* Max precision of datetime type of Doris. */
    public static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;

    public static DataType toFlinkType(
            String columnName, String columnType, int precision, int scale) {
        columnType = columnType.toUpperCase();
        switch (columnType) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                if (precision == 0) {
                    // The boolean type will become tinyint when queried in information_schema, and
                    // precision=0
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.TINYINT();
                }
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case DECIMAL:
            case DECIMAL_V3:
                return DataTypes.DECIMAL(precision, scale);
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case CHAR:
                return DataTypes.CHAR(precision);
            case VARCHAR:
                return DataTypes.VARCHAR(precision);
            case LARGEINT:
            case STRING:
            case JSONB:
            case JSON:
                // Currently, the subtype of the generic cannot be obtained,
                // so it is mapped to string
            case ARRAY:
            case MAP:
            case STRUCT:
            case IPV4:
            case IPV6:
            case VARIANT:
                return DataTypes.STRING();
            case DATE:
            case DATE_V2:
                return DataTypes.DATE();
            case DATETIME:
            case DATETIME_V2:
                return DataTypes.TIMESTAMP(scale);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support Doris type '%s' on column '%s'",
                                columnType, columnName));
        }
    }

    public static String toDorisType(DataType flinkType) {
        LogicalType logicalType = flinkType.getLogicalType();
        return logicalType.accept(new LogicalTypeVisitor(logicalType));
    }

    private static class LogicalTypeVisitor extends LogicalTypeDefaultVisitor<String> {
        private final LogicalType type;

        LogicalTypeVisitor(LogicalType type) {
            this.type = type;
        }

        @Override
        public String visit(CharType charType) {
            long length = charType.getLength() * 3L;
            if (length <= MAX_CHAR_SIZE) {
                return String.format("%s(%s)", DorisType.CHAR, length);
            } else {
                return visit(new VarCharType(charType.getLength()));
            }
        }

        @Override
        public String visit(VarCharType varCharType) {
            // Flink varchar length max value is int, it may overflow after multiplying by 3
            long length = varCharType.getLength() * 3L;
            return length >= MAX_VARCHAR_SIZE ? STRING : String.format("%s(%s)", VARCHAR, length);
        }

        @Override
        public String visit(BooleanType booleanType) {
            return BOOLEAN;
        }

        @Override
        public String visit(VarBinaryType varBinaryType) {
            return STRING;
        }

        @Override
        public String visit(DecimalType decimalType) {
            int precision = decimalType.getPrecision();
            int scale = decimalType.getScale();
            return precision <= 38
                    ? String.format(
                            "%s(%s,%s)", DorisType.DECIMAL_V3, precision, Math.max(scale, 0))
                    : DorisType.STRING;
        }

        @Override
        public String visit(TinyIntType tinyIntType) {
            return TINYINT;
        }

        @Override
        public String visit(SmallIntType smallIntType) {
            return SMALLINT;
        }

        @Override
        public String visit(IntType intType) {
            return INT;
        }

        @Override
        public String visit(BigIntType bigIntType) {
            return BIGINT;
        }

        @Override
        public String visit(FloatType floatType) {
            return FLOAT;
        }

        @Override
        public String visit(DoubleType doubleType) {
            return DOUBLE;
        }

        @Override
        public String visit(DateType dateType) {
            return DATE_V2;
        }

        @Override
        public String visit(TimestampType timestampType) {
            int precision = timestampType.getPrecision();
            return String.format(
                    "%s(%s)", DorisType.DATETIME_V2, Math.min(Math.max(precision, 0), 6));
        }

        @Override
        public String visit(ZonedTimestampType timestampType) {
            int precision = timestampType.getPrecision();
            return String.format(
                    "%s(%s)", DorisType.DATETIME_V2, Math.min(Math.max(precision, 0), 6));
        }

        @Override
        public String visit(LocalZonedTimestampType localZonedTimestampType) {
            int precision = localZonedTimestampType.getPrecision();
            return String.format(
                    "%s(%s)", DorisType.DATETIME_V2, Math.min(Math.max(precision, 0), 6));
        }

        @Override
        public String visit(TimeType timeType) {
            return STRING;
        }

        @Override
        public String visit(ArrayType arrayType) {
            return STRING;
        }

        @Override
        public String visit(MapType mapType) {
            return STRING;
        }

        @Override
        public String visit(RowType rowType) {
            return STRING;
        }

        @Override
        public String visit(MultisetType multisetType) {
            return STRING;
        }

        @Override
        public String visit(BinaryType binaryType) {
            return STRING;
        }

        @Override
        protected String defaultMethod(LogicalType logicalType) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Flink doesn't support converting type %s to Doris type yet.",
                            type.toString()));
        }
    }
}
