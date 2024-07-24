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

package org.apache.doris.flink.tools.cdc.db2;

import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.catalog.doris.DorisType;

public class Db2Type {
    private static final String BOOLEAN = "BOOLEAN";
    private static final String SMALLINT = "SMALLINT";
    private static final String INTEGER = "INTEGER";
    private static final String INT = "INT";
    private static final String BIGINT = "BIGINT";
    private static final String REAL = "REAL";
    private static final String DECFLOAT = "DECFLOAT";
    private static final String DOUBLE = "DOUBLE";
    private static final String DECIMAL = "DECIMAL";
    private static final String NUMERIC = "NUMERIC";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String CHARACTER = "CHARACTER";
    private static final String CHAR = "CHAR";
    private static final String LONG_VARCHAR = "LONG VARCHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String XML = "XML";
    private static final String VARGRAPHIC = "VARGRAPHIC";

    public static String toDorisType(String db2Type, Integer precision, Integer scale) {
        db2Type = db2Type.toUpperCase();
        switch (db2Type) {
            case BOOLEAN:
                return DorisType.BOOLEAN;
            case SMALLINT:
                return DorisType.SMALLINT;
            case INTEGER:
            case INT:
                return DorisType.INT;
            case BIGINT:
                return DorisType.BIGINT;
            case REAL:
                return DorisType.FLOAT;
            case DOUBLE:
                return DorisType.DOUBLE;
            case DATE:
                return DorisType.DATE_V2;
            case DECFLOAT:
            case DECIMAL:
            case NUMERIC:
                if (precision != null && precision > 0 && precision <= 38) {
                    if (scale != null && scale >= 0) {
                        return String.format("%s(%s,%s)", DorisType.DECIMAL_V3, precision, scale);
                    }
                    return String.format("%s(%s,%s)", DorisType.DECIMAL_V3, precision, 0);
                } else {
                    return DorisType.STRING;
                }
            case CHARACTER:
            case CHAR:
            case VARCHAR:
            case LONG_VARCHAR:
                Preconditions.checkNotNull(precision);
                return precision * 3 > 65533
                        ? DorisType.STRING
                        : String.format("%s(%s)", DorisType.VARCHAR, precision * 3);
            case TIMESTAMP:
                return String.format(
                        "%s(%s)", DorisType.DATETIME_V2, Math.min(scale == null ? 0 : scale, 6));
            case TIME:
            case VARGRAPHIC:
                // Currently, the Flink CDC connector does not support the XML data type from DB2.
                // Case XML:
                return DorisType.STRING;
            default:
                throw new UnsupportedOperationException("Unsupported DB2 Type: " + db2Type);
        }
    }
}
