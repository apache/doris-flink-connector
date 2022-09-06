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

public class DorisTypeMapper {

    // -------------------------number----------------------------
    private static final String DORIS_TINYINT = "TINYINT";
    private static final String DORIS_SMALLINT = "SMALLINT";
    private static final String DORIS_INT = "INT";
    private static final String DORIS_BIGINT = "BIGINT";
    private static final String DORIS_LARGEINT = "BIGINT UNSIGNED";
    private static final String DORIS_DECIMAL = "DECIMAL";
    private static final String DORIS_FLOAT = "FLOAT";
    private static final String DORIS_DOUBLE = "DOUBLE";

    // -------------------------string----------------------------
    private static final String DORIS_CHAR = "CHAR";
    private static final String DORIS_VARCHAR = "VARCHAR";
    private static final String DORIS_STRING = "STRING";
    private static final String DORIS_TEXT = "TEXT";

    // ------------------------------time-------------------------
    private static final String DORIS_DATE = "DATE";
    private static final String DORIS_DATETIME = "DATETIME";

    //------------------------------bool------------------------
    private static final String DORIS_BOOLEAN = "BOOLEAN";


    public static DataType toFlinkType(String columnName, String columnType, int precision, int scale) {
        columnType = columnType.toUpperCase();
        switch (columnType) {
            case DORIS_BOOLEAN:
                return DataTypes.BOOLEAN();
            case DORIS_TINYINT:
                if (precision == 0) {
                    //The boolean type will become tinyint when queried in information_schema, and precision=0
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.TINYINT();
                }
            case DORIS_SMALLINT:
                return DataTypes.SMALLINT();
            case DORIS_INT:
                return DataTypes.INT();
            case DORIS_BIGINT:
                return DataTypes.BIGINT();
            case DORIS_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case DORIS_FLOAT:
                return DataTypes.FLOAT();
            case DORIS_DOUBLE:
                return DataTypes.DOUBLE();
            case DORIS_CHAR:
                return DataTypes.CHAR(precision);
            case DORIS_LARGEINT:
            case DORIS_VARCHAR:
            case DORIS_STRING:
            case DORIS_TEXT:
                return DataTypes.STRING();
            case DORIS_DATE:
                return DataTypes.DATE();
            case DORIS_DATETIME:
                return DataTypes.TIMESTAMP(0);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support Doris type '%s' on column '%s'", columnType, columnName));
        }
    }
}
