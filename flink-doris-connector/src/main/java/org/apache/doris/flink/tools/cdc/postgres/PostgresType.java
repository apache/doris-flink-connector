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
package org.apache.doris.flink.tools.cdc.postgres;

import org.apache.doris.flink.catalog.doris.DorisType;
import org.apache.flink.util.Preconditions;

public class PostgresType {
    private static final String INT2 = "int2";
    private static final String SMALLSERIAL = "smallserial";
    private static final String INT4 = "int4";
    private static final String SERIAL = "serial";
    private static final String INT8 = "int8";
    private static final String BIGSERIAL = "bigserial";
    private static final String NUMERIC = "numeric";
    private static final String FLOAT4 = "float4";
    private static final String FLOAT8 = "float8";
    private static final String BPCHAR = "bpchar";
    private static final String TIMESTAMP = "timestamp";
    private static final String TIMESTAMPTZ = "timestamptz";
    private static final String DATE = "date";
    private static final String BOOL = "bool";
    private static final String BIT = "bit";
    private static final String POINT = "point";
    private static final String LINE = "line";
    private static final String LSEG = "lseg";
    private static final String BOX = "box";
    private static final String PATH = "path";
    private static final String POLYGON = "polygon";
    private static final String CIRCLE = "circle";
    private static final String VARCHAR = "varchar";
    private static final String TEXT = "text";
    private static final String TIME = "time";
    private static final String TIMETZ = "timetz";
    private static final String INTERVAL = "interval";
    private static final String CIDR = "cidr";
    private static final String INET = "inet";
    private static final String MACADDR = "macaddr";
    private static final String VARBIT = "varbit";
    private static final String UUID = "uuid";
    private static final String BYTEA = "bytea";
    private static final String JSON = "json";
    private static final String JSONB = "jsonb";

    public static String toDorisType(String postgresType, Integer precision, Integer scale) {
        postgresType = postgresType.toLowerCase();
        switch (postgresType){
            case INT2:
            case SMALLSERIAL:
                return DorisType.TINYINT;
            case INT4:
            case SERIAL:
                return DorisType.INT;
            case INT8:
            case BIGSERIAL:
                return DorisType.BIGINT;
            case NUMERIC:
                return precision != null && precision <= 38
                        ? String.format("%s(%s,%s)", DorisType.DECIMAL_V3, precision, scale != null && scale >= 0 ? scale : 0)
                        : DorisType.STRING;
            case FLOAT4:
                return DorisType.FLOAT;
            case FLOAT8:
                return DorisType.DOUBLE;
            case BPCHAR:
                Preconditions.checkNotNull(precision);
                return String.format("%s(%s)", DorisType.CHAR, precision);
            case TIMESTAMP:
            case TIMESTAMPTZ:
                return String.format("%s(%s)", DorisType.DATETIME_V2, Math.min(precision == null ? 0 : precision, 6));
            case DATE:
                return DorisType.DATE_V2;
            case BOOL:
                return DorisType.BOOLEAN;
            case BIT:
                return precision == 1 ? DorisType.BOOLEAN : DorisType.STRING;
            case POINT:
            case LINE:
            case LSEG:
            case BOX:
            case PATH:
            case POLYGON:
            case CIRCLE:
            case VARCHAR:
            case TEXT:
            case TIME:
            case TIMETZ:
            case INTERVAL:
            case CIDR:
            case INET:
            case MACADDR:
            case VARBIT:
            case UUID:
            case BYTEA:
                return DorisType.STRING;
            case JSON:
            case JSONB:
                return DorisType.JSONB;
            default:
                throw new UnsupportedOperationException("Unsupported Postgres Type: " + postgresType);
        }
    }
}
