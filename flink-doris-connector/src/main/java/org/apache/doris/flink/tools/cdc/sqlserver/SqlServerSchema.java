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

package org.apache.doris.flink.tools.cdc.sqlserver;

import org.apache.doris.flink.catalog.DorisTypeMapper;
import org.apache.doris.flink.tools.cdc.SourceSchema;

import java.sql.DatabaseMetaData;

public class SqlServerSchema extends SourceSchema {

    public static final String SQLSERVER_FUNCTIOIN_PATTERN = ".+\\(.*\\)";

    public SqlServerSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String schemaName,
            String tableName,
            String tableComment)
            throws Exception {
        super(metaData, databaseName, schemaName, tableName, tableComment);
    }

    @Override
    public String convertToDorisType(String fieldType, Integer precision, Integer scale) {
        return SqlServerType.toDorisType(fieldType, precision, scale);
    }

    @Override
    public String convertStringDefault(String defaultStr, String dorisType) {
        // (SYSDATETIMEOFFSET())
        if (defaultStr.matches("\\(.*\\)")) {
            if (defaultStr.matches(SQLSERVER_FUNCTIOIN_PATTERN)) {
                return null;
            }
            return defaultStr.replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("'", "");
        }
        return null;
    }

    @Override
    public String convertNoDateTimeDefault(String defaultStr, String dorisType) {
        if (defaultStr.matches("\\(\\(.*\\)\\)")) {
            return defaultStr.replaceAll("\\(", "").replaceAll("\\)", "");
        }
        return null;
    }

    @Override
    public String convertDateTimeDefault(String defaultStr, String dorisType) {
        String defaultStrUpperCase = defaultStr.toUpperCase();
        if (defaultStrUpperCase.startsWith(DEFAULT_CURRENT_TIMESTAMP)) {
            // In SQL Server, the default length of CURRENT_TIMESTAMP is 6.
            long length = 6;
            int startIndex = defaultStrUpperCase.indexOf('(') + 1;
            int endIndex = defaultStrUpperCase.indexOf(')');
            // In MariaDB, the CURRENT_TIMESTAMP() is the same as CURRENT_TIMESTAMP(0) in MySQL.
            if (endIndex >= 0) {
                String substring = defaultStrUpperCase.substring(startIndex, endIndex).trim();
                length = substring.isEmpty() ? 0 : Long.parseLong(substring);
                if (length > DorisTypeMapper.MAX_SUPPORTED_DATE_TIME_PRECISION) {
                    length = DorisTypeMapper.MAX_SUPPORTED_DATE_TIME_PRECISION;
                }
            }
            return String.format("CURRENT_TIMESTAMP(%d)", length);
        }
        String s = convertStringDefault(defaultStr, dorisType);
        return handleTimestampNanoseconds(s);
    }
}
