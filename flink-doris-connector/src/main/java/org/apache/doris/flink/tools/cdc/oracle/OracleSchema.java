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

package org.apache.doris.flink.tools.cdc.oracle;

import org.apache.doris.flink.catalog.DorisTypeMapper;
import org.apache.doris.flink.tools.cdc.SourceSchema;

import java.sql.DatabaseMetaData;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleSchema extends SourceSchema {
    private static final Pattern TO_DATE =
            Pattern.compile("TO_DATE\\('(.*)',[ ]*'(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TO_TIMESTAMP =
            Pattern.compile("TO_TIMESTAMP\\('(.*)',[ ]*'(.*)'\\)", Pattern.CASE_INSENSITIVE);

    private static final String ORACLE_DATE_PATTERN = "YYYY-MM-DD";
    private static final String ORACLE_DATETIME_PATTERN = "YYYY-MM-DD HH24:MI:SS";

    public OracleSchema(
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
        return OracleType.toDorisType(fieldType, precision, scale);
    }

    @Override
    public String convertStringDefault(String defaultStr, String dorisType) {
        return defaultStr.replaceAll(SourceSchema.DEFAULT_STRING_SINGLE_QUOTE, "");
    }

    @Override
    public String convertDateTimeDefault(String defaultStr, String dorisType) {
        if (defaultStr == null || defaultStr.isEmpty()) {
            return null;
        }
        String defaultStrUpperCase = defaultStr.toUpperCase();
        if (defaultStrUpperCase.startsWith(DEFAULT_CURRENT_TIMESTAMP)) {
            // In Oracle, the precision of CURRENT_TIMESTAMP is optional, and the default precision
            // is 6.
            long length = 6;
            int startIndex = defaultStrUpperCase.indexOf('(') + 1;
            int endIndex = defaultStrUpperCase.indexOf(')');
            if (endIndex >= 0) {
                String substring = defaultStrUpperCase.substring(startIndex, endIndex).trim();
                length = substring.isEmpty() ? 0 : Long.parseLong(substring);
                if (length > DorisTypeMapper.MAX_SUPPORTED_DATE_TIME_PRECISION) {
                    length = DorisTypeMapper.MAX_SUPPORTED_DATE_TIME_PRECISION;
                }
            }
            return String.format("CURRENT_TIMESTAMP(%d)", length);
        }
        return handleOracleTimestampDefault(defaultStr);
    }

    private String handleOracleTimestampDefault(String data) {
        final Matcher toTimestampMatcher = TO_TIMESTAMP.matcher(data);
        if (toTimestampMatcher.matches()) {
            String datetimeText = toTimestampMatcher.group(1);
            String timestampPattern = toTimestampMatcher.group(2);
            return normalizeDateValue(datetimeText, timestampPattern);
        }

        final Matcher toDateMatcher = TO_DATE.matcher(data);
        if (toDateMatcher.matches()) {
            String dateString = toDateMatcher.group(1);
            String datePattern = toDateMatcher.group(2);
            return normalizeDateValue(dateString, datePattern);
        }
        return null;
    }

    private String normalizeDateValue(String stringValue, String datePattern) {
        if (datePattern.length() <= ORACLE_DATE_PATTERN.length()) {
            return normalizeDate(stringValue) + " 00:00:00";
        } else if (datePattern.length() <= ORACLE_DATETIME_PATTERN.length()) {
            return normalizeDateTime(stringValue);
        } else if (datePattern.length() <= ORACLE_DATETIME_PATTERN.length() + 4) {
            return normalizeDateTimeWithNanoseconds(stringValue);
        }
        return null;
    }

    private String normalizeDate(String stringValue) {
        String[] split = stringValue.split("-");
        if (split.length != 3) {
            return null;
        }
        String month = padWithZeroIfNeeded(split[1]);
        String day = padWithZeroIfNeeded(split[2]);

        return split[0] + "-" + month + "-" + day;
    }

    private String normalizeDateTime(String stringValue) {
        String[] split = stringValue.split(" ");
        if (split.length != 2) {
            return null;
        }

        String normalizedDate = normalizeDate(split[0]);
        String[] timeArr = split[1].split(":");
        StringBuilder timeStr = new StringBuilder();
        for (int i = 0; i < timeArr.length; i++) {
            timeStr.append(padWithZeroIfNeeded(timeArr[i]));
            if (i < 2) {
                timeStr.append(":");
            }
        }
        int diff = 3 - timeArr.length;
        for (int i = 0; i < diff; i++) {
            timeStr.append("00");
            if (i != diff - 1) {
                timeStr.append(":");
            }
        }
        return normalizedDate + " " + timeStr;
    }

    private String normalizeDateTimeWithNanoseconds(String stringValue) {
        String[] split = stringValue.split("\\.");
        if (split.length != 2) {
            return null;
        }
        String normalizedDateTime = normalizeDateTime(split[0]);
        String nanoseconds = split[1];
        if (nanoseconds.length() > 6) {
            nanoseconds = nanoseconds.substring(0, 6);
        }
        return normalizedDateTime + "." + nanoseconds;
    }

    private String padWithZeroIfNeeded(String component) {
        return component.length() == 1 ? "0" + component : component;
    }
}
