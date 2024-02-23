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

package org.apache.doris.flink.tools.cdc;

import org.apache.flink.util.StringUtils;

import org.apache.doris.flink.catalog.DorisTypeMapper;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.catalog.doris.DorisType;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public abstract class SourceSchema {
    public static final String DEFAULT_FUNCTION_REGEX = ".*\\(.*\\)";
    public static final String DEFAULT_STRING_SINGLE_QUOTE = "(?<!')'";
    public static final String DEFAULT_CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    private final String databaseName;
    private final String schemaName;
    private final String tableName;
    private final String tableComment;
    private final LinkedHashMap<String, FieldSchema> fields;
    public final List<String> primaryKeys;
    public DataModel model = DataModel.UNIQUE;

    public SourceSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String schemaName,
            String tableName,
            String tableComment)
            throws Exception {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableComment = tableComment;
        // Oracle support '/' in table name, we give it a special treatment
        fields = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getColumns(databaseName, schemaName, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String comment = rs.getString("REMARKS");
                String fieldType = rs.getString("TYPE_NAME");
                String defaultValue = rs.getString("COLUMN_DEF");
                Integer precision = rs.getInt("COLUMN_SIZE");
                if (rs.wasNull()) {
                    precision = null;
                }

                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                String dorisTypeStr = convertToDorisType(fieldType, precision, scale);
                String convertedDefault = convertToDoriDefaultValue(defaultValue, dorisTypeStr);
                fields.put(
                        fieldName,
                        new FieldSchema(fieldName, dorisTypeStr, convertedDefault, comment));
            }
        }

        primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, schemaName, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }
        this.tableName = tableName.replace("%", "_");
    }

    public String convertToDoriDefaultValue(String defaultStr, String dorisType) {
        if (defaultStr == null) {
            return null;
        }
        String dorisTypeStrUpperCase = dorisType.toUpperCase();
        switch (dorisTypeStrUpperCase) {
            case DorisType.TINYINT:
            case DorisType.SMALLINT:
            case DorisType.INT:
            case DorisType.BIGINT:
            case DorisType.LARGEINT:
            case DorisType.DOUBLE:
                return convertNoDateTimeDefault(defaultStr, dorisType);
            case DorisType.JSON:
            case DorisType.JSONB:
                return convertStringDefault(defaultStr, dorisType);
            default:
                if (dorisTypeStrUpperCase.startsWith(DorisType.CHAR)
                        || dorisTypeStrUpperCase.startsWith(DorisType.VARCHAR)
                        || dorisTypeStrUpperCase.startsWith(DorisType.STRING)) {
                    return convertStringDefault(defaultStr, dorisType);
                } else if (dorisTypeStrUpperCase.startsWith(DorisType.DATETIME)
                        || dorisTypeStrUpperCase.startsWith(DorisType.DATETIME_V2)) {
                    return convertDateTimeDefault(defaultStr, dorisType);
                } else if (dorisTypeStrUpperCase.startsWith(DorisType.DECIMAL)
                        || dorisTypeStrUpperCase.startsWith(DorisType.DECIMAL_V3)) {
                    return convertNoDateTimeDefault(defaultStr, dorisType);
                } else if (dorisTypeStrUpperCase.startsWith(DorisType.DATE)
                        || dorisTypeStrUpperCase.startsWith(DorisType.DATE_V2)) {
                    return convertDateTimeDefault(defaultStr, dorisType);
                }
                return null;
        }
    }

    /**
     * Convert default value of numeric type to Doris type. In MySQL8.x, the default function may
     * like `((rand() * rand()))`, `((curdate() + interval 1 year))`, etc. We can't convert these
     * default value to Doris default value, so we return null.
     *
     * @param defaultStr default value of source Database
     * @param dorisType the type of the field in Doris
     */
    public String convertNoDateTimeDefault(String defaultStr, String dorisType) {
        if (defaultStr.matches(DEFAULT_FUNCTION_REGEX)) {
            return null;
        }
        return defaultStr.replaceAll(DEFAULT_STRING_SINGLE_QUOTE, "").replace(" ", "");
    }

    public String convertDateTimeDefault(String defaultStr, String dorisType) {
        String defaultStrUpperCase = defaultStr.toUpperCase();
        if (defaultStrUpperCase.startsWith(DEFAULT_CURRENT_TIMESTAMP)) {
            long length = 0;
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
        } else if (defaultStrUpperCase.matches(DEFAULT_FUNCTION_REGEX)) {
            return null;
        }
        return handleTimestampNanoseconds(defaultStr);
    }

    public String handleTimestampNanoseconds(String defaultStr) {
        if (null == defaultStr || defaultStr.isEmpty()) {
            return null;
        }

        if (defaultStr.length() < 20) {
            return defaultStr;
        }
        String[] split = defaultStr.split("\\.");
        if (split.length != 2) {
            return null;
        }
        String nanoseconds = split[1];
        if (nanoseconds.length() > 6) {
            nanoseconds = nanoseconds.substring(0, 6);
        }
        return split[0] + "." + nanoseconds;
    }

    public String convertStringDefault(String defaultStr, String dorisType) {
        return defaultStr.replaceAll("'", "");
    }

    public abstract String convertToDorisType(String fieldType, Integer precision, Integer scale);

    public String getTableIdentifier() {
        return getString(databaseName, schemaName, tableName);
    }

    public static String getString(String databaseName, String schemaName, String tableName) {
        StringJoiner identifier = new StringJoiner(".");
        if (!StringUtils.isNullOrWhitespaceOnly(databaseName)) {
            identifier.add(databaseName);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(schemaName)) {
            identifier.add(schemaName);
        }

        if (!StringUtils.isNullOrWhitespaceOnly(tableName)) {
            identifier.add(tableName);
        }

        return identifier.toString();
    }

    public TableSchema convertTableSchema(Map<String, String> tableProps) {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setModel(this.model);
        tableSchema.setFields(this.fields);
        tableSchema.setKeys(buildKeys());
        tableSchema.setTableComment(this.tableComment);
        tableSchema.setDistributeKeys(buildDistributeKeys());
        tableSchema.setProperties(tableProps);
        return tableSchema;
    }

    private List<String> buildKeys() {
        return buildDistributeKeys();
    }

    private List<String> buildDistributeKeys() {
        if (!this.primaryKeys.isEmpty()) {
            return primaryKeys;
        }
        if (!this.fields.isEmpty()) {
            Map.Entry<String, FieldSchema> firstField = this.fields.entrySet().iterator().next();
            return Collections.singletonList(firstField.getKey());
        }
        return new ArrayList<>();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, FieldSchema> getFields() {
        return fields;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public String getTableComment() {
        return tableComment;
    }

    public DataModel getModel() {
        return model;
    }

    public void setModel(DataModel model) {
        this.model = model;
    }
}
