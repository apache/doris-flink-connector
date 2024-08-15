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

import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * JdbcSourceSchema is a subclass of SourceSchema, used to build metadata about jdbc-related
 * databases.
 */
public abstract class JdbcSourceSchema extends SourceSchema {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSchema.class);

    public JdbcSourceSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String schemaName,
            String tableName,
            String tableComment)
            throws Exception {
        super(databaseName, schemaName, tableName, tableComment);
        fields = getColumnInfo(metaData, databaseName, schemaName, tableName);
        primaryKeys = getPrimaryKeys(metaData, databaseName, schemaName, tableName);
    }

    public LinkedHashMap<String, FieldSchema> getColumnInfo(
            DatabaseMetaData metaData, String databaseName, String schemaName, String tableName)
            throws SQLException {
        LinkedHashMap<String, FieldSchema> fields = new LinkedHashMap<>();
        LOG.debug("Starting to get column info for table: {}", tableName);
        try (ResultSet rs = metaData.getColumns(databaseName, schemaName, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String comment = rs.getString("REMARKS");
                String fieldType = rs.getString("TYPE_NAME");
                Integer precision = rs.getInt("COLUMN_SIZE");

                if (rs.wasNull()) {
                    precision = null;
                }
                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                String dorisTypeStr = null;
                try {
                    dorisTypeStr = convertToDorisType(fieldType, precision, scale);
                } catch (UnsupportedOperationException e) {
                    throw new UnsupportedOperationException(e + " in table: " + tableName);
                }
                fields.put(fieldName, new FieldSchema(fieldName, dorisTypeStr, comment));
            }
        }
        Preconditions.checkArgument(!fields.isEmpty(), "The column info of {} is empty", tableName);
        LOG.debug("Successfully retrieved column info for table: {}", tableName);
        return fields;
    }

    public List<String> getPrimaryKeys(
            DatabaseMetaData metaData, String databaseName, String schemaName, String tableName)
            throws SQLException {
        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, schemaName, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }

        return primaryKeys;
    }

    public abstract String convertToDorisType(String fieldType, Integer precision, Integer scale);
}
