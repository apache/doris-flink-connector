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

package org.apache.doris.flink.sink.schema;

import org.apache.flink.util.StringUtils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.catalog.doris.FieldSchema;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SchemaChangeHelper {
    public static final String DEFAULT_DATABASE = "information_schema";

    private static final List<String> dropFieldSchemas = Lists.newArrayList();
    private static final List<FieldSchema> addFieldSchemas = Lists.newArrayList();
    // Used to determine whether the doris table supports ddl
    private static final List<DDLSchema> ddlSchemas = Lists.newArrayList();
    private static final String ADD_DDL = "ALTER TABLE %s ADD COLUMN %s %s";
    private static final String DROP_DDL = "ALTER TABLE %s DROP COLUMN %s";
    private static final String RENAME_DDL = "ALTER TABLE %s RENAME COLUMN %s %s";
    private static final String CHECK_COLUMN_EXISTS =
            "SELECT COLUMN_NAME FROM information_schema.`COLUMNS` WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_NAME = '%s'";
    private static final String CHECK_DATABASE_EXISTS =
            "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA` WHERE SCHEMA_NAME = '%s'";
    private static final String CREATE_DATABASE_DDL = "CREATE DATABASE IF NOT EXISTS %s";
    private static final String MODIFY_TYPE_DDL = "ALTER TABLE %s MODIFY COLUMN %s %s";
    private static final String MODIFY_COMMENT_DDL = "ALTER TABLE %s MODIFY COLUMN %s COMMENT '%s'";

    public static void compareSchema(
            Map<String, FieldSchema> updateFiledSchemaMap,
            Map<String, FieldSchema> originFieldSchemaMap) {
        dropFieldSchemas.clear();
        addFieldSchemas.clear();
        for (Entry<String, FieldSchema> updateFieldSchema : updateFiledSchemaMap.entrySet()) {
            String columName = updateFieldSchema.getKey();
            if (!originFieldSchemaMap.containsKey(columName)) {
                addFieldSchemas.add(updateFieldSchema.getValue());
                originFieldSchemaMap.put(columName, updateFieldSchema.getValue());
            }
        }
        for (Entry<String, FieldSchema> originFieldSchema : originFieldSchemaMap.entrySet()) {
            String columName = originFieldSchema.getKey();
            if (!updateFiledSchemaMap.containsKey(columName)) {
                dropFieldSchemas.add(columName);
            }
        }
        if (CollectionUtils.isNotEmpty(dropFieldSchemas)) {
            dropFieldSchemas.forEach(originFieldSchemaMap::remove);
        }
    }

    public static List<String> generateRenameDDLSql(
            String table,
            String oldColumnName,
            String newColumnName,
            Map<String, FieldSchema> originFieldSchemaMap) {
        ddlSchemas.clear();
        List<String> ddlList = Lists.newArrayList();
        FieldSchema fieldSchema = null;
        for (Entry<String, FieldSchema> originFieldSchema : originFieldSchemaMap.entrySet()) {
            if (originFieldSchema.getKey().equals(oldColumnName)) {
                fieldSchema = originFieldSchema.getValue();
                ddlList.add(buildRenameColumnDDL(table, oldColumnName, newColumnName));
                ddlSchemas.add(new DDLSchema(oldColumnName, false));
            }
        }
        originFieldSchemaMap.remove(oldColumnName);
        originFieldSchemaMap.put(newColumnName, fieldSchema);
        return ddlList;
    }

    public static List<String> generateDDLSql(String table) {
        ddlSchemas.clear();
        List<String> ddlList = Lists.newArrayList();
        for (FieldSchema fieldSchema : addFieldSchemas) {
            ddlList.add(buildAddColumnDDL(table, fieldSchema));
            ddlSchemas.add(new DDLSchema(fieldSchema.getName(), false));
        }
        for (String columName : dropFieldSchemas) {
            ddlList.add(buildDropColumnDDL(table, columName));
            ddlSchemas.add(new DDLSchema(columName, true));
        }

        dropFieldSchemas.clear();
        addFieldSchemas.clear();
        return ddlList;
    }

    public static String buildAddColumnDDL(String tableIdentifier, FieldSchema fieldSchema) {
        String name = fieldSchema.getName();
        String type = fieldSchema.getTypeString();
        String defaultValue = fieldSchema.getDefaultValue();
        String comment = fieldSchema.getComment();
        StringBuilder addDDL =
                new StringBuilder(
                        String.format(
                                ADD_DDL,
                                DorisSystem.quoteTableIdentifier(tableIdentifier),
                                DorisSystem.identifier(name),
                                type));
        if (defaultValue != null) {
            addDDL.append(" DEFAULT ").append(DorisSystem.quoteDefaultValue(defaultValue));
        }
        commentColumn(addDDL, comment);
        return addDDL.toString();
    }

    public static String buildDropColumnDDL(String tableIdentifier, String columName) {
        return String.format(
                DROP_DDL,
                DorisSystem.quoteTableIdentifier(tableIdentifier),
                DorisSystem.identifier(columName));
    }

    public static String buildRenameColumnDDL(
            String tableIdentifier, String oldColumnName, String newColumnName) {
        return String.format(
                RENAME_DDL,
                DorisSystem.quoteTableIdentifier(tableIdentifier),
                DorisSystem.identifier(oldColumnName),
                DorisSystem.identifier(newColumnName));
    }

    public static String buildColumnExistsQuery(String database, String table, String column) {
        return String.format(CHECK_COLUMN_EXISTS, database, table, column);
    }

    public static String buildDatabaseExistsQuery(String database) {
        return String.format(CHECK_DATABASE_EXISTS, database);
    }

    public static String buildCreateDatabaseDDL(String database) {
        return String.format(CREATE_DATABASE_DDL, database);
    }

    public static String buildModifyColumnCommentDDL(
            String tableIdentifier, String columnName, String newComment) {
        return String.format(
                MODIFY_COMMENT_DDL,
                DorisSystem.quoteTableIdentifier(tableIdentifier),
                DorisSystem.identifier(columnName),
                DorisSystem.quoteComment(newComment));
    }

    public static String buildModifyColumnDataTypeDDL(
            String tableIdentifier, FieldSchema fieldSchema) {
        String columnName = fieldSchema.getName();
        String dataType = fieldSchema.getTypeString();
        String comment = fieldSchema.getComment();
        StringBuilder modifyDDL =
                new StringBuilder(
                        String.format(
                                MODIFY_TYPE_DDL,
                                DorisSystem.quoteTableIdentifier(tableIdentifier),
                                DorisSystem.identifier(columnName),
                                dataType));
        commentColumn(modifyDDL, comment);
        return modifyDDL.toString();
    }

    private static void commentColumn(StringBuilder ddl, String comment) {
        if (!StringUtils.isNullOrWhitespaceOnly(comment)) {
            ddl.append(" COMMENT '").append(DorisSystem.quoteComment(comment)).append("'");
        }
    }

    public static List<DDLSchema> getDdlSchemas() {
        return ddlSchemas;
    }

    public static class DDLSchema {
        private final String columnName;
        private final boolean isDropColumn;

        public DDLSchema(String columnName, boolean isDropColumn) {
            this.columnName = columnName;
            this.isDropColumn = isDropColumn;
        }

        public String getColumnName() {
            return columnName;
        }

        public boolean isDropColumn() {
            return isDropColumn;
        }
    }
}
