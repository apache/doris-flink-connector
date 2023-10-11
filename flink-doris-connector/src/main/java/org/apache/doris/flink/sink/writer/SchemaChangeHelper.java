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

package org.apache.doris.flink.sink.writer;

import org.apache.doris.flink.catalog.doris.FieldSchema;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SchemaChangeHelper {
    private static final List<String> dropFieldSchemas = Lists.newArrayList();
    private static final List<FieldSchema> addFieldSchemas = Lists.newArrayList();
    // Used to determine whether the doris table supports ddl
    private static final List<DDLSchema> ddlSchemas = Lists.newArrayList();
    public static final String ADD_DDL = "ALTER TABLE %s ADD COLUMN %s %s";
    public static final String DROP_DDL = "ALTER TABLE %s DROP COLUMN %s";

    public static void compareSchema(Map<String, FieldSchema> updateFiledSchemaMap,
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

    public static List<String> generateDDLSql(String table) {
        ddlSchemas.clear();
        List<String> ddlList = Lists.newArrayList();
        for (FieldSchema fieldSchema : addFieldSchemas) {
            String name = fieldSchema.getName();
            String type = fieldSchema.getTypeString();
            String defaultValue = fieldSchema.getDefaultValue();
            String comment = fieldSchema.getComment();
            String addDDL = String.format(ADD_DDL, table, name, type);
            if (!StringUtils.isNullOrWhitespaceOnly(defaultValue)) {
                addDDL = addDDL + " DEFAULT " + defaultValue;
            }
            if (!StringUtils.isNullOrWhitespaceOnly(comment)) {
                addDDL = addDDL + " COMMENT " + comment;
            }
            ddlList.add(addDDL);
            ddlSchemas.add(new DDLSchema(name, false));
        }
        for (String columName : dropFieldSchemas) {
            String dropDDL = String.format(DROP_DDL, table, columName);
            ddlList.add(dropDDL);
            ddlSchemas.add(new DDLSchema(columName, true));
        }

        dropFieldSchemas.clear();
        addFieldSchemas.clear();
        return ddlList;
    }

    public static List<DDLSchema> getDdlSchemas() {
        return ddlSchemas;
    }

    static class DDLSchema {
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
