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

package org.apache.doris.flink.catalog.doris;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSchema {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSchema.class);
    public static final String DORIS_TABLE_REGEX = "^[a-zA-Z][a-zA-Z0-9-_]*$";
    public static final String DORIS_COLUMN_REGEX =
            "^[_a-zA-Z@0-9\\s<>/][.a-zA-Z0-9_+-/><?@#$%^&*\"\\s,:]{0,255}$";
    private String database;
    private String table;
    private String tableComment;
    private Map<String, FieldSchema> fields;
    private List<String> keys = new ArrayList<>();
    private DataModel model = DataModel.DUPLICATE;
    private List<String> distributeKeys = new ArrayList<>();
    private Map<String, String> properties = new HashMap<>();

    private Integer tableBuckets;

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getTableComment() {
        return tableComment;
    }

    public Map<String, FieldSchema> getFields() {
        return fields;
    }

    public List<String> getKeys() {
        return keys;
    }

    public DataModel getModel() {
        return model;
    }

    public List<String> getDistributeKeys() {
        return distributeKeys;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public void setFields(Map<String, FieldSchema> fields) {
        this.fields = fields;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public void setModel(DataModel model) {
        this.model = model;
    }

    public void setDistributeKeys(List<String> distributeKeys) {
        this.distributeKeys = distributeKeys;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setTableBuckets(Integer tableBuckets) {
        this.tableBuckets = tableBuckets;
    }

    public Integer getTableBuckets() {
        return tableBuckets;
    }

    public static boolean isInvalidDorisTable(String tableName) {
        if (!tableName.matches(TableSchema.DORIS_TABLE_REGEX)) {
            LOGGER.warn(
                    String.format(
                            "The table name '%s' is invalid. Table names in Doris must match the regex pattern '%s'. Please consider renaming the table or use the 'excluding-tables' option to filter it out.",
                            tableName, TableSchema.DORIS_TABLE_REGEX));
            return true;
        }
        return false;
    }

    public static boolean isInValidDorisColumnName(String tableName, String columnName) {
        if (!columnName.matches(TableSchema.DORIS_COLUMN_REGEX)) {
            LOGGER.warn(
                    String.format(
                            "Incorrect column name '%s' is invalid. column names in Doris must match the regex pattern '%s'. Please consider renaming the column or use the 'excluding-tables' option to filter the table '%s'.",
                            columnName, TableSchema.DORIS_COLUMN_REGEX, tableName));
            return true;
        }
        return false;
    }
}
