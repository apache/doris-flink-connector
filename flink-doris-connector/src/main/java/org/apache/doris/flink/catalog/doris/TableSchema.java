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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSchema {
    public static final String DORIS_TABLE_REGEX = "^[a-zA-Z][a-zA-Z0-9-_]*$";
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

    @Override
    public String toString() {
        return "TableSchema{"
                + "database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", tableComment='"
                + tableComment
                + '\''
                + ", fields="
                + fields
                + ", keys="
                + String.join(",", keys)
                + ", model="
                + model.name()
                + ", distributeKeys="
                + String.join(",", distributeKeys)
                + ", properties="
                + properties
                + ", tableBuckets="
                + tableBuckets
                + '}';
    }
}
