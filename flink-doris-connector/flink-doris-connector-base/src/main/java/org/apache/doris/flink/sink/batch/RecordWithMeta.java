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

package org.apache.doris.flink.sink.batch;

public class RecordWithMeta {
    private String database;
    private String table;
    private String record;

    public RecordWithMeta() {}

    public RecordWithMeta(String database, String table, String record) {
        this.database = database;
        this.table = table;
        this.record = record;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }

    public String getTableIdentifier() {
        return this.database + "." + this.table;
    }

    @Override
    public String toString() {
        return "RecordWithMeta{"
                + "database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", record='"
                + record
                + '\''
                + '}';
    }
}
