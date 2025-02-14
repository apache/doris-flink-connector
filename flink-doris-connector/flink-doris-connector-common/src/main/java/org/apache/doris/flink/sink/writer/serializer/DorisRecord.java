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

package org.apache.doris.flink.sink.writer.serializer;

import java.io.Serializable;

public class DorisRecord implements Serializable {

    public static DorisRecord empty = new DorisRecord();

    private String database;
    private String table;
    private byte[] row;

    public DorisRecord() {}

    public DorisRecord(String database, String table, byte[] row) {
        this.database = database;
        this.table = table;
        this.row = row;
    }

    public String getTableIdentifier() {
        if (database == null || table == null) {
            return null;
        }
        return database + "." + table;
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

    public byte[] getRow() {
        return row;
    }

    public void setRow(byte[] row) {
        this.row = row;
    }

    public static DorisRecord of(String database, String table, byte[] row) {
        return new DorisRecord(database, table, row);
    }

    public static DorisRecord of(String tableIdentifier, byte[] row) {
        if (tableIdentifier != null) {
            String[] dbTbl = tableIdentifier.split("\\.");
            if (dbTbl.length == 2) {
                String database = dbTbl[0];
                String table = dbTbl[1];
                return new DorisRecord(database, table, row);
            }
        }
        return null;
    }

    public static DorisRecord of(byte[] row) {
        return new DorisRecord(null, null, row);
    }
}
