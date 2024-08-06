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

package org.apache.doris.flink.tools.cdc.db2;

import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.tools.cdc.JdbcSourceSchema;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;

public class Db2Schema extends JdbcSourceSchema {
    public Db2Schema(
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
        return Db2Type.toDorisType(fieldType, precision, scale);
    }

    @Override
    public String getCdcTableName() {
        return schemaName + "\\." + tableName;
    }

    @Override
    public LinkedHashMap<String, FieldSchema> getColumnInfo(
            DatabaseMetaData metaData, String databaseName, String schemaName, String tableName)
            throws SQLException {
        return super.getColumnInfo(metaData, null, schemaName, tableName);
    }
}
