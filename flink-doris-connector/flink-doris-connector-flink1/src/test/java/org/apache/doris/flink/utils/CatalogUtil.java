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

package org.apache.doris.flink.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.Map;

public class CatalogUtil {
    public static CatalogTable createTable(TableSchema tableSchema, Map<String, String> options) {
        return new CatalogTableImpl(tableSchema, options, "FlinkTable");
    }

    public static TableSchema getTableSchema() {
        return TableSchema.builder()
                .field("id", new AtomicDataType(new VarCharType(false, 128)))
                .field("c_boolean", DataTypes.BOOLEAN())
                .field("c_char", DataTypes.CHAR(1))
                .field("c_date", DataTypes.DATE())
                .field("c_datetime", DataTypes.TIMESTAMP(0))
                .field("c_decimal", DataTypes.DECIMAL(10, 2))
                .field("c_double", DataTypes.DOUBLE())
                .field("c_float", DataTypes.FLOAT())
                .field("c_int", DataTypes.INT())
                .field("c_bigint", DataTypes.BIGINT())
                .field("c_largeint", DataTypes.STRING())
                .field("c_smallint", DataTypes.SMALLINT())
                .field("c_string", DataTypes.STRING())
                .field("c_tinyint", DataTypes.TINYINT())
                .field("c_array", DataTypes.ARRAY(DataTypes.INT()))
                .field("c_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                .field("c_row", DataTypes.ROW())
                .field("c_varbinary", DataTypes.VARBINARY(16))
                .primaryKey("id")
                .build();
    }

    public static String[] getColumns() {
        return getTableSchema().getFieldNames();
    }
}
