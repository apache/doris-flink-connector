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

package org.apache.doris.flink.sink.writer.serializer.jsondebezium;

import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.doris.flink.tools.cdc.db2.Db2Type;
import org.apache.doris.flink.tools.cdc.mysql.MysqlType;
import org.apache.doris.flink.tools.cdc.oracle.OracleType;
import org.apache.doris.flink.tools.cdc.postgres.PostgresType;
import org.apache.doris.flink.tools.cdc.sqlserver.SqlServerType;

import java.util.Map;

public class JsonDebeziumChangeUtils {

    public static String getDorisTableIdentifier(
            JsonNode record, DorisOptions dorisOptions, Map<String, String> tableMapping) {
        String identifier = getCdcTableIdentifier(record);
        return getDorisTableIdentifier(identifier, dorisOptions, tableMapping);
    }

    public static String getDorisTableIdentifier(
            String cdcTableIdentifier,
            DorisOptions dorisOptions,
            Map<String, String> tableMapping) {
        if (!StringUtils.isNullOrWhitespaceOnly(dorisOptions.getTableIdentifier())) {
            return dorisOptions.getTableIdentifier();
        }
        if (!CollectionUtil.isNullOrEmpty(tableMapping)
                && !StringUtils.isNullOrWhitespaceOnly(cdcTableIdentifier)
                && tableMapping.get(cdcTableIdentifier) != null) {
            return tableMapping.get(cdcTableIdentifier);
        }
        return null;
    }

    public static String getCdcTableIdentifier(JsonNode record) {
        String db = extractJsonNode(record.get("source"), "db");
        String schema = extractJsonNode(record.get("source"), "schema");
        String table = extractJsonNode(record.get("source"), "table");
        return SourceSchema.getString(db, schema, table);
    }

    public static String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null && !(record.get(key) instanceof NullNode)
                ? record.get(key).asText()
                : null;
    }

    public static String buildDorisTypeName(
            SourceConnector sourceConnector, String dataType, Integer length, Integer scale) {
        String dorisTypeName;
        switch (sourceConnector) {
            case MYSQL:
                dorisTypeName = MysqlType.toDorisType(dataType, length, scale);
                break;
            case ORACLE:
                dorisTypeName = OracleType.toDorisType(dataType, length, scale);
                break;
            case POSTGRES:
                dorisTypeName = PostgresType.toDorisType(dataType, length, scale);
                break;
            case SQLSERVER:
                dorisTypeName = SqlServerType.toDorisType(dataType, length, scale);
                break;
            case DB2:
                dorisTypeName = Db2Type.toDorisType(dataType, length, scale);
                break;
            default:
                String errMsg = sourceConnector + " not support " + dataType + " schema change.";
                throw new UnsupportedOperationException(errMsg);
        }
        return dorisTypeName;
    }
}
