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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.sink.writer.EventType;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Synchronize the schema change in the upstream data source to the doris database table.
 *
 * <p>There are two schema change modes:<br>
 * 1. {@link JsonDebeziumSchemaChangeImpl} only supports table column name and column type changes,
 * and this mode is used by default. <br>
 * 2. {@link JsonDebeziumSchemaChangeImplV2} supports table column name, column type, default,
 * comment synchronization, supports multi-column changes, and supports column name rename. Need to
 * be enabled by configuring use-new-schema-change.
 */
public abstract class JsonDebeziumSchemaChange extends CdcSchemaChange {
    private static final Logger LOG = LoggerFactory.getLogger(JsonDebeziumSchemaChange.class);
    protected static String addDropDDLRegex =
            "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s]+))?.*";
    protected Pattern addDropDDLPattern;

    // table name of the cdc upstream, format is db.tbl
    protected String sourceTableName;
    protected DorisOptions dorisOptions;
    protected ObjectMapper objectMapper;
    // <cdc db.schema.table, doris db.table>
    protected Map<String, String> tableMapping;
    protected SchemaChangeManager schemaChangeManager;
    protected JsonDebeziumChangeContext changeContext;
    protected SourceConnector sourceConnector;
    protected String targetDatabase;
    protected String targetTablePrefix;
    protected String targetTableSuffix;
    protected DorisTableConfig dorisTableConfig;

    public abstract boolean schemaChange(JsonNode recordRoot);

    public abstract void init(JsonNode recordRoot, String dorisTableName);

    /** When cdc synchronizes multiple tables, it will capture multiple table schema changes. */
    protected boolean checkTable(JsonNode recordRoot) {
        String db = extractDatabase(recordRoot);
        String tbl = extractTable(recordRoot);
        String dbTbl = db + "." + tbl;
        return sourceTableName.equals(dbTbl);
    }

    @Override
    protected String extractDatabase(JsonNode record) {
        if (record.get("source").has("schema")) {
            // compatible with schema
            return extractJsonNode(record.get("source"), "schema");
        } else {
            return extractJsonNode(record.get("source"), "db");
        }
    }

    @Override
    protected String extractTable(JsonNode record) {
        return extractJsonNode(record.get("source"), "table");
    }

    protected String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null && !(record.get(key) instanceof NullNode)
                ? record.get(key).asText()
                : null;
    }

    /**
     * Parse doris database and table as a tuple.
     *
     * @param record from flink cdc.
     * @return Tuple(database, table)
     */
    protected Tuple2<String, String> getDorisTableTuple(JsonNode record) {
        String identifier =
                JsonDebeziumChangeUtils.getDorisTableIdentifier(record, dorisOptions, tableMapping);
        if (StringUtils.isNullOrWhitespaceOnly(identifier)) {
            return null;
        }
        String[] tableInfo = identifier.split("\\.");
        if (tableInfo.length != 2) {
            return null;
        }
        return Tuple2.of(tableInfo[0], tableInfo[1]);
    }

    @VisibleForTesting
    @Override
    public String getCdcTableIdentifier(JsonNode record) {
        String db = extractJsonNode(record.get("source"), "db");
        String schema = extractJsonNode(record.get("source"), "schema");
        String table = extractJsonNode(record.get("source"), "table");
        return SourceSchema.getString(db, schema, table);
    }

    protected JsonNode extractHistoryRecord(JsonNode record) throws JsonProcessingException {
        if (record != null && record.has("historyRecord")) {
            return objectMapper.readTree(record.get("historyRecord").asText());
        }
        // The ddl passed by some scenes will not be included in the historyRecord,
        // such as DebeziumSourceFunction
        return record;
    }

    /** Parse event type. */
    protected EventType extractEventType(JsonNode record) throws JsonProcessingException {
        JsonNode tableChange = extractTableChange(record);
        if (tableChange == null || tableChange.get("type") == null) {
            return null;
        }
        String type = tableChange.get("type").asText();
        if (EventType.ALTER.toString().equalsIgnoreCase(type)) {
            return EventType.ALTER;
        } else if (EventType.CREATE.toString().equalsIgnoreCase(type)) {
            return EventType.CREATE;
        }
        LOG.warn("Not supported this event type. type={}", type);
        return null;
    }

    protected JsonNode extractTableChange(JsonNode record) throws JsonProcessingException {
        JsonNode historyRecord = extractHistoryRecord(record);
        JsonNode tableChanges = historyRecord.get("tableChanges");
        if (Objects.nonNull(tableChanges)) {
            return tableChanges.get(0);
        }
        LOG.warn("Failed to extract tableChanges. record={}", record);
        return null;
    }

    protected boolean executeAlterDDLs(
            List<String> ddlSqlList,
            JsonNode recordRoot,
            Tuple2<String, String> dorisTableTuple,
            boolean status)
            throws IOException, IllegalArgumentException {
        if (CollectionUtils.isEmpty(ddlSqlList)) {
            LOG.info("The recordRoot cannot extract ddl sql. recordRoot={}", recordRoot);
            return false;
        }

        for (String ddlSql : ddlSqlList) {
            status = schemaChangeManager.execute(ddlSql, dorisTableTuple.f0);
            LOG.info("schema change status:{}, ddl: {}", status, ddlSql);
        }
        return status;
    }

    protected void extractSourceConnector(JsonNode record) {
        if (Objects.isNull(sourceConnector)) {
            sourceConnector =
                    SourceConnector.valueOf(
                            record.get("source").get("connector").asText().toUpperCase());
        }
    }

    protected String getCreateTableIdentifier(JsonNode record) {
        String table = extractJsonNode(record.get("source"), "table");
        return targetDatabase + "." + targetTablePrefix + table + targetTableSuffix;
    }

    public Map<String, String> getTableMapping() {
        return tableMapping;
    }

    @VisibleForTesting
    public void setSchemaChangeManager(SchemaChangeManager schemaChangeManager) {
        this.schemaChangeManager = schemaChangeManager;
    }
}
