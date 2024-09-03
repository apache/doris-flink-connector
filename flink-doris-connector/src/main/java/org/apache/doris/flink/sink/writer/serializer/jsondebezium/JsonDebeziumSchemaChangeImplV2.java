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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.flink.catalog.doris.DorisSchemaFactory;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Field;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.sink.schema.SchemaChangeHelper;
import org.apache.doris.flink.sink.schema.SchemaChangeHelper.DDLSchema;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.sink.writer.EventType;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extract the columns that need to be changed based on the change records of the upstream data
 * source.
 */
public class JsonDebeziumSchemaChangeImplV2 extends JsonDebeziumSchemaChange {
    private static final Logger LOG = LoggerFactory.getLogger(JsonDebeziumSchemaChangeImplV2.class);
    private static final Pattern renameDDLPattern =
            Pattern.compile(
                    "ALTER\\s+TABLE\\s+\\w+\\s+"
                            + "(RENAME\\s+COLUMN\\s+(\\w+)\\s+TO\\s+(\\w+)|"
                            + "CHANGE\\s+(?:column\\s+)?(\\w+)\\s+(\\w+)\\s+(\\w+))",
                    Pattern.CASE_INSENSITIVE);
    // schemaChange saves table names, field, and field column information
    private Map<String, Map<String, FieldSchema>> originFieldSchemaMap = new LinkedHashMap<>();
    // create table properties
    private final Set<String> filledTables = new HashSet<>();

    public JsonDebeziumSchemaChangeImplV2(JsonDebeziumChangeContext changeContext) {
        this.addDropDDLPattern = Pattern.compile(addDropDDLRegex, Pattern.CASE_INSENSITIVE);
        this.changeContext = changeContext;
        this.sourceTableName = changeContext.getSourceTableName();
        this.dorisOptions = changeContext.getDorisOptions();
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        this.targetDatabase = changeContext.getTargetDatabase();
        this.dorisTableConfig = changeContext.getDorisTableConf();
        this.tableMapping = changeContext.getTableMapping();
        this.objectMapper = changeContext.getObjectMapper();
        this.targetTablePrefix =
                changeContext.getTargetTablePrefix() == null
                        ? ""
                        : changeContext.getTargetTablePrefix();
        this.targetTableSuffix =
                changeContext.getTargetTableSuffix() == null
                        ? ""
                        : changeContext.getTargetTableSuffix();
    }

    @Override
    public void init(JsonNode recordRoot, String dorisTableName) {
        Set<String> columnNameSet = extractAfterRow(recordRoot).keySet();
        if (CollectionUtils.isEmpty(columnNameSet)) {
            columnNameSet = extractBeforeRow(recordRoot).keySet();
        }
        Map<String, FieldSchema> fieldSchemaMap = new LinkedHashMap<>();
        columnNameSet.forEach(columnName -> fieldSchemaMap.put(columnName, new FieldSchema()));

        originFieldSchemaMap.put(dorisTableName, fieldSchemaMap);
    }

    @Override
    public boolean schemaChange(JsonNode recordRoot) {
        boolean status = false;
        try {
            if (!StringUtils.isNullOrWhitespaceOnly(sourceTableName) && !checkTable(recordRoot)) {
                return false;
            }

            EventType eventType = extractEventType(recordRoot);
            if (eventType == null) {
                LOG.warn("Failed to parse eventType. recordRoot={}", recordRoot);
                return false;
            }
            if (eventType.equals(EventType.CREATE)) {
                TableSchema tableSchema = extractCreateTableSchema(recordRoot);
                status = schemaChangeManager.createTable(tableSchema);
                if (status) {
                    String cdcTbl = getCdcTableIdentifier(recordRoot);
                    String dorisTbl = getCreateTableIdentifier(recordRoot);
                    changeContext.getTableMapping().put(cdcTbl, dorisTbl);
                    this.tableMapping = changeContext.getTableMapping();
                    LOG.info(
                            "create table ddl status: {}, add tableMapping {},{}",
                            status,
                            cdcTbl,
                            dorisTbl);
                }
            } else if (eventType.equals(EventType.ALTER)) {
                Tuple2<String, String> dorisTableTuple = getDorisTableTuple(recordRoot);
                if (dorisTableTuple == null) {
                    LOG.warn("Failed to get doris table tuple. record={}", recordRoot);
                    return false;
                }
                List<String> ddlSqlList = extractDDLList(recordRoot);
                status = executeAlterDDLs(ddlSqlList, recordRoot, dorisTableTuple, status);
            }
        } catch (Exception ex) {
            LOG.warn("schema change error : ", ex);
        }
        return status;
    }

    /** Parse Alter Event. */
    @VisibleForTesting
    public List<String> extractDDLList(JsonNode record) throws IOException {
        String dorisTable =
                JsonDebeziumChangeUtils.getDorisTableIdentifier(record, dorisOptions, tableMapping);
        JsonNode historyRecord = extractHistoryRecord(record);
        String ddl = extractJsonNode(historyRecord, "ddl");
        extractSourceConnector(record);
        return parserDebeziumStructure(dorisTable, ddl, record);
    }

    private List<String> parserDebeziumStructure(String dorisTable, String ddl, JsonNode record)
            throws JsonProcessingException {
        JsonNode tableChange = extractTableChange(record);
        if (Objects.isNull(tableChange) || Objects.isNull(ddl)) {
            LOG.warn(
                    "tableChange or ddl is empty, cannot do schema change. dorisTable={}, tableChange={}, ddl={}",
                    dorisTable,
                    tableChange,
                    ddl);
            return null;
        }

        JsonNode columns = tableChange.get("table").get("columns");
        if (Objects.isNull(sourceConnector)) {
            sourceConnector =
                    SourceConnector.valueOf(
                            record.get("source").get("connector").asText().toUpperCase());
        }
        if (!filledTables.contains(dorisTable)) {
            fillOriginSchema(dorisTable, columns);
            filledTables.add(dorisTable);
        }

        Map<String, FieldSchema> fieldSchemaMap = originFieldSchemaMap.get(dorisTable);
        // remove backtick
        ddl = ddl.replace("`", "");
        // rename ddl
        Matcher renameDdlMatcher = renameDDLPattern.matcher(ddl);
        if (renameDdlMatcher.find()) {
            String oldColumnName = renameDdlMatcher.group(2);
            String newColumnName = renameDdlMatcher.group(3);
            // Change operation
            if (oldColumnName == null) {
                oldColumnName = renameDdlMatcher.group(4);
                newColumnName = renameDdlMatcher.group(5);
            }
            return SchemaChangeHelper.generateRenameDDLSql(
                    dorisTable, oldColumnName, newColumnName, fieldSchemaMap);
        }
        // add/drop ddl
        Map<String, FieldSchema> updateFiledSchema = new LinkedHashMap<>();
        for (JsonNode column : columns) {
            buildFieldSchema(updateFiledSchema, column);
        }
        SchemaChangeHelper.compareSchema(updateFiledSchema, fieldSchemaMap);
        // In order to avoid other source table column change operations other than add/drop/rename,
        // which may lead to the accidental deletion of the doris column.
        Matcher matcher = addDropDDLPattern.matcher(ddl);
        if (!matcher.find()) {
            return null;
        }
        return SchemaChangeHelper.generateDDLSql(dorisTable);
    }

    @VisibleForTesting
    public TableSchema extractCreateTableSchema(JsonNode record) throws JsonProcessingException {
        if (sourceConnector == null) {
            sourceConnector =
                    SourceConnector.valueOf(
                            record.get("source").get("connector").asText().toUpperCase());
        }

        String dorisTable = getCreateTableIdentifier(record);
        JsonNode tableChange = extractTableChange(record);
        JsonNode pkColumns = tableChange.get("table").get("primaryKeyColumnNames");
        JsonNode columns = tableChange.get("table").get("columns");
        JsonNode comment = tableChange.get("table").get("comment");
        String tblComment = comment == null ? "" : comment.asText();
        Map<String, FieldSchema> fields = new LinkedHashMap<>();
        for (JsonNode column : columns) {
            buildFieldSchema(fields, column);
        }
        List<String> pkList = new ArrayList<>();
        for (JsonNode column : pkColumns) {
            String fieldName = column.asText();
            pkList.add(fieldName);
        }
        String[] dbTable = dorisTable.split("\\.");
        Preconditions.checkArgument(dbTable.length == 2);

        return DorisSchemaFactory.createTableSchema(
                dbTable[0], dbTable[1], fields, pkList, dorisTableConfig, tblComment);
    }

    private boolean checkSchemaChange(String database, String table, DDLSchema ddlSchema)
            throws IOException, IllegalArgumentException {
        Map<String, Object> param =
                SchemaChangeManager.buildRequestParam(
                        ddlSchema.isDropColumn(), ddlSchema.getColumnName());
        return schemaChangeManager.checkSchemaChange(database, table, param);
    }

    private Map<String, Object> extractBeforeRow(JsonNode record) {
        return extractRow(record.get("before"));
    }

    private Map<String, Object> extractAfterRow(JsonNode record) {
        return extractRow(record.get("after"));
    }

    private Map<String, Object> extractRow(JsonNode recordRow) {
        Map<String, Object> recordMap =
                objectMapper.convertValue(recordRow, new TypeReference<Map<String, Object>>() {});
        return recordMap != null ? recordMap : new HashMap<>();
    }

    @VisibleForTesting
    public void fillOriginSchema(String tableName, JsonNode columns) {
        Map<String, FieldSchema> fieldSchemaMap = originFieldSchemaMap.get(tableName);
        if (Objects.nonNull(fieldSchemaMap)) {
            for (JsonNode column : columns) {
                String fieldName = column.get("name").asText();
                if (fieldSchemaMap.containsKey(fieldName)) {
                    String dorisTypeName = buildDorisTypeName(column);
                    String defaultValue =
                            handleDefaultValue(extractJsonNode(column, "defaultValueExpression"));
                    String comment = extractJsonNode(column, "comment");
                    FieldSchema fieldSchema = fieldSchemaMap.get(fieldName);
                    fieldSchema.setName(fieldName);
                    fieldSchema.setTypeString(dorisTypeName);
                    fieldSchema.setComment(comment);
                    fieldSchema.setDefaultValue(defaultValue);
                }
            }
        } else {
            // In order to be compatible with column changes, the data is empty or started from
            // flink checkpoint, resulting in the originFieldSchemaMap not being filled.
            LOG.info(tableName + " fill origin field schema from doris schema.");
            fieldSchemaMap = new LinkedHashMap<>();
            String[] splitTableName = tableName.split("\\.");
            Schema schema =
                    RestService.getSchema(dorisOptions, splitTableName[0], splitTableName[1], LOG);
            List<Field> columnFields = schema.getProperties();
            for (Field column : columnFields) {
                String columnName = column.getName();
                String columnType = column.getType();
                String columnComment = column.getComment();
                // TODO need to fill column with default value
                fieldSchemaMap.put(
                        columnName, new FieldSchema(columnName, columnType, columnComment));
            }
            originFieldSchemaMap.put(tableName, fieldSchemaMap);
        }
    }

    @VisibleForTesting
    public void buildFieldSchema(Map<String, FieldSchema> filedSchemaMap, JsonNode column) {
        String fieldName = column.get("name").asText();
        String dorisTypeName = buildDorisTypeName(column);
        String defaultValue = handleDefaultValue(extractJsonNode(column, "defaultValueExpression"));
        String comment = extractJsonNode(column, "comment");
        filedSchemaMap.put(
                fieldName, new FieldSchema(fieldName, dorisTypeName, defaultValue, comment));
    }

    @VisibleForTesting
    public String buildDorisTypeName(JsonNode column) {
        int length = column.get("length") == null ? 0 : column.get("length").asInt();
        int scale = column.get("scale") == null ? 0 : column.get("scale").asInt();
        String sourceTypeName = column.get("typeName").asText();
        return JsonDebeziumChangeUtils.buildDorisTypeName(
                sourceConnector, sourceTypeName, length, scale);
    }

    private String handleDefaultValue(String defaultValue) {
        if (defaultValue == null) {
            return null;
        }
        if (defaultValue.equals("1970-01-01 00:00:00")) {
            // TODO: The default value of setting the current time in CDC is 1970-01-01 00:00:00
            return "current_timestamp";
        }
        return defaultValue;
    }

    @VisibleForTesting
    public void setOriginFieldSchemaMap(
            Map<String, Map<String, FieldSchema>> originFieldSchemaMap) {
        this.originFieldSchemaMap = originFieldSchemaMap;
    }

    @VisibleForTesting
    public Map<String, Map<String, FieldSchema>> getOriginFieldSchemaMap() {
        return originFieldSchemaMap;
    }

    @VisibleForTesting
    public void setSourceConnector(String sourceConnector) {
        this.sourceConnector = SourceConnector.valueOf(sourceConnector.toUpperCase());
    }
}
