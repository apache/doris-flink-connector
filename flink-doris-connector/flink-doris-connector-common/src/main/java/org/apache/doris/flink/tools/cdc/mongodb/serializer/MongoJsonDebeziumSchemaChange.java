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

package org.apache.doris.flink.tools.cdc.mongodb.serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.CdcSchemaChange;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeContext;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.doris.flink.tools.cdc.mongodb.MongoDBType;
import org.apache.doris.flink.tools.cdc.mongodb.MongoDateConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils.getDorisTableIdentifier;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.DATE_FIELD;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.DECIMAL_FIELD;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.FIELD_DATA;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.FIELD_DATABASE;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.FIELD_NAMESPACE;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.FIELD_TABLE;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.LONG_FIELD;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.TIMESTAMP_FIELD;

public class MongoJsonDebeziumSchemaChange extends CdcSchemaChange {

    private static final Logger LOG = LoggerFactory.getLogger(MongoJsonDebeziumSchemaChange.class);

    private final ObjectMapper objectMapper;

    private final Map<String, Map<String, String>> tableFields;

    private final SchemaChangeManager schemaChangeManager;

    private final DorisSystem dorisSystem;

    public Map<String, String> tableMapping;
    private final DorisOptions dorisOptions;

    private final Set<String> specialFields =
            new HashSet<>(Arrays.asList(DATE_FIELD, TIMESTAMP_FIELD, DECIMAL_FIELD, LONG_FIELD));

    public MongoJsonDebeziumSchemaChange(JsonDebeziumChangeContext changeContext) {
        this.objectMapper = changeContext.getObjectMapper();
        this.dorisOptions = changeContext.getDorisOptions();
        this.tableFields = new HashMap<>();
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        this.dorisSystem = new DorisSystem(dorisOptions);
        this.tableMapping = changeContext.getTableMapping();
    }

    @Override
    public String extractDatabase(JsonNode record) {
        return null;
    }

    @Override
    public String extractTable(JsonNode record) {
        return null;
    }

    @Override
    public boolean schemaChange(JsonNode recordRoot) {
        try {
            JsonNode logData = getFullDocument(recordRoot);
            String cdcTableIdentifier = getCdcTableIdentifier(recordRoot);
            String dorisTableIdentifier =
                    getDorisTableIdentifier(cdcTableIdentifier, dorisOptions, tableMapping);
            String[] tableInfo = dorisTableIdentifier.split("\\.");
            if (tableInfo.length != 2) {
                throw new DorisRuntimeException();
            }
            String dataBase = tableInfo[0];
            String table = tableInfo[1];
            // build table fields mapping for all record
            buildDorisTableFieldsMapping(dataBase, table);

            // Determine whether change stream log and tableField are exactly the same, if not,
            // perform
            // schema change
            checkAndUpdateSchemaChange(logData, dorisTableIdentifier, dataBase, table);
            formatSpecialFieldData(logData);
            ((ObjectNode) recordRoot).set(FIELD_DATA, logData);
            return true;
        } catch (Exception ex) {
            LOG.warn("schema change error : ", ex);
            return false;
        }
    }

    private void formatSpecialFieldData(JsonNode logData) {
        logData.fieldNames()
                .forEachRemaining(
                        fieldName -> {
                            JsonNode fieldNode = logData.get(fieldName);
                            if (fieldNode.isObject() && fieldNode.size() == 1) {
                                String fieldKey = fieldNode.fieldNames().next();
                                if (specialFields.contains(fieldKey)) {
                                    switch (fieldKey) {
                                        case DATE_FIELD:
                                        case TIMESTAMP_FIELD:
                                            JsonNode jsonNode = fieldNode.get(fieldKey);
                                            long timestamp =
                                                    fieldKey.equals(TIMESTAMP_FIELD)
                                                            ? jsonNode.get("t").asLong() * 1000L
                                                            : jsonNode.asLong();
                                            String formattedDate =
                                                    MongoDateConverter.convertTimestampToString(
                                                            timestamp);
                                            ((ObjectNode) logData).put(fieldName, formattedDate);
                                            break;
                                        case DECIMAL_FIELD:
                                            String numberDecimal =
                                                    fieldNode.get(DECIMAL_FIELD).asText();
                                            ((ObjectNode) logData).put(fieldName, numberDecimal);
                                            break;

                                        case LONG_FIELD:
                                            long longFiled = fieldNode.get(LONG_FIELD).asLong();
                                            ((ObjectNode) logData).put(fieldName, longFiled);
                                            break;
                                    }
                                }
                            }
                        });
    }

    private JsonNode getFullDocument(JsonNode recordRoot) {
        try {
            return objectMapper.readTree(recordRoot.get(FIELD_DATA).asText());
        } catch (IOException e) {
            throw new DorisRuntimeException("Failed to parse fullDocument JSON", e);
        }
    }

    private void checkAndUpdateSchemaChange(
            JsonNode logData, String dorisTableIdentifier, String database, String table) {
        Map<String, String> tableFieldMap = tableFields.get(dorisTableIdentifier);
        logData.fieldNames()
                .forEachRemaining(
                        name -> {
                            try {
                                if (!tableFieldMap.containsKey(name)) {
                                    doSchemaChange(name, logData, database, table);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException("Error during schema change", e);
                            }
                        });
    }

    private void doSchemaChange(
            String logFieldName, JsonNode logData, String database, String table)
            throws IOException, IllegalArgumentException {
        String dorisType = MongoDBType.jsonNodeToDorisType(logData.get(logFieldName));
        schemaChangeManager.addColumn(
                database, table, new FieldSchema(logFieldName, dorisType, null));
        String identifier = database + "." + table;
        tableFields.computeIfAbsent(identifier, k -> new HashMap<>()).put(logFieldName, dorisType);
    }

    private void buildDorisTableFieldsMapping(String databaseName, String tableName) {
        String identifier = databaseName + "." + tableName;
        tableFields.computeIfAbsent(
                identifier, k -> dorisSystem.getTableFieldNames(databaseName, tableName));
    }

    @Override
    public String getCdcTableIdentifier(JsonNode record) {
        if (record.get(FIELD_NAMESPACE) == null
                || record.get(FIELD_NAMESPACE) instanceof NullNode) {
            LOG.error("Failed to get cdc namespace");
            throw new RuntimeException();
        }
        JsonNode nameSpace = record.get(FIELD_NAMESPACE);
        String table = nameSpace.get(FIELD_TABLE).asText();
        String db = nameSpace.get(FIELD_DATABASE).asText();
        return SourceSchema.getString(db, null, table);
    }
}
