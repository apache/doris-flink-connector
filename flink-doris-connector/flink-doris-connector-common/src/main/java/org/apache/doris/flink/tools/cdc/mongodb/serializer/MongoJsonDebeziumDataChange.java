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

import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.ChangeEvent;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.CdcDataChange;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeContext;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.doris.flink.sink.util.DeleteOperation.addDeleteSign;
import static org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils.extractJsonNode;
import static org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils.getDorisTableIdentifier;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.FIELD_DATA;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.FIELD_DATABASE;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.FIELD_DOCUMENT_KEY;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.FIELD_NAMESPACE;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.FIELD_TABLE;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.ID_FIELD;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.OID_FIELD;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.OP_DELETE;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.OP_INSERT;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.OP_REPLACE;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.OP_UPDATE;

public class MongoJsonDebeziumDataChange extends CdcDataChange implements ChangeEvent {
    private static final Logger LOG = LoggerFactory.getLogger(MongoJsonDebeziumDataChange.class);

    public DorisOptions dorisOptions;
    public String lineDelimiter;
    public JsonDebeziumChangeContext changeContext;
    public ObjectMapper objectMapper;
    public Map<String, String> tableMapping;
    private final boolean enableDelete;

    public MongoJsonDebeziumDataChange(JsonDebeziumChangeContext changeContext) {
        this.changeContext = changeContext;
        this.dorisOptions = changeContext.getDorisOptions();
        this.objectMapper = changeContext.getObjectMapper();
        this.lineDelimiter = changeContext.getLineDelimiter();
        this.tableMapping = changeContext.getTableMapping();
        this.enableDelete = changeContext.enableDelete();
    }

    @Override
    public DorisRecord serialize(String record, JsonNode recordRoot, String op) throws IOException {
        // Filter out table records that are not in tableMapping
        String cdcTableIdentifier = getCdcTableIdentifier(recordRoot);
        String dorisTableIdentifier =
                getDorisTableIdentifier(cdcTableIdentifier, dorisOptions, tableMapping);
        if (StringUtils.isNullOrWhitespaceOnly(dorisTableIdentifier)) {
            LOG.warn(
                    "filter table {}, because it is not listened, record detail is {}",
                    cdcTableIdentifier,
                    record);
            return null;
        }
        Map<String, Object> valueMap;
        switch (op) {
            case OP_INSERT:
            case OP_UPDATE:
            case OP_REPLACE:
                valueMap = extractAfterRow(recordRoot);
                addDeleteSign(valueMap, false);
                break;
            case OP_DELETE:
                valueMap = extractDeleteRow(recordRoot);
                addDeleteSign(valueMap, enableDelete);
                break;
            default:
                LOG.error("parse record fail, unknown op {} in {}", op, record);
                return null;
        }

        return DorisRecord.of(
                dorisTableIdentifier,
                objectMapper.writeValueAsString(valueMap).getBytes(StandardCharsets.UTF_8));
    }

    public String getCdcTableIdentifier(JsonNode record) {
        if (record.get(FIELD_NAMESPACE) == null
                || record.get(FIELD_NAMESPACE) instanceof NullNode) {
            LOG.error("Failed to get cdc namespace");
            throw new RuntimeException();
        }
        JsonNode nameSpace = record.get(FIELD_NAMESPACE);
        String db = extractJsonNode(nameSpace, FIELD_DATABASE);
        String table = extractJsonNode(nameSpace, FIELD_TABLE);
        return SourceSchema.getString(db, null, table);
    }

    @Override
    public Map<String, Object> extractBeforeRow(JsonNode record) {
        return null;
    }

    @Override
    public Map<String, Object> extractAfterRow(JsonNode recordRoot) {
        JsonNode dataNode = recordRoot.get(FIELD_DATA);
        Map<String, Object> rowMap = extractRow(dataNode);
        String objectId;
        // if user specifies the `_id` field manually, the $oid field may not exist
        if (rowMap.get(ID_FIELD) instanceof Map<?, ?>) {
            objectId = ((Map<?, ?>) rowMap.get(ID_FIELD)).get(OID_FIELD).toString();
        } else {
            objectId = rowMap.get(ID_FIELD).toString();
        }
        rowMap.put(ID_FIELD, objectId);
        return rowMap;
    }

    private Map<String, Object> extractDeleteRow(JsonNode recordRoot)
            throws JsonProcessingException {
        String documentKey = extractJsonNode(recordRoot, FIELD_DOCUMENT_KEY);
        JsonNode jsonNode = objectMapper.readTree(documentKey);
        String objectId;
        // if user specifies the `_id` field manually, the $oid field may not exist
        if (jsonNode.get(ID_FIELD).has(OID_FIELD)) {
            objectId = extractJsonNode(jsonNode.get(ID_FIELD), OID_FIELD);
        } else {
            objectId = jsonNode.get(ID_FIELD).asText();
        }
        Map<String, Object> row = new HashMap<>();
        row.put(ID_FIELD, objectId);
        return row;
    }

    private Map<String, Object> extractRow(JsonNode recordRow) {
        Map<String, Object> recordMap =
                objectMapper.convertValue(recordRow, new TypeReference<Map<String, Object>>() {});
        return recordMap != null ? recordMap : new HashMap<>();
    }
}
