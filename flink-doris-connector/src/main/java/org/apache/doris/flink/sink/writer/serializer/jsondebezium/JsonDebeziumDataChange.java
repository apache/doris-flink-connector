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

import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.doris.flink.sink.util.DeleteOperation.addDeleteSign;

/**
 * Convert the data change record of the upstream data source into a byte array that can be imported
 * into doris through stream load.<br>
 * Supported data changes include: read, insert, update, delete.
 */
public class JsonDebeziumDataChange extends CdcDataChange {
    private static final Logger LOG = LoggerFactory.getLogger(JsonDebeziumDataChange.class);

    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete
    private final ObjectMapper objectMapper;
    private final DorisOptions dorisOptions;
    private final boolean ignoreUpdateBefore;
    private final boolean enableDelete;
    private final String lineDelimiter;
    private final JsonDebeziumChangeContext changeContext;

    public JsonDebeziumDataChange(JsonDebeziumChangeContext changeContext) {
        this.changeContext = changeContext;
        this.dorisOptions = changeContext.getDorisOptions();
        this.objectMapper = changeContext.getObjectMapper();
        this.ignoreUpdateBefore = changeContext.isIgnoreUpdateBefore();
        this.lineDelimiter = changeContext.getLineDelimiter();
        this.enableDelete = changeContext.enableDelete();
    }

    public DorisRecord serialize(String record, JsonNode recordRoot, String op) throws IOException {
        // Filter out table records that are not in tableMapping
        Map<String, String> tableMapping = changeContext.getTableMapping();
        String cdcTableIdentifier = JsonDebeziumChangeUtils.getCdcTableIdentifier(recordRoot);
        String dorisTableIdentifier =
                JsonDebeziumChangeUtils.getDorisTableIdentifier(
                        cdcTableIdentifier, dorisOptions, tableMapping);
        if (StringUtils.isNullOrWhitespaceOnly(dorisTableIdentifier)) {
            LOG.warn(
                    "filter table {}, because it is not listened, record detail is {}",
                    cdcTableIdentifier,
                    record);
            return null;
        }

        Map<String, Object> valueMap;
        switch (op) {
            case OP_READ:
            case OP_CREATE:
                valueMap = extractAfterRow(recordRoot);
                addDeleteSign(valueMap, false);
                break;
            case OP_UPDATE:
                return DorisRecord.of(dorisTableIdentifier, extractUpdate(recordRoot));
            case OP_DELETE:
                valueMap = extractBeforeRow(recordRoot);
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

    /**
     * Change the update event into two.
     *
     * @param recordRoot
     * @return
     */
    private byte[] extractUpdate(JsonNode recordRoot) throws JsonProcessingException {
        StringBuilder updateRow = new StringBuilder();
        if (!ignoreUpdateBefore) {
            // convert delete
            Map<String, Object> beforeRow = extractBeforeRow(recordRoot);
            addDeleteSign(beforeRow, true);
            updateRow.append(objectMapper.writeValueAsString(beforeRow)).append(this.lineDelimiter);
        }

        // convert insert
        Map<String, Object> afterRow = extractAfterRow(recordRoot);
        addDeleteSign(afterRow, false);
        updateRow.append(objectMapper.writeValueAsString(afterRow));
        return updateRow.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected Map<String, Object> extractBeforeRow(JsonNode record) {
        return extractRow(record.get("before"));
    }

    @Override
    protected Map<String, Object> extractAfterRow(JsonNode record) {
        return extractRow(record.get("after"));
    }

    private Map<String, Object> extractRow(JsonNode recordRow) {
        Map<String, Object> recordMap =
                objectMapper.convertValue(recordRow, new TypeReference<Map<String, Object>>() {});
        return recordMap != null ? recordMap : new HashMap<>();
    }
}
