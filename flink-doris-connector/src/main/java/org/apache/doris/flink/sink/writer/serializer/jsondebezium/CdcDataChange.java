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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.ChangeEvent;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class CdcDataChange implements ChangeEvent {

    public DorisOptions dorisOptions;
    public String lineDelimiter;
    public JsonDebeziumChangeContext changeContext;
    public ObjectMapper objectMapper;

    public abstract DorisRecord serialize(String record, JsonNode recordRoot, String op)
            throws IOException;

    public abstract String getCdcTableIdentifier(JsonNode record);

    public abstract Map<String, Object> extractBeforeRow(JsonNode record);

    public abstract Map<String, Object> extractAfterRow(JsonNode record);

    public String getDorisTableIdentifier(String cdcTableIdentifier) {
        if (!StringUtils.isNullOrWhitespaceOnly(dorisOptions.getTableIdentifier())) {
            return dorisOptions.getTableIdentifier();
        }
        Map<String, String> tableMapping = changeContext.getTableMapping();
        if (!CollectionUtil.isNullOrEmpty(tableMapping)
                && !StringUtils.isNullOrWhitespaceOnly(cdcTableIdentifier)
                && tableMapping.get(cdcTableIdentifier) != null) {
            return tableMapping.get(cdcTableIdentifier);
        }
        return null;
    }

    public String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null && !(record.get(key) instanceof NullNode)
                ? record.get(key).asText()
                : null;
    }

    public Map<String, Object> extractRow(JsonNode recordRow) {
        Map<String, Object> recordMap =
                objectMapper.convertValue(recordRow, new TypeReference<Map<String, Object>>() {});
        return recordMap != null ? recordMap : new HashMap<>();
    }
}
