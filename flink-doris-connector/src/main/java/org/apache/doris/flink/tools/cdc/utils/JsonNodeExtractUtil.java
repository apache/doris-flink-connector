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

package org.apache.doris.flink.tools.cdc.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.ID_FIELD;
import static org.apache.doris.flink.tools.cdc.mongodb.ChangeStreamConstant.OID_FIELD;

/** Utility class for extracting data from JSON nodes */
public class JsonNodeExtractUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JsonNodeExtractUtil.class);

    /**
     * Extract row data from JsonNode and convert to Map
     *
     * @param recordRow JsonNode containing row data
     * @param objectMapper ObjectMapper instance for JSON conversion
     * @return Map containing extracted row data
     */
    public static Map<String, Object> extractRow(JsonNode recordRow, ObjectMapper objectMapper) {
        Map<String, Object> recordMap =
                objectMapper.convertValue(recordRow, new TypeReference<Map<String, Object>>() {});
        return recordMap != null ? recordMap : new HashMap<>();
    }

    public static Map<String, Object> extractAfterRow(
            JsonNode recordRoot, ObjectMapper objectMapper) {
        Map<String, Object> rowMap = JsonNodeExtractUtil.extractRow(recordRoot, objectMapper);
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
}
