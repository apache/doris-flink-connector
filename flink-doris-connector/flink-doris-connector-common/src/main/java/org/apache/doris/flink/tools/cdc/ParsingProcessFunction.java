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

package org.apache.doris.flink.tools.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class ParsingProcessFunction extends ProcessFunction<String, Void> {
    protected ObjectMapper objectMapper = new ObjectMapper();
    private transient Map<String, OutputTag<String>> recordOutputTags;
    private DatabaseSync.TableNameConverter converter;
    private String database;

    public ParsingProcessFunction(String database, DatabaseSync.TableNameConverter converter) {
        this.database = database;
        this.converter = converter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        recordOutputTags = new HashMap<>();
    }

    @Override
    public void processElement(
            String record, ProcessFunction<String, Void>.Context context, Collector<Void> collector)
            throws Exception {
        String tableName = getRecordTableName(record);
        String dorisTableName = converter.convert(tableName);
        String dorisDbName = database;
        if (StringUtils.isNullOrWhitespaceOnly(database)) {
            dorisDbName = getRecordDatabaseName(record);
        }
        context.output(getRecordOutputTag(dorisDbName, dorisTableName), record);
    }

    private String getRecordDatabaseName(String record) throws JsonProcessingException {
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        return extractJsonNode(recordRoot.get("source"), "db");
    }

    protected String getRecordTableName(String record) throws Exception {
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        return extractJsonNode(recordRoot.get("source"), "table");
    }

    protected String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null ? record.get(key).asText() : null;
    }

    private OutputTag<String> getRecordOutputTag(String databaseName, String tableName) {
        String tableIdentifier = databaseName + "." + tableName;
        return recordOutputTags.computeIfAbsent(
                tableIdentifier, k -> createRecordOutputTag(databaseName, tableName));
    }

    public static OutputTag<String> createRecordOutputTag(String databaseName, String tableName) {
        return new OutputTag<String>(String.format("record-%s-%s", databaseName, tableName)) {};
    }
}
