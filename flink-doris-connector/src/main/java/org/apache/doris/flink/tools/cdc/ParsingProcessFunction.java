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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class ParsingProcessFunction extends ProcessFunction<String, Void> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private transient Map<String, OutputTag<String>> recordOutputTags;
    private DatabaseSync.TableNameConverter converter;

    public ParsingProcessFunction(DatabaseSync.TableNameConverter converter) {
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
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        String tableName = extractJsonNode(recordRoot.get("source"), "table");
        String dorisName = converter.convert(tableName);
        context.output(getRecordOutputTag(dorisName), record);
    }

    private String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null ? record.get(key).asText() : null;
    }

    private OutputTag<String> getRecordOutputTag(String tableName) {
        return recordOutputTags.computeIfAbsent(
                tableName, ParsingProcessFunction::createRecordOutputTag);
    }

    public static OutputTag<String> createRecordOutputTag(String tableName) {
        return new OutputTag<String>("record-" + tableName) {};
    }
}
