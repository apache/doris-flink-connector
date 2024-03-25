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

package org.apache.doris.flink.sink.writer.serializer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.CdcDataChange;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.CdcSchemaChange;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeContext;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.MongoJsonDebeziumDataChange;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.MongoJsonDebeziumSchemaChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

public class MongoDBJsonDebeziumSchemaSerializer implements DorisRecordSerializer<String> {

    private static final Logger LOG =
            LoggerFactory.getLogger(MongoDBJsonDebeziumSchemaSerializer.class);
    private final Pattern pattern;
    private final DorisOptions dorisOptions;
    private final ObjectMapper objectMapper = new ObjectMapper();
    // table name of the cdc upstream, format is db.tbl
    private final String sourceTableName;
    private String lineDelimiter = LINE_DELIMITER_DEFAULT;
    private boolean ignoreUpdateBefore = true;
    // <cdc db.schema.table, doris db.table>
    private Map<String, String> tableMapping;
    // create table properties
    private Map<String, String> tableProperties;
    private String targetDatabase;

    private CdcDataChange dataChange;
    private CdcSchemaChange schemaChange;

    private String targetTablePrefix;
    private String targetTableSuffix;

    public MongoDBJsonDebeziumSchemaSerializer(
            DorisOptions dorisOptions,
            Pattern pattern,
            String sourceTableName,
            DorisExecutionOptions executionOptions,
            Map<String, String> tableMapping,
            Map<String, String> tableProperties,
            String targetDatabase,
            String targetTablePrefix,
            String targetTableSuffix) {
        this.dorisOptions = dorisOptions;
        this.pattern = pattern;
        this.sourceTableName = sourceTableName;
        // Prevent loss of decimal data precision
        this.objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        this.objectMapper.setNodeFactory(jsonNodeFactory);
        this.tableMapping = tableMapping;
        this.tableProperties = tableProperties;
        this.targetDatabase = targetDatabase;
        this.targetTablePrefix = targetTablePrefix;
        this.targetTableSuffix = targetTableSuffix;
        if (executionOptions != null) {
            this.lineDelimiter =
                    executionOptions
                            .getStreamLoadProp()
                            .getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT);
            this.ignoreUpdateBefore = executionOptions.getIgnoreUpdateBefore();
        }
        init();
    }

    private void init() {
        JsonDebeziumChangeContext changeContext =
                new JsonDebeziumChangeContext(
                        dorisOptions,
                        tableMapping,
                        sourceTableName,
                        targetDatabase,
                        tableProperties,
                        objectMapper,
                        pattern,
                        lineDelimiter,
                        ignoreUpdateBefore,
                        targetTablePrefix,
                        targetTableSuffix);
        this.dataChange = new MongoJsonDebeziumDataChange(changeContext);
        this.schemaChange = new MongoJsonDebeziumSchemaChange(changeContext);
    }

    @Override
    public DorisRecord serialize(String record) throws IOException {
        LOG.debug("received debezium json data {} :", record);
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        String op = getOperateType(recordRoot);
        try {
            schemaChange.schemaChange(recordRoot);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return dataChange.serialize(record, recordRoot, op);
    }

    private String getOperateType(JsonNode recordRoot) {
        return recordRoot.get("operationType").asText();
    }

    public static MongoDBJsonDebeziumSchemaSerializer.Builder builder() {
        return new MongoDBJsonDebeziumSchemaSerializer.Builder();
    }

    public static class Builder {
        private DorisOptions dorisOptions;
        private Pattern addDropDDLPattern;
        private String sourceTableName;
        private DorisExecutionOptions executionOptions;
        private Map<String, String> tableMapping;
        private Map<String, String> tableProperties;
        private String targetDatabase;
        private String targetTablePrefix = "";
        private String targetTableSuffix = "";

        public MongoDBJsonDebeziumSchemaSerializer.Builder setDorisOptions(
                DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public MongoDBJsonDebeziumSchemaSerializer.Builder setPattern(Pattern addDropDDLPattern) {
            this.addDropDDLPattern = addDropDDLPattern;
            return this;
        }

        public MongoDBJsonDebeziumSchemaSerializer.Builder setSourceTableName(
                String sourceTableName) {
            this.sourceTableName = sourceTableName;
            return this;
        }

        public MongoDBJsonDebeziumSchemaSerializer.Builder setExecutionOptions(
                DorisExecutionOptions executionOptions) {
            this.executionOptions = executionOptions;
            return this;
        }

        public MongoDBJsonDebeziumSchemaSerializer.Builder setTableMapping(
                Map<String, String> tableMapping) {
            this.tableMapping = tableMapping;
            return this;
        }

        public MongoDBJsonDebeziumSchemaSerializer.Builder setTableProperties(
                Map<String, String> tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public MongoDBJsonDebeziumSchemaSerializer.Builder setTargetDatabase(
                String targetDatabase) {
            this.targetDatabase = targetDatabase;
            return this;
        }

        public MongoDBJsonDebeziumSchemaSerializer build() {
            return new MongoDBJsonDebeziumSchemaSerializer(
                    dorisOptions,
                    addDropDDLPattern,
                    sourceTableName,
                    executionOptions,
                    tableMapping,
                    tableProperties,
                    targetDatabase,
                    targetTablePrefix,
                    targetTableSuffix);
        }
    }
}
