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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.schema.SchemaChangeMode;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeContext;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumDataChange;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumSchemaChange;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumSchemaChangeImpl;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumSchemaChangeImplV2;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.SQLParserSchemaChange;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

/**
 * Serialize the records of the upstream data source into a data form that can be recognized by
 * downstream doris.
 *
 * <p>There are two serialization methods here: <br>
 * 1. data change{@link JsonDebeziumDataChange} record. <br>
 * 2. schema change{@link JsonDebeziumSchemaChange} records.
 */
@PublicEvolving
public class JsonDebeziumSchemaSerializer implements DorisRecordSerializer<String> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonDebeziumSchemaSerializer.class);
    private final Pattern pattern;
    private final DorisOptions dorisOptions;
    private final ObjectMapper objectMapper = new ObjectMapper();
    // table name of the cdc upstream, format is db.tbl
    private final String sourceTableName;
    private final boolean newSchemaChange;
    private String lineDelimiter = LINE_DELIMITER_DEFAULT;
    private boolean ignoreUpdateBefore = true;
    private boolean enableDelete = true;
    // <cdc db.schema.table, doris db.table>
    private Map<String, String> tableMapping;
    // create table properties
    private DorisTableConfig dorisTableConfig;
    private String targetDatabase;
    private String targetTablePrefix;
    private String targetTableSuffix;
    private JsonDebeziumDataChange dataChange;
    private JsonDebeziumSchemaChange schemaChange;
    private SchemaChangeMode schemaChangeMode;
    private final Set<String> initTableSet = new HashSet<>();

    public JsonDebeziumSchemaSerializer(
            DorisOptions dorisOptions,
            Pattern pattern,
            String sourceTableName,
            boolean newSchemaChange) {
        this.dorisOptions = dorisOptions;
        this.pattern = pattern;
        this.sourceTableName = sourceTableName;
        // Prevent loss of decimal data precision
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        objectMapper.configure(Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        objectMapper.setNodeFactory(jsonNodeFactory);
        this.newSchemaChange = newSchemaChange;
    }

    public JsonDebeziumSchemaSerializer(
            DorisOptions dorisOptions,
            Pattern pattern,
            String sourceTableName,
            boolean newSchemaChange,
            DorisExecutionOptions executionOptions) {
        this(dorisOptions, pattern, sourceTableName, newSchemaChange);
        if (executionOptions != null) {
            this.lineDelimiter =
                    executionOptions
                            .getStreamLoadProp()
                            .getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT);
            this.ignoreUpdateBefore = executionOptions.getIgnoreUpdateBefore();
            this.enableDelete = executionOptions.getDeletable();
        }
    }

    public JsonDebeziumSchemaSerializer(
            DorisOptions dorisOptions,
            Pattern pattern,
            String sourceTableName,
            boolean newSchemaChange,
            DorisExecutionOptions executionOptions,
            Map<String, String> tableMapping,
            DorisTableConfig dorisTableConfig,
            String targetDatabase,
            String targetTablePrefix,
            String targetTableSuffix,
            SchemaChangeMode schemaChangeMode) {
        this(dorisOptions, pattern, sourceTableName, newSchemaChange, executionOptions);
        this.tableMapping = tableMapping;
        this.targetDatabase = targetDatabase;
        this.targetTablePrefix = targetTablePrefix;
        this.targetTableSuffix = targetTableSuffix;
        this.schemaChangeMode = schemaChangeMode;
        this.dorisTableConfig = dorisTableConfig;
        init();
    }

    private void init() {
        JsonDebeziumChangeContext changeContext =
                new JsonDebeziumChangeContext(
                        dorisOptions,
                        tableMapping,
                        sourceTableName,
                        targetDatabase,
                        dorisTableConfig,
                        objectMapper,
                        pattern,
                        lineDelimiter,
                        ignoreUpdateBefore,
                        targetTablePrefix,
                        targetTableSuffix,
                        enableDelete);
        initSchemaChangeInstance(changeContext);
        this.dataChange = new JsonDebeziumDataChange(changeContext);
    }

    private void initSchemaChangeInstance(JsonDebeziumChangeContext changeContext) {
        if (!newSchemaChange) {
            LOG.info(
                    "newSchemaChange set to false, instantiation schema change uses JsonDebeziumSchemaChangeImpl.");
            this.schemaChange = new JsonDebeziumSchemaChangeImpl(changeContext);
            return;
        }

        if (Objects.nonNull(schemaChangeMode)
                && SchemaChangeMode.SQL_PARSER.equals(schemaChangeMode)) {
            LOG.info(
                    "SchemaChangeMode set to SQL_PARSER, instantiation schema change uses SQLParserService.");
            this.schemaChange = new SQLParserSchemaChange(changeContext);
        } else {
            LOG.info("instantiation schema change uses JsonDebeziumSchemaChangeImplV2.");
            this.schemaChange = new JsonDebeziumSchemaChangeImplV2(changeContext);
        }
    }

    @Override
    public DorisRecord serialize(String record) throws IOException {
        LOG.debug("received debezium json data {} :", record);
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        String op = extractJsonNode(recordRoot, "op");
        if (Objects.isNull(op)) {
            // schema change ddl
            schemaChange.schemaChange(recordRoot);
            return null;
        }

        this.tableMapping = schemaChange.getTableMapping();
        String dorisTableName =
                JsonDebeziumChangeUtils.getDorisTableIdentifier(
                        recordRoot, dorisOptions, tableMapping);
        if (initSchemaChange(dorisTableName)) {
            schemaChange.init(recordRoot, dorisTableName);
        }
        return dataChange.serialize(record, recordRoot, op);
    }

    private boolean initSchemaChange(String dorisTableName) {
        if (initTableSet.contains(dorisTableName)) {
            return false;
        }
        initTableSet.add(dorisTableName);
        return true;
    }

    private String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null && !(record.get(key) instanceof NullNode)
                ? record.get(key).asText()
                : null;
    }

    @VisibleForTesting
    public JsonDebeziumSchemaChange getJsonDebeziumSchemaChange() {
        return this.schemaChange;
    }

    public static JsonDebeziumSchemaSerializer.Builder builder() {
        return new JsonDebeziumSchemaSerializer.Builder();
    }

    /** Builder for JsonDebeziumSchemaSerializer. */
    public static class Builder {
        private DorisOptions dorisOptions;
        private Pattern addDropDDLPattern;
        private String sourceTableName;
        private boolean newSchemaChange = true;
        private SchemaChangeMode schemaChangeMode;
        private DorisExecutionOptions executionOptions;
        private Map<String, String> tableMapping;
        private DorisTableConfig dorisTableConfig;
        private String targetDatabase;
        private String targetTablePrefix = "";
        private String targetTableSuffix = "";

        public JsonDebeziumSchemaSerializer.Builder setDorisOptions(DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public JsonDebeziumSchemaSerializer.Builder setNewSchemaChange(boolean newSchemaChange) {
            this.newSchemaChange = newSchemaChange;
            return this;
        }

        public JsonDebeziumSchemaSerializer.Builder setSchemaChangeMode(String schemaChangeMode) {
            if (org.apache.commons.lang3.StringUtils.isEmpty(schemaChangeMode)) {
                return this;
            }
            this.schemaChangeMode = SchemaChangeMode.valueOf(schemaChangeMode.toUpperCase());
            return this;
        }

        public JsonDebeziumSchemaSerializer.Builder setPattern(Pattern addDropDDLPattern) {
            this.addDropDDLPattern = addDropDDLPattern;
            return this;
        }

        public JsonDebeziumSchemaSerializer.Builder setSourceTableName(String sourceTableName) {
            this.sourceTableName = sourceTableName;
            return this;
        }

        public Builder setExecutionOptions(DorisExecutionOptions executionOptions) {
            this.executionOptions = executionOptions;
            return this;
        }

        public Builder setTableMapping(Map<String, String> tableMapping) {
            this.tableMapping = tableMapping;
            return this;
        }

        @Deprecated
        public Builder setTableProperties(Map<String, String> tableProperties) {
            this.dorisTableConfig = new DorisTableConfig(tableProperties);
            return this;
        }

        public Builder setDorisTableConf(DorisTableConfig dorisTableConfig) {
            this.dorisTableConfig = dorisTableConfig;
            return this;
        }

        public Builder setTargetDatabase(String targetDatabase) {
            this.targetDatabase = targetDatabase;
            return this;
        }

        public Builder setTargetTablePrefix(String tablePrefix) {
            if (!StringUtils.isNullOrWhitespaceOnly(tablePrefix)) {
                this.targetTablePrefix = tablePrefix;
            }
            return this;
        }

        public Builder setTargetTableSuffix(String tableSuffix) {
            if (!StringUtils.isNullOrWhitespaceOnly(tableSuffix)) {
                this.targetTableSuffix = tableSuffix;
            }
            return this;
        }

        public JsonDebeziumSchemaSerializer build() {
            return new JsonDebeziumSchemaSerializer(
                    dorisOptions,
                    addDropDDLPattern,
                    sourceTableName,
                    newSchemaChange,
                    executionOptions,
                    tableMapping,
                    dorisTableConfig,
                    targetDatabase,
                    targetTablePrefix,
                    targetTableSuffix,
                    schemaChangeMode);
        }
    }
}
