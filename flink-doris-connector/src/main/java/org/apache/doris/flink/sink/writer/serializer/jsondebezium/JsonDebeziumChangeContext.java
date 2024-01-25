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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisOptions;

import java.io.Serializable;
import java.util.Map;
import java.util.regex.Pattern;

/** Record the context of schema change and data change during serialization. */
public class JsonDebeziumChangeContext implements Serializable {
    private final DorisOptions dorisOptions;
    // <cdc db.schema.table, doris db.table>
    private final Map<String, String> tableMapping;
    // table name of the cdc upstream, format is db.tbl
    private final String sourceTableName;
    private final String targetDatabase;
    // create table properties
    private final Map<String, String> tableProperties;
    private final ObjectMapper objectMapper;
    private final Pattern pattern;
    private final String lineDelimiter;
    private final boolean ignoreUpdateBefore;
    private String targetTablePrefix;
    private String targetTableSuffix;

    public JsonDebeziumChangeContext(
            DorisOptions dorisOptions,
            Map<String, String> tableMapping,
            String sourceTableName,
            String targetDatabase,
            Map<String, String> tableProperties,
            ObjectMapper objectMapper,
            Pattern pattern,
            String lineDelimiter,
            boolean ignoreUpdateBefore,
            String targetTablePrefix,
            String targetTableSuffix) {
        this.dorisOptions = dorisOptions;
        this.tableMapping = tableMapping;
        this.sourceTableName = sourceTableName;
        this.targetDatabase = targetDatabase;
        this.tableProperties = tableProperties;
        this.objectMapper = objectMapper;
        this.pattern = pattern;
        this.lineDelimiter = lineDelimiter;
        this.ignoreUpdateBefore = ignoreUpdateBefore;
        this.targetTablePrefix = targetTablePrefix;
        this.targetTableSuffix = targetTableSuffix;
    }

    public DorisOptions getDorisOptions() {
        return dorisOptions;
    }

    public Map<String, String> getTableMapping() {
        return tableMapping;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public Map<String, String> getTableProperties() {
        return tableProperties;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public boolean isIgnoreUpdateBefore() {
        return ignoreUpdateBefore;
    }

    public String getTargetTablePrefix() {
        return targetTablePrefix;
    }

    public String getTargetTableSuffix() {
        return targetTableSuffix;
    }
}
