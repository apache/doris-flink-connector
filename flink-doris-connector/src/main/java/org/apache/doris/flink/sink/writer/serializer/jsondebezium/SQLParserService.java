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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.flink.sink.schema.SQLParserManager;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.sink.writer.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class SQLParserService extends JsonDebeziumSchemaChange {
    private static final Logger LOG = LoggerFactory.getLogger(SQLParserService.class);
    private final SQLParserManager sqlParserManager;

    public SQLParserService(JsonDebeziumChangeContext changeContext) {
        this.changeContext = changeContext;
        this.dorisOptions = changeContext.getDorisOptions();
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        this.sqlParserManager = new SQLParserManager();
        this.tableMapping = changeContext.getTableMapping();
        this.objectMapper = changeContext.getObjectMapper();
    }

    @Override
    public void init(JsonNode recordRoot, String dorisTableName) {
        // do nothing
    }

    @Override
    public boolean schemaChange(JsonNode recordRoot) {
        boolean status = false;
        try {
            if (!StringUtils.isNullOrWhitespaceOnly(sourceTableName) && !checkTable(recordRoot)) {
                return false;
            }

            EventType eventType = extractEventType(recordRoot);
            if (eventType == null) {
                LOG.warn("Failed to parse eventType. recordRoot={}", recordRoot);
                return false;
            }

            if (eventType.equals(EventType.CREATE)) {
                // TODO support auto create table
                LOG.warn("Not auto support create table. recordRoot={}", recordRoot);
            } else if (eventType.equals(EventType.ALTER)) {
                Tuple2<String, String> dorisTableTuple = getDorisTableTuple(recordRoot);
                if (dorisTableTuple == null) {
                    LOG.warn("Failed to get doris table tuple. record={}", recordRoot);
                    return false;
                }
                List<String> ddlList = tryParserAlterDDLs(recordRoot);
                status = executeAlterDDLs(ddlList, recordRoot, dorisTableTuple, status);
            }
        } catch (Exception ex) {
            LOG.warn("schema change error : ", ex);
        }
        return status;
    }

    @VisibleForTesting
    public List<String> tryParserAlterDDLs(JsonNode record) throws IOException {
        String dorisTable =
                JsonDebeziumChangeUtils.getDorisTableIdentifier(record, dorisOptions, tableMapping);
        JsonNode historyRecord = extractHistoryRecord(record);
        String ddl = extractJsonNode(historyRecord, "ddl");
        extractSourceConnector(record);
        return sqlParserManager.parserAlterDDLs(sourceConnector, ddl, dorisTable);
    }
}
