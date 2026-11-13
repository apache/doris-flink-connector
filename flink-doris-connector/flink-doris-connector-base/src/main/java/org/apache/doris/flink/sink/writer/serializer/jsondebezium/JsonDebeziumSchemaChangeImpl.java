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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Use expression to match ddl sql.
 *
 * <p>The way of parsing DDL statements relies on regular expression matching, and this parsing
 * method has many flaws. In order to solve this problem, we introduced the com.github.jsqlparser
 * framework, which can accurately parse the schema change of DDL.
 *
 * <p>This class is no longer recommended, we recommend using {@link SQLParserSchemaChange}
 */
@Deprecated
public class JsonDebeziumSchemaChangeImpl extends JsonDebeziumSchemaChange {
    private static final Logger LOG = LoggerFactory.getLogger(JsonDebeziumSchemaChangeImpl.class);
    // alter table tbl add cloumn aca int
    public static final String EXECUTE_DDL = "ALTER TABLE %s %s COLUMN %s %s";

    public JsonDebeziumSchemaChangeImpl(JsonDebeziumChangeContext changeContext) {
        this.changeContext = changeContext;
        this.dorisOptions = changeContext.getDorisOptions();
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        this.sourceTableName = changeContext.getSourceTableName();
        this.tableMapping = changeContext.getTableMapping();
        this.objectMapper = changeContext.getObjectMapper();
        this.addDropDDLPattern =
                Objects.isNull(changeContext.getPattern())
                        ? Pattern.compile(addDropDDLRegex, Pattern.CASE_INSENSITIVE)
                        : changeContext.getPattern();
    }

    @Override
    public void init(JsonNode recordRoot, String dorisTableName) {
        // do nothing
    }

    @VisibleForTesting
    @Override
    public boolean schemaChange(JsonNode recordRoot) {
        boolean status = false;
        try {
            if (!StringUtils.isNullOrWhitespaceOnly(sourceTableName) && !checkTable(recordRoot)) {
                return false;
            }
            // db,table
            Tuple2<String, String> tuple = getDorisTableTuple(recordRoot);
            if (tuple == null) {
                return false;
            }

            String ddl = extractDDL(recordRoot);
            if (StringUtils.isNullOrWhitespaceOnly(ddl)) {
                LOG.info("ddl can not do schema change:{}", recordRoot);
                return false;
            }

            boolean doSchemaChange = checkSchemaChange(tuple.f0, tuple.f1, ddl);
            status = doSchemaChange && schemaChangeManager.execute(ddl, tuple.f0);
            LOG.info("schema change status:{}", status);
        } catch (Exception ex) {
            LOG.warn("schema change error :", ex);
        }
        return status;
    }

    private boolean checkSchemaChange(String database, String table, String ddl)
            throws IOException, IllegalArgumentException {
        Map<String, Object> param = buildRequestParam(ddl);
        return schemaChangeManager.checkSchemaChange(database, table, param);
    }

    /** Build param { "isDropColumn": true, "columnName" : "column" }. */
    protected Map<String, Object> buildRequestParam(String ddl) {
        Map<String, Object> params = new HashMap<>();
        Matcher matcher = addDropDDLPattern.matcher(ddl);
        if (matcher.find()) {
            String op = matcher.group(1);
            String col = matcher.group(3);
            params.put("isDropColumn", op.equalsIgnoreCase("DROP"));
            params.put("columnName", col);
        }
        return params;
    }

    @VisibleForTesting
    public String extractDDL(JsonNode record) throws JsonProcessingException {
        JsonNode historyRecord = extractHistoryRecord(record);
        String ddl = extractJsonNode(historyRecord, "ddl");
        LOG.debug("received debezium ddl :{}", ddl);
        if (!Objects.isNull(ddl)) {
            // filter add/drop operation
            Matcher matcher = addDropDDLPattern.matcher(ddl);
            if (matcher.find()) {
                String op = matcher.group(1);
                String col = matcher.group(3);
                String type = matcher.group(5);
                type = handleType(type);
                ddl =
                        String.format(
                                EXECUTE_DDL,
                                JsonDebeziumChangeUtils.getDorisTableIdentifier(
                                        record, dorisOptions, tableMapping),
                                op,
                                col,
                                type);
                LOG.info("parse ddl:{}", ddl);
                return ddl;
            }
        }
        return null;
    }

    private String handleType(String type) {
        if (type == null || "".equals(type)) {
            return "";
        }
        // varchar len * 3
        Pattern pattern = Pattern.compile("varchar\\(([1-9][0-9]*)\\)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(type);
        if (matcher.find()) {
            String len = matcher.group(1);
            return String.format("varchar(%d)", Math.min(Integer.parseInt(len) * 3, 65533));
        }

        return type;
    }
}
