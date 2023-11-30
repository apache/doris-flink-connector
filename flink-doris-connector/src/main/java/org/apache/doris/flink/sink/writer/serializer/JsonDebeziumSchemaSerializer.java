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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeHelper;
import org.apache.doris.flink.sink.schema.SchemaChangeHelper.DDLSchema;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.doris.flink.tools.cdc.mysql.MysqlType;
import org.apache.doris.flink.tools.cdc.oracle.OracleType;
import org.apache.doris.flink.tools.cdc.postgres.PostgresType;
import org.apache.doris.flink.tools.cdc.sqlserver.SqlServerType;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.doris.flink.sink.util.DeleteOperation.addDeleteSign;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

public class JsonDebeziumSchemaSerializer implements DorisRecordSerializer<String> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonDebeziumSchemaSerializer.class);
    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete

    public static final String EXECUTE_DDL = "ALTER TABLE %s %s COLUMN %s %s"; // alter table tbl add cloumn aca int
    private static final String addDropDDLRegex
            = "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s]+))?.*";
    private static final Pattern renameDDLPattern = Pattern.compile(
            "ALTER\\s+TABLE\\s+(\\w+)\\s+RENAME\\s+COLUMN\\s+(\\w+)\\s+TO\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
    private final Pattern addDropDDLPattern;
    private DorisOptions dorisOptions;
    private ObjectMapper objectMapper = new ObjectMapper();
    private String database;
    private String table;
    // table name of the cdc upstream, format is db.tbl
    private String sourceTableName;
    private boolean firstLoad;
    private boolean firstSchemaChange;
    private Map<String, FieldSchema> originFieldSchemaMap;
    private final boolean newSchemaChange;
    private String lineDelimiter = LINE_DELIMITER_DEFAULT;
    private boolean ignoreUpdateBefore = true;
    private SourceConnector sourceConnector;
    private SchemaChangeManager schemaChangeManager;
    // <cdc db.schema.table, doris db.table>
    private Map<String, String> tableMapping;

    public JsonDebeziumSchemaSerializer(DorisOptions dorisOptions,
            Pattern pattern,
            String sourceTableName,
            boolean newSchemaChange) {
        this.dorisOptions = dorisOptions;
        this.addDropDDLPattern = pattern == null ? Pattern.compile(addDropDDLRegex, Pattern.CASE_INSENSITIVE) : pattern;
        if(!StringUtils.isNullOrWhitespaceOnly(dorisOptions.getTableIdentifier())){
            String[] tableInfo = dorisOptions.getTableIdentifier().split("\\.");
            this.database = tableInfo[0];
            this.table = tableInfo[1];
        }
        this.sourceTableName = sourceTableName;
        // Prevent loss of decimal data precision
        this.objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        this.objectMapper.setNodeFactory(jsonNodeFactory);
        this.newSchemaChange = newSchemaChange;
        this.firstLoad = true;
        this.firstSchemaChange = true;
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
    }

    public JsonDebeziumSchemaSerializer(DorisOptions dorisOptions,
            Pattern pattern,
            String sourceTableName,
            boolean newSchemaChange,
            DorisExecutionOptions executionOptions) {
        this(dorisOptions, pattern, sourceTableName, newSchemaChange);
        if (executionOptions != null) {
            this.lineDelimiter = executionOptions.getStreamLoadProp()
                    .getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT);
            this.ignoreUpdateBefore = executionOptions.getIgnoreUpdateBefore();
        }
    }

    public JsonDebeziumSchemaSerializer(DorisOptions dorisOptions,
            Pattern pattern,
            String sourceTableName,
            boolean newSchemaChange,
            DorisExecutionOptions executionOptions,
            Map<String, String> tableMapping) {
        this(dorisOptions, pattern, sourceTableName, newSchemaChange, executionOptions);
        this.tableMapping = tableMapping;
    }

    @Override
    public DorisRecord serialize(String record) throws IOException {
        LOG.debug("received debezium json data {} :", record);
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);

        //Filter out table records that are not in tableMapping
        String cdcTableIdentifier = getCdcTableIdentifier(recordRoot);
        String dorisTableIdentifier = getDorisTableIdentifier(cdcTableIdentifier);
        if(StringUtils.isNullOrWhitespaceOnly(dorisTableIdentifier)){
            LOG.warn("filter table {}, because it is not listened, record detail is {}", cdcTableIdentifier, record);
            return null;
        }

        String op = extractJsonNode(recordRoot, "op");
        if (Objects.isNull(op)) {
            // schema change ddl
            if (newSchemaChange) {
                schemaChangeV2(recordRoot);
            } else {
                schemaChange(recordRoot);
            }
            return null;
        }

        if (newSchemaChange && firstLoad) {
            initOriginFieldSchema(recordRoot);
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
                addDeleteSign(valueMap, true);
                break;
            default:
                LOG.error("parse record fail, unknown op {} in {}", op, record);
                return null;
        }

        return DorisRecord.of(dorisTableIdentifier, objectMapper.writeValueAsString(valueMap).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Change the update event into two
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
            updateRow.append(objectMapper.writeValueAsString(beforeRow))
                    .append(this.lineDelimiter);
        }

        // convert insert
        Map<String, Object> afterRow = extractAfterRow(recordRoot);
        addDeleteSign(afterRow, false);
        updateRow.append(objectMapper.writeValueAsString(afterRow));
        return updateRow.toString().getBytes(StandardCharsets.UTF_8);
    }

    public boolean schemaChangeV2(JsonNode recordRoot) {
        boolean status = false;
        try {
            if (!StringUtils.isNullOrWhitespaceOnly(sourceTableName) && !checkTable(recordRoot)) {
                return false;
            }

            // db,table
            Tuple2<String, String> tuple = getDorisTableTuple(recordRoot);
            if(tuple == null){
                return false;
            }

            List<String> ddlSqlList = extractDDLList(recordRoot);
            if (CollectionUtils.isEmpty(ddlSqlList)) {
                LOG.info("ddl can not do schema change:{}", recordRoot);
                return false;
            }

            List<DDLSchema> ddlSchemas = SchemaChangeHelper.getDdlSchemas();
            for (int i = 0; i < ddlSqlList.size(); i++) {
                DDLSchema ddlSchema = ddlSchemas.get(i);
                String ddlSql = ddlSqlList.get(i);
                boolean doSchemaChange = checkSchemaChange(tuple.f0, tuple.f1, ddlSchema);
                status = doSchemaChange && schemaChangeManager.execute(ddlSql, tuple.f0);
                LOG.info("schema change status:{}, ddl:{}", status, ddlSql);
            }
        } catch (Exception ex) {
            LOG.warn("schema change error :", ex);
        }
        return status;
    }

    @VisibleForTesting
    public List<String> extractDDLList(JsonNode record) throws JsonProcessingException {
        String dorisTable = getDorisTableIdentifier(record);
        JsonNode historyRecord = extractHistoryRecord(record);
        JsonNode tableChanges = historyRecord.get("tableChanges");
        String ddl = extractJsonNode(historyRecord, "ddl");
        if (Objects.isNull(tableChanges) || Objects.isNull(ddl)) {
            return new ArrayList<>();
        }
        LOG.debug("received debezium ddl :{}", ddl);
        JsonNode tableChange = tableChanges.get(0);
        if (Objects.isNull(tableChange) || !tableChange.get("type").asText().equals("ALTER")) {
            return null;
        }

        JsonNode columns = tableChange.get("table").get("columns");
        if (firstSchemaChange) {
            sourceConnector = SourceConnector.valueOf(record.get("source").get("connector").asText().toUpperCase());
            fillOriginSchema(columns);
        }

        // rename ddl
        Matcher renameMatcher = renameDDLPattern.matcher(ddl);
        if (renameMatcher.find()) {
            String oldColumnName = renameMatcher.group(2);
            String newColumnName = renameMatcher.group(3);
            return SchemaChangeHelper.generateRenameDDLSql(
                    dorisTable, oldColumnName, newColumnName, originFieldSchemaMap);
        }

        // add/drop ddl
        Map<String, FieldSchema> updateFiledSchema = new LinkedHashMap<>();
        for (JsonNode column : columns) {
            buildFieldSchema(updateFiledSchema, column);
        }
        SchemaChangeHelper.compareSchema(updateFiledSchema, originFieldSchemaMap);
        // In order to avoid other source table column change operations other than add/drop/rename,
        // which may lead to the accidental deletion of the doris column.
        Matcher matcher = addDropDDLPattern.matcher(ddl);
        if (!matcher.find()) {
            return null;
        }
        return SchemaChangeHelper.generateDDLSql(dorisTable);
    }

    @VisibleForTesting
    public void setOriginFieldSchemaMap(Map<String, FieldSchema> originFieldSchemaMap) {
        this.originFieldSchemaMap = originFieldSchemaMap;
    }
    @VisibleForTesting
    public boolean schemaChange(JsonNode recordRoot) {
        boolean status = false;
        try {
            if (!StringUtils.isNullOrWhitespaceOnly(sourceTableName) && !checkTable(recordRoot)) {
                return false;
            }
            // db,table
            Tuple2<String, String> tuple = getDorisTableTuple(recordRoot);
            if(tuple == null){
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

    /**
     * When cdc synchronizes multiple tables, it will capture multiple table schema changes
     */
    protected boolean checkTable(JsonNode recordRoot) {
        String db = extractDatabase(recordRoot);
        String tbl = extractTable(recordRoot);
        String dbTbl = db + "." + tbl;
        return sourceTableName.equals(dbTbl);
    }

    public String getCdcTableIdentifier(JsonNode record){
        String db = extractJsonNode(record.get("source"), "db");
        String schema = extractJsonNode(record.get("source"), "schema");
        String table = extractJsonNode(record.get("source"), "table");
        return SourceSchema.getString(db, schema, table);
    }

    public String getDorisTableIdentifier(String cdcTableIdentifier){
        if(!StringUtils.isNullOrWhitespaceOnly(dorisOptions.getTableIdentifier())){
            return dorisOptions.getTableIdentifier();
        }
        if(!CollectionUtil.isNullOrEmpty(tableMapping)
                && !StringUtils.isNullOrWhitespaceOnly(cdcTableIdentifier)
                && tableMapping.get(cdcTableIdentifier) != null){
            return tableMapping.get(cdcTableIdentifier);
        }
        return null;
    }

    protected String getDorisTableIdentifier(JsonNode record){
        String identifier = getCdcTableIdentifier(record);
        return getDorisTableIdentifier(identifier);
    }

    protected Tuple2<String, String> getDorisTableTuple(JsonNode record){
        String identifier = getDorisTableIdentifier(record);
        if(StringUtils.isNullOrWhitespaceOnly(identifier)){
            return null;
        }
        String[] tableInfo = identifier.split("\\.");
        if(tableInfo.length != 2){
            return null;
        }
        return Tuple2.of(tableInfo[0], tableInfo[1]);
    }

    private boolean checkSchemaChange(String database, String table, String ddl) throws IOException, IllegalArgumentException {
        Map<String, Object> param = buildRequestParam(ddl);
        return schemaChangeManager.checkSchemaChange(database, table, param);
    }

    private boolean checkSchemaChange(String database, String table, DDLSchema ddlSchema) throws IOException, IllegalArgumentException {
        Map<String, Object> param = SchemaChangeManager.buildRequestParam(ddlSchema.isDropColumn(), ddlSchema.getColumnName());
        return schemaChangeManager.checkSchemaChange(database, table, param);
    }

    /**
     * Build param
     * {
     * "isDropColumn": true,
     * "columnName" : "column"
     * }
     */
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

    protected String extractDatabase(JsonNode record) {
        if (record.get("source").has("schema")) {
            // compatible with schema
            return extractJsonNode(record.get("source"), "schema");
        } else {
            return extractJsonNode(record.get("source"), "db");
        }
    }

    protected String extractTable(JsonNode record) {
        return extractJsonNode(record.get("source"), "table");
    }

    private String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null &&
                !(record.get(key) instanceof NullNode) ? record.get(key).asText() : null;
    }

    private Map<String, Object> extractBeforeRow(JsonNode record) {
        return extractRow(record.get("before"));
    }

    private Map<String, Object> extractAfterRow(JsonNode record) {
        return extractRow(record.get("after"));
    }

    private Map<String, Object> extractRow(JsonNode recordRow) {
        Map<String, Object> recordMap = objectMapper.convertValue(recordRow, new TypeReference<Map<String, Object>>() {
        });
        return recordMap != null ? recordMap : new HashMap<>();
    }

    private JsonNode extractHistoryRecord(JsonNode record) throws JsonProcessingException {
        if (record.has("historyRecord")) {
            return objectMapper.readTree(record.get("historyRecord").asText());
        }
        // The ddl passed by some scenes will not be included in the historyRecord, such as DebeziumSourceFunction
        return record;
    }

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
                ddl = String.format(EXECUTE_DDL, getDorisTableIdentifier(record), op, col, type);
                LOG.info("parse ddl:{}", ddl);
                return ddl;
            }
        }
        return null;
    }



    @VisibleForTesting
    public void fillOriginSchema(JsonNode columns) {
        if (Objects.nonNull(originFieldSchemaMap)) {
            for (JsonNode column : columns) {
                String fieldName = column.get("name").asText();
                if (originFieldSchemaMap.containsKey(fieldName)) {
                    String dorisTypeName = buildDorisTypeName(column);
                    String defaultValue = handleDefaultValue(extractJsonNode(column, "defaultValueExpression"));
                    String comment = extractJsonNode(column, "comment");
                    FieldSchema fieldSchema = originFieldSchemaMap.get(fieldName);
                    fieldSchema.setName(fieldName);
                    fieldSchema.setTypeString(dorisTypeName);
                    fieldSchema.setComment(comment);
                    fieldSchema.setDefaultValue(defaultValue);
                }
            }
        } else {
            LOG.error("Current schema change failed! You need to ensure that "
                    + "there is data in the table." + dorisOptions.getTableIdentifier());
            originFieldSchemaMap = new LinkedHashMap<>();
            columns.forEach(column -> buildFieldSchema(originFieldSchemaMap, column));
        }
        firstSchemaChange = false;
        firstLoad = false;
    }

    private void buildFieldSchema(Map<String, FieldSchema> filedSchemaMap, JsonNode column) {
        String fieldName = column.get("name").asText();
        String dorisTypeName = buildDorisTypeName(column);
        String defaultValue = handleDefaultValue(extractJsonNode(column, "defaultValueExpression"));
        String comment = extractJsonNode(column, "comment");
        filedSchemaMap.put(fieldName, new FieldSchema(fieldName, dorisTypeName, defaultValue, comment));
    }

    @VisibleForTesting
    public String buildDorisTypeName(JsonNode column) {
        int length = column.get("length") == null ? 0 : column.get("length").asInt();
        int scale = column.get("scale") == null ? 0 : column.get("scale").asInt();
        String sourceTypeName = column.get("typeName").asText();
        String dorisTypeName;
        switch (sourceConnector) {
            case MYSQL:
                dorisTypeName = MysqlType.toDorisType(sourceTypeName, length, scale);
                break;
            case ORACLE:
                dorisTypeName = OracleType.toDorisType(sourceTypeName, length, scale);
                break;
            case POSTGRES:
                dorisTypeName = PostgresType.toDorisType(sourceTypeName, length, scale);
                break;
            case SQLSERVER:
                dorisTypeName = SqlServerType.toDorisType(sourceTypeName, length, scale);
                break;
            default:
                String errMsg = "Not support " + sourceTypeName + " schema change.";
                throw new UnsupportedOperationException(errMsg);
        }
        return dorisTypeName;
    }

    private String handleDefaultValue(String defaultValue) {
        if (StringUtils.isNullOrWhitespaceOnly(defaultValue)) {
            return null;
        }
        // Due to historical reasons, doris needs to add quotes to the default value of the new column
        // For example in mysql: alter table add column c1 int default 100
        // In Doris: alter table add column c1 int default '100'
        if (Pattern.matches("['\"].*?['\"]", defaultValue)) {
            return defaultValue;
        } else if (defaultValue.equals("1970-01-01 00:00:00")) {
            // TODO: The default value of setting the current time in CDC is 1970-01-01 00:00:00
            return "current_timestamp";
        }
        return "'" + defaultValue + "'";
    }

    private void initOriginFieldSchema(JsonNode recordRoot) {
        originFieldSchemaMap = new LinkedHashMap<>();
        Set<String> columnNameSet = extractAfterRow(recordRoot).keySet();
        if (CollectionUtils.isEmpty(columnNameSet)) {
            columnNameSet = extractBeforeRow(recordRoot).keySet();
        }
        columnNameSet.forEach(columnName -> originFieldSchemaMap.put(columnName, new FieldSchema()));
        firstLoad = false;
    }

    @VisibleForTesting
    public Map<String, FieldSchema> getOriginFieldSchemaMap() {
        return originFieldSchemaMap;
    }

    @VisibleForTesting
    public void setSourceConnector(String sourceConnector) {
        this.sourceConnector = SourceConnector.valueOf(sourceConnector.toUpperCase());
    }

    @VisibleForTesting
    public void setTableMapping(Map<String, String> tableMapping) {
        this.tableMapping = tableMapping;
    }

    public static JsonDebeziumSchemaSerializer.Builder builder() {
        return new JsonDebeziumSchemaSerializer.Builder();
    }

    /**
     * Builder for JsonDebeziumSchemaSerializer.
     */
    public static class Builder {
        private DorisOptions dorisOptions;
        private Pattern addDropDDLPattern;
        private String sourceTableName;
        private boolean newSchemaChange;
        private DorisExecutionOptions executionOptions;
        private Map<String, String> tableMapping;

        public JsonDebeziumSchemaSerializer.Builder setDorisOptions(DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public JsonDebeziumSchemaSerializer.Builder setNewSchemaChange(boolean newSchemaChange) {
            this.newSchemaChange = newSchemaChange;
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

        public JsonDebeziumSchemaSerializer build() {
            return new JsonDebeziumSchemaSerializer(dorisOptions, addDropDDLPattern, sourceTableName, newSchemaChange,
                    executionOptions, tableMapping);
        }
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