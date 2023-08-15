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

package org.apache.doris.flink.sink.writer;

import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.sink.HttpGetWithEntity;
import org.apache.doris.flink.sink.writer.SchemaChangeHelper.DDLSchema;
import org.apache.doris.flink.tools.cdc.mysql.MysqlType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import static org.apache.doris.flink.sink.writer.LoadConstants.DORIS_DELETE_SIGN;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonDebeziumSchemaSerializer implements DorisRecordSerializer<String> {
    public static final String EXECUTE_DDL = "ALTER TABLE %s %s COLUMN %s %s"; // alter table tbl add cloumn aca int
    private static final Logger LOG = LoggerFactory.getLogger(JsonDebeziumSchemaSerializer.class);
    private static final String CHECK_SCHEMA_CHANGE_API = "http://%s/api/enable_light_schema_change/%s/%s";
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete
    private static final String addDropDDLRegex
            = "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s]+))?.*";
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

    public JsonDebeziumSchemaSerializer(DorisOptions dorisOptions,
            Pattern pattern,
            String sourceTableName,
            boolean newSchemaChange) {
        this.dorisOptions = dorisOptions;
        this.addDropDDLPattern = pattern == null ? Pattern.compile(addDropDDLRegex, Pattern.CASE_INSENSITIVE) : pattern;
        String[] tableInfo = dorisOptions.getTableIdentifier().split("\\.");
        this.database = tableInfo[0];
        this.table = tableInfo[1];
        this.sourceTableName = sourceTableName;
        // Prevent loss of decimal data precision
        this.objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        this.objectMapper.setNodeFactory(jsonNodeFactory);
        this.newSchemaChange = newSchemaChange;
        this.firstLoad = true;
        this.firstSchemaChange = true;
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

    @Override
    public byte[] serialize(String record) throws IOException {
        LOG.debug("received debezium json data {} :", record);
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
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
                return extractUpdate(recordRoot);
            case OP_DELETE:
                valueMap = extractBeforeRow(recordRoot);
                addDeleteSign(valueMap, true);
                break;
            default:
                LOG.error("parse record fail, unknown op {} in {}", op, record);
                return null;
        }
        return objectMapper.writeValueAsString(valueMap).getBytes(StandardCharsets.UTF_8);
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
            List<String> ddlSqlList = extractDDLList(recordRoot);
            if (CollectionUtils.isEmpty(ddlSqlList)) {
                LOG.info("ddl can not do schema change:{}", recordRoot);
                return false;
            }

            List<DDLSchema> ddlSchemas = SchemaChangeHelper.getDdlSchemas();
            for (int i = 0; i < ddlSqlList.size(); i++) {
                DDLSchema ddlSchema = ddlSchemas.get(i);
                String ddlSql = ddlSqlList.get(i);
                boolean doSchemaChange = checkSchemaChange(ddlSchema);
                status = doSchemaChange && execSchemaChange(ddlSql);
                LOG.info("schema change status:{}", status);
            }
        } catch (Exception ex) {
            LOG.warn("schema change error :", ex);
        }
        return status;
    }

    private boolean checkSchemaChange(String ddl) throws IOException, IllegalArgumentException {
        String requestUrl = String.format(CHECK_SCHEMA_CHANGE_API,
                RestService.randomEndpoint(dorisOptions.getFenodes(), LOG), database, table);
        Map<String, Object> param = buildRequestParam(ddl);
        if (param.size() != 2) {
            return false;
        }
        HttpGetWithEntity httpGet = new HttpGetWithEntity(requestUrl);
        httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpGet.setEntity(new StringEntity(objectMapper.writeValueAsString(param)));
        boolean success = handleResponse(httpGet);
        if (!success) {
            LOG.warn("schema change can not do table {}.{}", database, table);
        }
        return success;
    }

    private boolean checkSchemaChange(DDLSchema ddlSchema) throws IOException, IllegalArgumentException {
        String requestUrl = String.format(CHECK_SCHEMA_CHANGE_API,
                RestService.randomEndpoint(dorisOptions.getFenodes(), LOG), database, table);
        Map<String, Object> param = buildRequestParam(ddlSchema);
        HttpGetWithEntity httpGet = new HttpGetWithEntity(requestUrl);
        httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpGet.setEntity(new StringEntity(objectMapper.writeValueAsString(param)));
        boolean success = handleResponse(httpGet);
        if (!success) {
            LOG.warn("schema change can not do table {}.{}", database, table);
        }
        return success;
    }

    @VisibleForTesting
    public List<String> extractDDLList(JsonNode record) throws JsonProcessingException {
        JsonNode historyRecord = objectMapper.readTree(extractJsonNode(record, "historyRecord"));
        JsonNode tableChanges = historyRecord.get("tableChanges");
        JsonNode tableChange = tableChanges.get(0);
        String ddl = extractJsonNode(historyRecord, "ddl");
        LOG.debug("received debezium ddl :{}", ddl);

        Matcher matcher = addDropDDLPattern.matcher(ddl);
        if (Objects.isNull(tableChange) || !tableChange.get("type").asText().equals("ALTER") || !matcher.find()) {
            return null;
        }

        JsonNode columns = tableChange.get("table").get("columns");
        if (firstSchemaChange) {
            fillOriginSchema(columns);
        }
        Map<String, FieldSchema> updateFiledSchema = new LinkedHashMap<>();
        for (JsonNode column : columns) {
            buildFieldSchema(updateFiledSchema, column);
        }
        SchemaChangeHelper.compareSchema(updateFiledSchema, originFieldSchemaMap);
        return SchemaChangeHelper.generateDDLSql(dorisOptions.getTableIdentifier());
    }

    @VisibleForTesting
    public boolean schemaChange(JsonNode recordRoot) {
        boolean status = false;
        try {
            if (!StringUtils.isNullOrWhitespaceOnly(sourceTableName) && !checkTable(recordRoot)) {
                return false;
            }
            String ddl = extractDDL(recordRoot);
            if (StringUtils.isNullOrWhitespaceOnly(ddl)) {
                LOG.info("ddl can not do schema change:{}", recordRoot);
                return false;
            }
            boolean doSchemaChange = checkSchemaChange(ddl);
            status = doSchemaChange && execSchemaChange(ddl);
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

    private void addDeleteSign(Map<String, Object> valueMap, boolean delete) {
        if (delete) {
            valueMap.put(DORIS_DELETE_SIGN, "1");
        } else {
            valueMap.put(DORIS_DELETE_SIGN, "0");
        }
    }

    protected Map<String, Object> buildRequestParam(DDLSchema ddlSchema) {
        Map<String, Object> params = new HashMap<>();
        params.put("isDropColumn", ddlSchema.isDropColumn());
        params.put("columnName", ddlSchema.getColumnName());
        return params;
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

    private boolean execSchemaChange(String ddl) throws IOException, IllegalArgumentException {
        Map<String, String> param = new HashMap<>();
        param.put("stmt", ddl);
        String requestUrl = String.format(SCHEMA_CHANGE_API, RestService.randomEndpoint(dorisOptions.getFenodes(), LOG),
                database);
        HttpPost httpPost = new HttpPost(requestUrl);
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        httpPost.setEntity(new StringEntity(objectMapper.writeValueAsString(param)));
        boolean success = handleResponse(httpPost);
        return success;
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

    private boolean handleResponse(HttpUriRequest request) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(request);
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 && response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                Map<String, Object> responseMap = objectMapper.readValue(loadResult, Map.class);
                String code = responseMap.getOrDefault("code", "-1").toString();
                if (code.equals("0")) {
                    return true;
                } else {
                    LOG.error("schema change response:{}", loadResult);
                }
            }
        } catch (Exception e) {
            LOG.error("http request error,", e);
        }
        return false;
    }

    private String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null
                && !(record.get(key) instanceof NullNode) ? record.get(key).asText() : null;
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

    public String extractDDL(JsonNode record) throws JsonProcessingException {
        String historyRecord = extractJsonNode(record, "historyRecord");
        if (Objects.isNull(historyRecord)) {
            return null;
        }
        String ddl = extractJsonNode(objectMapper.readTree(historyRecord), "ddl");
        LOG.debug("received debezium ddl :{}", ddl);
        if (!Objects.isNull(ddl)) {
            // filter add/drop operation
            Matcher matcher = addDropDDLPattern.matcher(ddl);
            if (matcher.find()) {
                String op = matcher.group(1);
                String col = matcher.group(3);
                String type = matcher.group(5);
                type = handleType(type);
                ddl = String.format(EXECUTE_DDL, dorisOptions.getTableIdentifier(), op, col, type);
                LOG.info("parse ddl:{}", ddl);
                return ddl;
            }
        }
        return null;
    }

    private String authHeader() {
        return "Basic " + new String(Base64.encodeBase64(
                (dorisOptions.getUsername() + ":" + dorisOptions.getPassword()).getBytes(StandardCharsets.UTF_8)));
    }

    @VisibleForTesting
    public void fillOriginSchema(JsonNode columns) {
        if (Objects.nonNull(originFieldSchemaMap)) {
            for (JsonNode column : columns) {
                String fieldName = column.get("name").asText();
                if (originFieldSchemaMap.containsKey(fieldName)) {
                    JsonNode length = column.get("length");
                    JsonNode scale = column.get("scale");
                    String type = MysqlType.toDorisType(column.get("typeName").asText(),
                            length == null ? 0 : length.asInt(),
                            scale == null ? 0 : scale.asInt());
                    String defaultValue = handleDefaultValue(extractJsonNode(column, "defaultValueExpression"));
                    String comment = extractJsonNode(column, "comment");
                    FieldSchema fieldSchema = originFieldSchemaMap.get(fieldName);
                    fieldSchema.setName(fieldName);
                    fieldSchema.setTypeString(type);
                    fieldSchema.setComment(comment);
                    fieldSchema.setDefaultValue(defaultValue);
                }
            }
        } else {
            originFieldSchemaMap = new LinkedHashMap<>();
            columns.forEach(column -> buildFieldSchema(originFieldSchemaMap, column));
        }
        firstSchemaChange = false;
        firstLoad = false;
    }

    private void buildFieldSchema(Map<String, FieldSchema> filedSchemaMap, JsonNode column) {
        String fieldName = column.get("name").asText();
        JsonNode length = column.get("length");
        JsonNode scale = column.get("scale");
        String type = MysqlType.toDorisType(column.get("typeName").asText(),
                length == null ? 0 : length.asInt(), scale == null ? 0 : scale.asInt());
        String defaultValue = handleDefaultValue(extractJsonNode(column, "defaultValueExpression"));
        String comment = extractJsonNode(column, "comment");
        filedSchemaMap.put(fieldName, new FieldSchema(fieldName, type, defaultValue, comment));
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

        public JsonDebeziumSchemaSerializer build() {
            return new JsonDebeziumSchemaSerializer(dorisOptions, addDropDDLPattern, sourceTableName, newSchemaChange,
                    executionOptions);
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