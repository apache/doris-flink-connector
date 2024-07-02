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

package org.apache.doris.flink.sink.schema;

import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Base64;
import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.DorisSchemaChangeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Field;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.sink.HttpGetWithEntity;
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
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SchemaChangeManager implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeManager.class);
    private static final String CHECK_SCHEMA_CHANGE_API =
            "http://%s/api/enable_light_schema_change/%s/%s";
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private ObjectMapper objectMapper = new ObjectMapper();
    private DorisOptions dorisOptions;
    private String charsetEncoding = "UTF-8";

    public SchemaChangeManager(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
    }

    public SchemaChangeManager(DorisOptions dorisOptions, String charsetEncoding) {
        this.dorisOptions = dorisOptions;
        this.charsetEncoding = charsetEncoding;
    }

    public boolean createTable(TableSchema table) throws IOException, IllegalArgumentException {
        // auto create database if not exists
        if (!checkDatabaseExists(table.getDatabase())) {
            execute(
                    SchemaChangeHelper.buildCreateDatabaseDDL(table.getDatabase()),
                    SchemaChangeHelper.DEFAULT_DATABASE);
        }
        String createTableDDL = DorisSystem.buildCreateTableDDL(table);
        return execute(createTableDDL, table.getDatabase());
    }

    public boolean addColumn(String database, String table, FieldSchema field)
            throws IOException, IllegalArgumentException {
        if (checkColumnExists(database, table, field.getName())) {
            LOG.warn(
                    "The column {} already exists in table {}, no need to add it again",
                    field.getName(),
                    table);
            return true;
        }
        String tableIdentifier = getTableIdentifier(database, table);
        String addColumnDDL = SchemaChangeHelper.buildAddColumnDDL(tableIdentifier, field);
        return schemaChange(
                database, table, buildRequestParam(false, field.getName()), addColumnDDL);
    }

    public boolean dropColumn(String database, String table, String columnName)
            throws IOException, IllegalArgumentException {
        if (!checkColumnExists(database, table, columnName)) {
            LOG.warn("The column {} not exists in table {}, no need to drop", columnName, table);
            return true;
        }
        String tableIdentifier = getTableIdentifier(database, table);
        String dropColumnDDL = SchemaChangeHelper.buildDropColumnDDL(tableIdentifier, columnName);
        return schemaChange(database, table, buildRequestParam(true, columnName), dropColumnDDL);
    }

    public boolean renameColumn(
            String database, String table, String oldColumnName, String newColumnName)
            throws IOException, IllegalArgumentException {
        String tableIdentifier = getTableIdentifier(database, table);
        String renameColumnDDL =
                SchemaChangeHelper.buildRenameColumnDDL(
                        tableIdentifier, oldColumnName, newColumnName);
        return schemaChange(
                database, table, buildRequestParam(true, oldColumnName), renameColumnDDL);
    }

    public boolean modifyColumnDataType(String database, String table, FieldSchema field)
            throws IOException, IllegalArgumentException {
        if (!checkColumnExists(database, table, field.getName())) {
            LOG.warn(
                    "The column {} is not exists in table {}, can not modify it type",
                    field.getName(),
                    table);
            return false;
        }
        // If user does not give a comment, need fill it from
        // original table schema to avoid miss comment
        if (StringUtils.isNullOrWhitespaceOnly(field.getComment())) {
            Schema tableSchema = getTableSchema(database, table);
            Optional<Field> originalField =
                    tableSchema.getProperties().stream()
                            .filter(column -> column.getName().equals(field.getName()))
                            .findAny();
            originalField.ifPresent(oldField -> field.setComment(oldField.getComment()));
        }
        String tableIdentifier = getTableIdentifier(database, table);
        String modifyColumnDDL =
                SchemaChangeHelper.buildModifyColumnDataTypeDDL(tableIdentifier, field);
        return schemaChange(
                database, table, buildRequestParam(false, field.getName()), modifyColumnDDL);
    }

    public boolean modifyColumnComment(
            String database, String table, String columnName, String newComment)
            throws IOException, IllegalArgumentException {
        if (!checkColumnExists(database, table, columnName)) {
            LOG.warn(
                    "The column {} is not exists in table {}, can not modify it's comment",
                    columnName,
                    table);
            return false;
        }
        String tableIdentifier = getTableIdentifier(database, table);
        String modifyColumnCommentDDL =
                SchemaChangeHelper.buildModifyColumnCommentDDL(
                        tableIdentifier, columnName, newComment);
        return schemaChange(
                database, table, buildRequestParam(false, columnName), modifyColumnCommentDDL);
    }

    public Schema getTableSchema(String database, String table) {
        return RestService.getSchema(dorisOptions, database, table, LOG);
    }

    public boolean schemaChange(
            String database, String table, Map<String, Object> params, String sql)
            throws IOException, IllegalArgumentException {
        if (checkSchemaChange(database, table, params)) {
            return execute(sql, database);
        }
        return false;
    }

    public static Map<String, Object> buildRequestParam(boolean dropColumn, String columnName) {
        Map<String, Object> params = new HashMap<>();
        params.put("isDropColumn", dropColumn);
        params.put("columnName", columnName);
        return params;
    }

    /** check ddl can do light schema change. */
    public boolean checkSchemaChange(String database, String table, Map<String, Object> params)
            throws IOException, IllegalArgumentException {
        if (CollectionUtil.isNullOrEmpty(params)) {
            return false;
        }
        String requestUrl =
                String.format(
                        CHECK_SCHEMA_CHANGE_API,
                        RestService.randomEndpoint(dorisOptions.getFenodes(), LOG),
                        database,
                        table);
        HttpGetWithEntity httpGet = new HttpGetWithEntity(requestUrl);
        httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpGet.setEntity(
                new StringEntity(objectMapper.writeValueAsString(params), charsetEncoding));
        String responseEntity = "";
        Map<String, Object> responseMap = handleResponse(httpGet, responseEntity);
        return handleSchemaChange(responseMap, responseEntity);
    }

    private boolean handleSchemaChange(Map<String, Object> responseMap, String responseEntity) {
        String code = responseMap.getOrDefault("code", "-1").toString();
        if (code.equals("0")) {
            return true;
        } else {
            throw new DorisSchemaChangeException(
                    "Failed to schemaChange, response: " + responseEntity);
        }
    }

    /** execute sql in doris. */
    public boolean execute(String ddl, String database)
            throws IOException, IllegalArgumentException {
        if (StringUtils.isNullOrWhitespaceOnly(ddl)) {
            return false;
        }
        LOG.info("Execute SQL: {}", ddl);
        HttpPost httpPost = buildHttpPost(ddl, database);
        String responseEntity = "";
        Map<String, Object> responseMap = handleResponse(httpPost, responseEntity);
        return handleSchemaChange(responseMap, responseEntity);
    }

    public HttpPost buildHttpPost(String ddl, String database)
            throws IllegalArgumentException, IOException {
        Map<String, String> param = new HashMap<>();
        param.put("stmt", ddl);
        String requestUrl =
                String.format(
                        SCHEMA_CHANGE_API,
                        RestService.randomEndpoint(dorisOptions.getFenodes(), LOG),
                        database);
        HttpPost httpPost = new HttpPost(requestUrl);
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpPost.setHeader(
                HttpHeaders.CONTENT_TYPE,
                String.format("application/json;charset=%s", charsetEncoding));
        httpPost.setEntity(
                new StringEntity(objectMapper.writeValueAsString(param), charsetEncoding));
        return httpPost;
    }

    private Map<String, Object> handleResponse(HttpUriRequest request, String responseEntity) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(request);
            final int statusCode = response.getStatusLine().getStatusCode();
            final String reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode == 200 && response.getEntity() != null) {
                responseEntity = EntityUtils.toString(response.getEntity());
                Map<String, Object> responseMap = objectMapper.readValue(responseEntity, Map.class);
                return responseMap;
            } else {
                throw new DorisSchemaChangeException(
                        "Failed to schemaChange, status: "
                                + statusCode
                                + ", reason: "
                                + reasonPhrase);
            }
        } catch (Exception e) {
            LOG.error("SchemaChange request error,", e);
            throw new DorisSchemaChangeException(
                    "SchemaChange request error with " + e.getMessage());
        }
    }

    /** When processing a column, determine whether it exists and be idempotent. */
    public boolean checkColumnExists(String database, String table, String columnName)
            throws IllegalArgumentException, IOException {
        String existsQuery = SchemaChangeHelper.buildColumnExistsQuery(database, table, columnName);
        return sendHttpPostRequest(existsQuery, database);
    }

    private boolean checkDatabaseExists(String database)
            throws IllegalArgumentException, IOException {
        String existsQuery = SchemaChangeHelper.buildDatabaseExistsQuery(database);
        return sendHttpPostRequest(existsQuery, SchemaChangeHelper.DEFAULT_DATABASE);
    }

    private boolean sendHttpPostRequest(String sql, String database)
            throws IOException, IllegalArgumentException {
        HttpPost httpPost = buildHttpPost(sql, database);
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(httpPost);
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 && response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                JsonNode responseNode = objectMapper.readTree(loadResult);
                String code = responseNode.get("code").asText("-1");
                if (code.equals("0")) {
                    JsonNode data = responseNode.get("data").get("data");
                    if (!data.isEmpty()) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(
                    "send http post request error {}, default return false, SQL:{}",
                    e.getMessage(),
                    sql);
        }
        return false;
    }

    private String authHeader() {
        return "Basic "
                + new String(
                        Base64.encodeBase64(
                                (dorisOptions.getUsername() + ":" + dorisOptions.getPassword())
                                        .getBytes(StandardCharsets.UTF_8)));
    }

    private static String getTableIdentifier(String database, String table) {
        return String.format("%s.%s", database, table);
    }
}
