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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Base64;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
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
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.doris.flink.sink.writer.LoadConstants.DORIS_DELETE_SIGN;

public class JsonDebeziumSchemaSerializer implements DorisRecordSerializer<String> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonDebeziumSchemaSerializer.class);
    private static final String CHECK_SCHEMA_CHANGE_API = "http://%s/api/column_change_can_sync/default_cluster/%s/%s";
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete

    private static final String addDropDDLRegex = "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+COLUMN\\s+.*";
    private final Pattern addDropDDLPattern;
    private DorisOptions dorisOptions;
    private ObjectMapper objectMapper = new ObjectMapper();

    public JsonDebeziumSchemaSerializer(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
        this.addDropDDLPattern = Pattern.compile(addDropDDLRegex, Pattern.CASE_INSENSITIVE);
    }

    public JsonDebeziumSchemaSerializer(DorisOptions dorisOptions, Pattern pattern) {
        this.dorisOptions = dorisOptions;
        this.addDropDDLPattern = pattern;
    }

    @Override
    public byte[] serialize(String record) throws IOException {
        LOG.info("===" + record);
        JsonNode recordRoot = objectMapper.readTree(record);
        String op = extractJsonNode(recordRoot, "op");
        if (Objects.isNull(op)) {
            //schema change ddl
            boolean doSchemaChange = checkSchemaChange(recordRoot);
            boolean status = doSchemaChange && schemaChange(recordRoot);
            LOG.info("schema change status:{}", status);
            return null;
        }
        Map<String, String> valueMap;
        if (OP_READ.equals(op) || OP_CREATE.equals(op)) {
            valueMap = extractAfterRow(recordRoot);
            valueMap.put(DORIS_DELETE_SIGN, "0");
        } else if (OP_UPDATE.equals(op)) {
            valueMap = extractAfterRow(recordRoot);
            valueMap.put(DORIS_DELETE_SIGN, "0");
        } else if (OP_DELETE.equals(op)) {
            valueMap = extractBeforeRow(recordRoot);
            valueMap.put(DORIS_DELETE_SIGN, "1");
        } else {
            LOG.error("parse record fail, unknown op {} in {}",op,record);
            return null;
        }
        return objectMapper.writeValueAsString(valueMap).getBytes(StandardCharsets.UTF_8);
    }

    private boolean checkSchemaChange(JsonNode record) throws IOException {
        String database = extractDatabase(record);
        String table = extractTable(record);
        String requestUrl = String.format(CHECK_SCHEMA_CHANGE_API, dorisOptions.getFenodes(), database, table);
        HttpGet httpGet = new HttpGet(requestUrl);
        boolean success = handleResponse(httpGet);
        if (!success) {
            LOG.warn("schema change can not do table {}.{}",database,table);
        }
        return success;
    }

    private boolean schemaChange(JsonNode record) throws IOException {
        String extractDDL = extractDDL(record);
        Map<String, String> param = new HashMap<>();
        param.put("stmt", extractDDL);
        String database = extractDatabase(record);
        String requestUrl = String.format(SCHEMA_CHANGE_API, dorisOptions.getFenodes(), database);
        HttpPost httpPost = new HttpPost(requestUrl);
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        httpPost.setEntity(new StringEntity(objectMapper.writeValueAsString(param)));
        boolean success = handleResponse(httpPost);
        return success;
    }

    private String extractDatabase(JsonNode record) {
        return extractJsonNode(record.get("source"), "db");
    }

    private String extractTable(JsonNode record) {
        return extractJsonNode(record.get("source"), "table");
    }

    private boolean handleResponse(HttpUriRequest request) throws IOException {
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
        }catch(Exception e){
            LOG.error("http request error,", e);
        }
        return false;
    }

    private String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null ? record.get(key).asText() : null;
    }

    private Map<String, String> extractBeforeRow(JsonNode record) {
        return extractRow(record.get("before"));
    }

    private Map<String, String> extractAfterRow(JsonNode record) {
        return extractRow(record.get("after"));
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        Map<String, String> recordMap = objectMapper.convertValue(recordRow, new TypeReference<Map<String, String>>() {
        });
        return recordMap != null ? recordMap : new HashMap<>();
    }

    private String extractDDL(JsonNode record) throws JsonProcessingException {
        String historyRecord = extractJsonNode(record, "historyRecord");
        if (Objects.isNull(historyRecord)) {
            return null;
        }
        String ddl = extractJsonNode(objectMapper.readTree(historyRecord), "ddl");
        if (!Objects.isNull(ddl)) {
            //filter add/drop operation
            if (addDropDDLPattern.matcher(ddl).matches()) {
                return ddl;
            }
        }
        LOG.info("parse ddl:{}", ddl);
        return null;
    }

    private String authHeader() {
        return "Basic " + new String(Base64.encodeBase64((dorisOptions.getUsername() + ":" + dorisOptions.getPassword()).getBytes(StandardCharsets.UTF_8)));
    }

    public static JsonDebeziumSchemaSerializer.Builder builder() {
        return new JsonDebeziumSchemaSerializer.Builder();
    }

    /**
     * Builder for JsonDebeziumSchemaSerializer.
     */
    public static class Builder {
        private DorisOptions dorisOptions;

        public JsonDebeziumSchemaSerializer.Builder setDorisOptions(DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public JsonDebeziumSchemaSerializer build() {
            return new JsonDebeziumSchemaSerializer(dorisOptions);
        }
    }
}
