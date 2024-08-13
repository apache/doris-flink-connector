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

package org.apache.doris.flink.rest;

import org.apache.flink.annotation.VisibleForTesting;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.ConfigurationOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.ConnectedFailedException;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.DorisSchemaChangeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.exception.ShouldNeverHappenException;
import org.apache.doris.flink.rest.models.BackendV2;
import org.apache.doris.flink.rest.models.BackendV2.BackendRowV2;
import org.apache.doris.flink.rest.models.QueryPlan;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.rest.models.Tablet;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.HttpGetWithEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_TABLET_SIZE;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_MIN;
import static org.apache.doris.flink.util.ErrorMessages.CONNECT_FAILED_MESSAGE;
import static org.apache.doris.flink.util.ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE;
import static org.apache.doris.flink.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;

/** Service for communicate with Doris FE. */
public class RestService implements Serializable {
    public static final int REST_RESPONSE_STATUS_OK = 200;
    public static final int REST_RESPONSE_CODE_OK = 0;
    private static final String REST_RESPONSE_BE_ROWS_KEY = "rows";
    private static final String UNIQUE_KEYS_TYPE = "UNIQUE_KEYS";
    @Deprecated private static final String BACKENDS = "/rest/v1/system?path=//backends";
    private static final String BACKENDS_V2 = "/api/backends?is_alive=true";
    private static final String FE_LOGIN = "/rest/v1/login";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String TABLE_SCHEMA_API = "http://%s/api/%s/%s/_schema";
    private static final String QUERY_PLAN_API = "http://%s/api/%s/%s/_query_plan";

    /**
     * send request to Doris FE and get response json string.
     *
     * @param options configuration of request
     * @param request {@link HttpRequestBase} real request
     * @param logger {@link Logger}
     * @return Doris FE response in json string
     * @throws ConnectedFailedException throw when cannot connect to Doris FE
     */
    private static String send(
            DorisOptions options,
            DorisReadOptions readOptions,
            HttpRequestBase request,
            Logger logger)
            throws ConnectedFailedException {
        int connectTimeout =
                readOptions.getRequestConnectTimeoutMs() == null
                        ? ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT
                        : readOptions.getRequestConnectTimeoutMs();
        int socketTimeout =
                readOptions.getRequestReadTimeoutMs() == null
                        ? ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT
                        : readOptions.getRequestReadTimeoutMs();
        int retries =
                readOptions.getRequestRetries() == null
                        ? ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT
                        : readOptions.getRequestRetries();
        logger.trace(
                "connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                connectTimeout,
                socketTimeout,
                retries);

        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(connectTimeout)
                        .setSocketTimeout(socketTimeout)
                        .build();

        request.setConfig(requestConfig);
        logger.info(
                "Send request to Doris FE '{}' with user '{}'.",
                request.getURI(),
                options.getUsername());
        IOException ex = null;
        int statusCode = -1;

        for (int attempt = 0; attempt < retries; attempt++) {
            logger.debug("Attempt {} to request {}.", attempt, request.getURI());
            try {
                String response;
                if (request instanceof HttpGet) {
                    response = getConnectionGet(request, options, logger);
                } else {
                    response = getConnectionPost(request, options, logger);
                }
                if (response == null) {
                    logger.warn(
                            "Failed to get response from Doris FE {}, http code is {}",
                            request.getURI(),
                            statusCode);
                    continue;
                }
                logger.trace(
                        "Success get response from Doris FE: {}, response is: {}.",
                        request.getURI(),
                        response);
                // Handle the problem of inconsistent data format returned by http v1 and v2
                ObjectMapper mapper = new ObjectMapper();
                Map map = mapper.readValue(response, Map.class);
                if (map.containsKey("code") && map.containsKey("msg")) {
                    Object data = map.get("data");
                    return mapper.writeValueAsString(data);
                } else {
                    return response;
                }
            } catch (IOException e) {
                ex = e;
                logger.warn(CONNECT_FAILED_MESSAGE, request.getURI(), e);
            }
        }

        logger.error(CONNECT_FAILED_MESSAGE, request.getURI(), ex);
        throw new ConnectedFailedException(request.getURI().toString(), statusCode, ex);
    }

    private static String getConnectionPost(
            HttpRequestBase request, DorisOptions dorisOptions, Logger logger) throws IOException {
        URL url = new URL(request.getURI().toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod(request.getMethod());
        conn.setRequestProperty("Authorization", authHeader(dorisOptions));
        InputStream content = ((HttpPost) request).getEntity().getContent();
        String res = IOUtils.toString(content);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setConnectTimeout(request.getConfig().getConnectTimeout());
        conn.setReadTimeout(request.getConfig().getSocketTimeout());
        PrintWriter out = new PrintWriter(conn.getOutputStream());
        // send request params
        out.print(res);
        // flush
        out.flush();
        // read response
        return parseResponse(conn, logger);
    }

    private static String getConnectionGet(
            HttpRequestBase request, DorisOptions dorisOptions, Logger logger) throws IOException {
        URL realUrl = new URL(request.getURI().toString());
        // open connection
        HttpURLConnection connection = (HttpURLConnection) realUrl.openConnection();
        connection.setRequestProperty("Authorization", authHeader(dorisOptions));

        connection.connect();
        connection.setConnectTimeout(request.getConfig().getConnectTimeout());
        connection.setReadTimeout(request.getConfig().getSocketTimeout());
        return parseResponse(connection, logger);
    }

    @VisibleForTesting
    public static String parseResponse(HttpURLConnection connection, Logger logger)
            throws IOException {
        if (connection.getResponseCode() != HttpStatus.SC_OK) {
            logger.warn(
                    "Failed to get response from Doris  {}, http code is {}",
                    connection.getURL(),
                    connection.getResponseCode());
            throw new IOException("Failed to get response from Doris");
        }
        StringBuffer result = new StringBuffer();
        try (Scanner scanner = new Scanner(connection.getInputStream(), "utf-8")) {
            while (scanner.hasNext()) {
                result.append(scanner.next());
            }
            return result.toString();
        }
    }

    @VisibleForTesting
    public static String parseFlightSql(
            DorisReadOptions readOptions,
            DorisOptions options,
            PartitionDefinition partition,
            Logger logger)
            throws IllegalArgumentException {
        String[] tableIdentifiers = parseIdentifier(options.getTableIdentifier(), logger);
        String readFields =
                StringUtils.isBlank(readOptions.getReadFields())
                        ? "*"
                        : readOptions.getReadFields();
        String sql =
                "select "
                        + readFields
                        + " from `"
                        + tableIdentifiers[0]
                        + "`.`"
                        + tableIdentifiers[1]
                        + "`";
        String tablet =
                partition.getTabletIds().stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","));
        sql += "  TABLET(" + tablet + ") ";
        if (!StringUtils.isEmpty(readOptions.getFilterQuery())) {
            sql += " where " + readOptions.getFilterQuery();
        }
        logger.info("Query SQL Sending to Doris FE is: '{}'.", sql);
        return sql;
    }

    /**
     * parse table identifier to array.
     *
     * @param tableIdentifier table identifier string
     * @param logger {@link Logger}
     * @return first element is db name, second element is table name
     * @throws IllegalArgumentException table identifier is illegal
     */
    @VisibleForTesting
    static String[] parseIdentifier(String tableIdentifier, Logger logger)
            throws IllegalArgumentException {
        logger.trace("Parse identifier '{}'.", tableIdentifier);
        if (StringUtils.isEmpty(tableIdentifier)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "table.identifier", tableIdentifier);
            throw new IllegalArgumentException("table.identifier", tableIdentifier);
        }
        String[] identifier = tableIdentifier.split("\\.");
        if (identifier.length != 2) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "table.identifier", tableIdentifier);
            throw new IllegalArgumentException("table.identifier", tableIdentifier);
        }
        return identifier;
    }

    /**
     * choice a Doris FE node to request.
     *
     * @param feNodes Doris FE node list, separate be comma
     * @param logger slf4j logger
     * @return the chosen one Doris FE node
     * @throws IllegalArgumentException fe nodes is illegal
     */
    @VisibleForTesting
    public static String randomEndpoint(String feNodes, Logger logger)
            throws IllegalArgumentException {
        logger.trace("Parse fenodes '{}'.", feNodes);
        if (StringUtils.isEmpty(feNodes)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "fenodes", feNodes);
            throw new IllegalArgumentException("fenodes", feNodes);
        }
        List<String> nodes = Arrays.asList(feNodes.split(","));
        Collections.shuffle(nodes);
        for (String feNode : nodes) {
            String host = feNode.trim();
            if (BackendUtil.tryHttpConnection(host)) {
                return host;
            }
        }
        throw new DorisRuntimeException(
                "No Doris FE is available, please check configuration or cluster status.");
    }

    /**
     * choice a Doris FE node to request.
     *
     * @param feNodes Doris FE node list, separate be comma
     * @param logger slf4j logger
     * @return the array of Doris FE nodes
     * @throws IllegalArgumentException fe nodes is illegal
     */
    @VisibleForTesting
    static List<String> allEndpoints(String feNodes, Logger logger) {
        logger.trace("Parse fenodes '{}'.", feNodes);
        if (StringUtils.isEmpty(feNodes)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "fenodes", feNodes);
            throw new DorisRuntimeException("fenodes is empty");
        }
        List<String> nodes =
                Arrays.stream(feNodes.split(",")).map(String::trim).collect(Collectors.toList());
        Collections.shuffle(nodes);
        return nodes;
    }

    /**
     * get Doris BE nodes to request.
     *
     * @param options configuration of request
     * @param logger slf4j logger
     * @return the chosen one Doris BE node
     * @throws IllegalArgumentException BE nodes is illegal
     */
    @VisibleForTesting
    public static List<BackendRowV2> getBackendsV2(
            DorisOptions options, DorisReadOptions readOptions, Logger logger) {
        String feNodes = options.getFenodes();
        List<String> feNodeList = allEndpoints(feNodes, logger);

        if (options.isAutoRedirect() && !feNodeList.isEmpty()) {
            return convert(feNodeList);
        }

        for (String feNode : feNodeList) {
            try {
                String beUrl = "http://" + feNode + BACKENDS_V2;
                HttpGet httpGet = new HttpGet(beUrl);
                String response = send(options, readOptions, httpGet, logger);
                logger.info("Backend Info:{}", response);
                List<BackendRowV2> backends = parseBackendV2(response, logger);
                return backends;
            } catch (ConnectedFailedException e) {
                logger.info(
                        "Doris FE node {} is unavailable: {}, Request the next Doris FE node",
                        feNode,
                        e.getMessage());
            }
        }
        String errMsg = "No Doris FE is available, please check configuration";
        logger.error(errMsg);
        throw new DorisRuntimeException(errMsg);
    }

    /**
     * When the user turns on redirection, there is no need to explicitly obtain the be list, just
     * treat the fe list as the be list.
     *
     * @param feNodeList
     * @return
     */
    private static List<BackendRowV2> convert(List<String> feNodeList) {
        List<BackendRowV2> nodeList = new ArrayList<>();
        for (String node : feNodeList) {
            String[] split = node.split(":");
            nodeList.add(BackendRowV2.of(split[0], Integer.valueOf(split[1]), true));
        }
        return nodeList;
    }

    static List<BackendRowV2> parseBackendV2(String response, Logger logger) {
        ObjectMapper mapper = new ObjectMapper();
        BackendV2 backend;
        try {
            backend = mapper.readValue(response, BackendV2.class);
        } catch (IOException e) {
            String errMsg = "Parse Doris BE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisRuntimeException(errMsg, e);
        }

        if (backend == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }
        List<BackendRowV2> backendRows = backend.getBackends();
        logger.debug("Parsing schema result is '{}'.", backendRows);
        return backendRows;
    }

    /**
     * discover Doris table schema from Doris FE.
     *
     * @param options configuration of request
     * @param logger slf4j logger
     * @return Doris table schema
     * @throws DorisException throw when discover failed
     */
    public static Schema getSchema(
            DorisOptions options, DorisReadOptions readOptions, Logger logger)
            throws DorisException {
        logger.trace("Finding schema.");
        String[] tableIdentifier = parseIdentifier(options.getTableIdentifier(), logger);
        String tableSchemaUri =
                String.format(
                        TABLE_SCHEMA_API,
                        randomEndpoint(options.getFenodes(), logger),
                        tableIdentifier[0],
                        tableIdentifier[1]);
        HttpGet httpGet = new HttpGet(tableSchemaUri);
        String response = send(options, readOptions, httpGet, logger);
        logger.debug("Find schema response is '{}'.", response);
        return parseSchema(response, logger);
    }

    public static Schema getSchema(
            DorisOptions dorisOptions, String db, String table, Logger logger) {
        logger.trace("start get " + db + "." + table + " schema from doris.");
        Object responseData = null;
        try {
            String tableSchemaUri =
                    String.format(
                            TABLE_SCHEMA_API,
                            randomEndpoint(dorisOptions.getFenodes(), logger),
                            db,
                            table);
            HttpGetWithEntity httpGet = new HttpGetWithEntity(tableSchemaUri);
            httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader(dorisOptions));
            Map<String, Object> responseMap = handleResponse(httpGet, logger);
            responseData = responseMap.get("data");
            String schemaStr = objectMapper.writeValueAsString(responseData);
            return objectMapper.readValue(schemaStr, Schema.class);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new DorisSchemaChangeException(
                    "can not parse response schema " + responseData, e);
        }
    }

    private static Map handleResponse(HttpUriRequest request, Logger logger) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(request);
            final int statusCode = response.getStatusLine().getStatusCode();
            final String reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode == 200 && response.getEntity() != null) {
                String responseEntity = EntityUtils.toString(response.getEntity());
                return objectMapper.readValue(responseEntity, Map.class);
            } else {
                throw new DorisSchemaChangeException(
                        "Failed to schemaChange, status: "
                                + statusCode
                                + ", reason: "
                                + reasonPhrase);
            }
        } catch (Exception e) {
            logger.trace("SchemaChange request error,", e);
            throw new DorisSchemaChangeException(
                    "SchemaChange request error with " + e.getMessage());
        }
    }

    private static String authHeader(DorisOptions dorisOptions) {
        return "Basic "
                + new String(
                        org.apache.commons.codec.binary.Base64.encodeBase64(
                                (dorisOptions.getUsername() + ":" + dorisOptions.getPassword())
                                        .getBytes(StandardCharsets.UTF_8)));
    }

    public static boolean isUniqueKeyType(
            DorisOptions options, DorisReadOptions readOptions, Logger logger)
            throws DorisRuntimeException {
        // disable 2pc in multi-table scenario
        if (StringUtils.isBlank(options.getTableIdentifier())) {
            logger.info("table model verification is skipped in multi-table scenarios.");
            return true;
        }
        try {
            return UNIQUE_KEYS_TYPE.equals(getSchema(options, readOptions, logger).getKeysType());
        } catch (Exception e) {
            throw new DorisRuntimeException(e);
        }
    }

    /**
     * translate Doris FE response to inner {@link Schema} struct.
     *
     * @param response Doris FE response
     * @param logger {@link Logger}
     * @return inner {@link Schema} struct
     * @throws DorisException throw when translate failed
     */
    @VisibleForTesting
    public static Schema parseSchema(String response, Logger logger) throws DorisException {
        logger.trace("Parse response '{}' to schema.", response);
        ObjectMapper mapper = new ObjectMapper();
        Schema schema;
        try {
            schema = mapper.readValue(response, Schema.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris FE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris FE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        }

        if (schema == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }

        if (schema.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "Doris FE's response is not OK, status is " + schema.getStatus();
            logger.error(errMsg);
            throw new DorisException(errMsg);
        }
        logger.debug("Parsing schema result is '{}'.", schema);
        return schema;
    }

    /**
     * find Doris partitions from Doris FE.
     *
     * @param options configuration of request
     * @param logger {@link Logger}
     * @return a list of Doris partitions
     * @throws DorisException throw when find partition failed
     */
    public static List<PartitionDefinition> findPartitions(
            DorisOptions options, DorisReadOptions readOptions, Logger logger)
            throws DorisException {
        String[] tableIdentifiers = parseIdentifier(options.getTableIdentifier(), logger);
        String readFields =
                StringUtils.isBlank(readOptions.getReadFields())
                        ? "*"
                        : readOptions.getReadFields();
        String sql =
                "select "
                        + readFields
                        + " from `"
                        + tableIdentifiers[0]
                        + "`.`"
                        + tableIdentifiers[1]
                        + "`";
        if (!StringUtils.isEmpty(readOptions.getFilterQuery())) {
            sql += " where " + readOptions.getFilterQuery();
        }
        logger.info("Query SQL Sending to Doris FE is: '{}'.", sql);
        String[] tableIdentifier = parseIdentifier(options.getTableIdentifier(), logger);
        String queryPlanUri =
                String.format(
                        QUERY_PLAN_API,
                        randomEndpoint(options.getFenodes(), logger),
                        tableIdentifier[0],
                        tableIdentifier[1]);
        HttpPost httpPost = new HttpPost(queryPlanUri);
        String entity = "{\"sql\": \"" + sql + "\"}";
        logger.debug("Post body Sending to Doris FE is: '{}'.", entity);
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);

        String resStr = send(options, readOptions, httpPost, logger);
        logger.debug("Find partition response is '{}'.", resStr);
        QueryPlan queryPlan = getQueryPlan(resStr, logger);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan, logger);
        return tabletsMapToPartition(
                options,
                readOptions,
                be2Tablets,
                queryPlan.getOpaquedQueryPlan(),
                tableIdentifiers[0],
                tableIdentifiers[1],
                logger);
    }

    /**
     * translate Doris FE response string to inner {@link QueryPlan} struct.
     *
     * @param response Doris FE response string
     * @param logger {@link Logger}
     * @return inner {@link QueryPlan} struct
     * @throws DorisException throw when translate failed.
     */
    @VisibleForTesting
    static QueryPlan getQueryPlan(String response, Logger logger) throws DorisException {
        ObjectMapper mapper = new ObjectMapper();
        QueryPlan queryPlan;
        try {
            queryPlan = mapper.readValue(response, QueryPlan.class);
        } catch (IOException e) {
            String errMsg = "Parse Doris FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        }

        if (queryPlan == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }

        if (queryPlan.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "Doris FE's response is not OK, status is " + queryPlan.getStatus();
            logger.error(errMsg);
            throw new DorisException(errMsg);
        }
        logger.debug("Parsing partition result is '{}'.", queryPlan);
        return queryPlan;
    }

    /**
     * select which Doris BE to get tablet data.
     *
     * @param queryPlan {@link QueryPlan} translated from Doris FE response
     * @param logger {@link Logger}
     * @return BE to tablets {@link Map}
     * @throws DorisException throw when select failed.
     */
    @VisibleForTesting
    static Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan, Logger logger)
            throws DorisException {
        Map<String, List<Long>> be2Tablets = new HashMap<>();
        for (Entry<String, Tablet> part : queryPlan.getPartitions().entrySet()) {
            logger.debug("Parse tablet info: '{}'.", part);
            long tabletId;
            try {
                tabletId = Long.parseLong(part.getKey());
            } catch (NumberFormatException e) {
                String errMsg = "Parse tablet id '" + part.getKey() + "' to long failed.";
                logger.error(errMsg, e);
                throw new DorisException(errMsg, e);
            }
            String target = null;
            int tabletCount = Integer.MAX_VALUE;
            for (String candidate : part.getValue().getRoutings()) {
                logger.trace("Evaluate Doris BE '{}' to tablet '{}'.", candidate, tabletId);
                if (!be2Tablets.containsKey(candidate)) {
                    logger.debug(
                            "Choice a new Doris BE '{}' for tablet '{}'.", candidate, tabletId);
                    List<Long> tablets = new ArrayList<>();
                    be2Tablets.put(candidate, tablets);
                    target = candidate;
                    break;
                } else {
                    if (be2Tablets.get(candidate).size() < tabletCount) {
                        target = candidate;
                        tabletCount = be2Tablets.get(candidate).size();
                        logger.debug(
                                "Current candidate Doris BE to tablet '{}' is '{}' with tablet count {}.",
                                tabletId,
                                target,
                                tabletCount);
                    }
                }
            }
            if (target == null) {
                String errMsg = "Cannot choice Doris BE for tablet " + tabletId;
                logger.error(errMsg);
                throw new DorisException(errMsg);
            }

            logger.debug("Choice Doris BE '{}' for tablet '{}'.", target, tabletId);
            be2Tablets.get(target).add(tabletId);
        }
        return be2Tablets;
    }

    /**
     * tablet count limit for one Doris RDD partition.
     *
     * @param readOptions configuration of request
     * @param logger {@link Logger}
     * @return tablet count limit
     */
    @VisibleForTesting
    static int tabletCountLimitForOnePartition(DorisReadOptions readOptions, Logger logger) {
        int tabletsSize = DORIS_TABLET_SIZE_DEFAULT;
        if (readOptions.getRequestTabletSize() != null) {
            tabletsSize = readOptions.getRequestTabletSize();
        }
        if (tabletsSize < DORIS_TABLET_SIZE_MIN) {
            logger.warn(
                    "{} is less than {}, set to default value {}.",
                    DORIS_TABLET_SIZE,
                    DORIS_TABLET_SIZE_MIN,
                    DORIS_TABLET_SIZE_MIN);
            tabletsSize = DORIS_TABLET_SIZE_MIN;
        }
        logger.debug("Tablet size is set to {}.", tabletsSize);
        return tabletsSize;
    }

    /**
     * translate BE tablets map to Doris RDD partition.
     *
     * @param options configuration of request
     * @param be2Tablets BE to tablets {@link Map}
     * @param opaquedQueryPlan Doris BE execute plan getting from Doris FE
     * @param database database name of Doris table
     * @param table table name of Doris table
     * @param logger {@link Logger}
     * @return Doris RDD partition {@link List}
     * @throws IllegalArgumentException throw when translate failed
     */
    @VisibleForTesting
    static List<PartitionDefinition> tabletsMapToPartition(
            DorisOptions options,
            DorisReadOptions readOptions,
            Map<String, List<Long>> be2Tablets,
            String opaquedQueryPlan,
            String database,
            String table,
            Logger logger)
            throws IllegalArgumentException {
        int tabletsSize = tabletCountLimitForOnePartition(readOptions, logger);
        List<PartitionDefinition> partitions = new ArrayList<>();
        for (Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            logger.debug("Generate partition with beInfo: '{}'.", beInfo);
            HashSet<Long> tabletSet = new HashSet<>(beInfo.getValue());
            beInfo.getValue().clear();
            beInfo.getValue().addAll(tabletSet);
            int first = 0;
            while (first < beInfo.getValue().size()) {
                Set<Long> partitionTablets =
                        new HashSet<>(
                                beInfo.getValue()
                                        .subList(
                                                first,
                                                Math.min(
                                                        beInfo.getValue().size(),
                                                        first + tabletsSize)));
                first = first + tabletsSize;
                PartitionDefinition partitionDefinition =
                        new PartitionDefinition(
                                database,
                                table,
                                beInfo.getKey(),
                                partitionTablets,
                                opaquedQueryPlan);
                logger.debug("Generate one PartitionDefinition '{}'.", partitionDefinition);
                partitions.add(partitionDefinition);
            }
        }
        return partitions;
    }
}
