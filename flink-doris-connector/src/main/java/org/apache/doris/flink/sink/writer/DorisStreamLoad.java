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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.StreamLoadException;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.HttpPutBuilder;
import org.apache.doris.flink.sink.ResponseUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static org.apache.doris.flink.sink.LoadStatus.FAIL;
import static org.apache.doris.flink.sink.ResponseUtil.LABEL_EXIST_PATTERN;
import static org.apache.doris.flink.sink.LoadStatus.LABEL_ALREADY_EXIST;
import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.FORMAT_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.JSON;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

/**
 * load data to doris.
 **/
public class DorisStreamLoad implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final String labelSuffix;
    private final byte[] lineDelimiter;
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final String ABORT_URL_PATTERN = "http://%s/api/%s/_stream_load_2pc";
    private static final byte[] JSON_ARRAY_START = "[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] JSON_ARRAY_END = "]".getBytes(StandardCharsets.UTF_8);
    private static final String JOB_EXIST_FINISHED = "FINISHED";

    private String loadUrlStr;
    private String hostPort;
    private final String abortUrlStr;
    private final String user;
    private final String passwd;
    private final String db;
    private final String table;
    private final Properties streamLoadProp;
    private final RecordStream recordStream;
    private Future<CloseableHttpResponse> pendingLoadFuture;
    private final CloseableHttpClient httpClient;
    private final ExecutorService executorService;
    private final String format;
    private boolean loadBatchFirstRecord;

    public DorisStreamLoad(String hostPort,
                           DorisOptions dorisOptions,
                           DorisExecutionOptions executionOptions,
                           String labelSuffix,
                           CloseableHttpClient httpClient) {
        this.hostPort = hostPort;
        String[] tableInfo = dorisOptions.getTableIdentifier().split("\\.");
        this.db = tableInfo[0];
        this.table = tableInfo[1];
        this.user = dorisOptions.getUsername();
        this.passwd = dorisOptions.getPassword();
        this.labelSuffix = labelSuffix;
        this.loadUrlStr = String.format(LOAD_URL_PATTERN, hostPort, db, table);
        this.abortUrlStr = String.format(ABORT_URL_PATTERN, hostPort, db);
        this.streamLoadProp = executionOptions.getStreamLoadProp();
        this.httpClient = httpClient;
        this.executorService = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ExecutorThreadFactory("stream-load-upload"));
        this.recordStream = new RecordStream(executionOptions.getBufferSize(), executionOptions.getBufferCount());
        this.format = executionOptions.getStreamLoadProp().getProperty(FORMAT_KEY, CSV);
        if (JSON.equals(format)) {
            lineDelimiter = ",".getBytes();
        } else {
            lineDelimiter = streamLoadProp.getProperty(LINE_DELIMITER_KEY, "\n").getBytes();
        }
        loadBatchFirstRecord = true;
    }

    public String getDb() {
        return db;
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
        this.loadUrlStr = String.format(LOAD_URL_PATTERN, hostPort, this.db, this.table);
    }

    public Future<CloseableHttpResponse> getPendingLoadFuture() {
        return pendingLoadFuture;
    }

    /**
     * try to discard pending transactions with labels beginning with labelSuffix.
     * @param labelSuffix
     * @param chkID
     * @throws Exception
     */
    public void abortPreCommit(String labelSuffix, long chkID) throws Exception {
        long startChkID = chkID;
        LOG.info("abort for labelSuffix {}. start chkId {}.", labelSuffix, chkID);
        while (true) {
            try {
                String label = labelSuffix + "_" + startChkID;
                HttpPutBuilder builder = new HttpPutBuilder();
                builder.setUrl(loadUrlStr)
                        .baseAuth(user, passwd)
                        .addCommonHeader()
                        .enable2PC()
                        .setLabel(label)
                        .setEmptyEntity()
                        .addProperties(streamLoadProp);
                RespContent respContent = handlePreCommitResponse(httpClient.execute(builder.build()));
                Preconditions.checkState("true".equals(respContent.getTwoPhaseCommit()));
                if (LABEL_ALREADY_EXIST.equals(respContent.getStatus())) {
                    // label already exist and job finished
                    if (JOB_EXIST_FINISHED.equals(respContent.getExistingJobStatus())) {
                        throw new DorisException("Load status is " + LABEL_ALREADY_EXIST + " and load job finished, " +
                                "change you label prefix or restore from latest savepoint!");

                    }
                    // job not finished, abort.
                    Matcher matcher  = LABEL_EXIST_PATTERN.matcher(respContent.getMessage());
                    if (matcher.find()) {
                        Preconditions.checkState(label.equals(matcher.group(1)));
                        long txnId = Long.parseLong(matcher.group(2));
                        LOG.info("abort {} for exist label {}", txnId, label);
                        abortTransaction(txnId);
                    } else {
                        LOG.error("response: {}", respContent.toString());
                        throw new DorisException("Load Status is " + LABEL_ALREADY_EXIST + ", but no txnID associated with it!");
                    }
                } else {
                    LOG.info("abort {} for check label {}.", respContent.getTxnId(), label);
                    abortTransaction(respContent.getTxnId());
                    break;
                }
                startChkID++;
            } catch (Exception e) {
                LOG.warn("failed to stream load data", e);
                throw e;
            }
        }
        LOG.info("abort for labelSuffix {} finished", labelSuffix);
    }

    /**
     * write record into stream.
     * @param record
     * @throws IOException
     */
    public void writeRecord(byte[] record) throws IOException{
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else {
            recordStream.write(lineDelimiter);
        }
        recordStream.write(record);
    }

    @VisibleForTesting
    public RecordStream getRecordStream() {
        return recordStream;
    }

    public RespContent handlePreCommitResponse(CloseableHttpResponse response) throws Exception{
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 && response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            LOG.info("load Result {}", loadResult);
            return OBJECT_MAPPER.readValue(loadResult, RespContent.class);
        }
        throw new StreamLoadException("stream load error: " + response.getStatusLine().toString());
    }

    public RespContent stopLoad() throws IOException{
        if (JSON.equals(format)) {
            recordStream.write(JSON_ARRAY_END);
        }
        recordStream.endInput();
        LOG.info("stream load stopped.");
        Preconditions.checkState(pendingLoadFuture != null);
        try {
           return handlePreCommitResponse(pendingLoadFuture.get());
        } catch (Exception e) {
            throw new DorisRuntimeException(e);
        }
    }

    /**
     * start write data for new checkpoint.
     * @param chkID
     * @throws IOException
     */
    public void startLoad(long chkID) throws IOException{
        loadBatchFirstRecord = true;
        String label = labelSuffix + "_" + chkID;
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        recordStream.startInput();
        LOG.info("stream load started for {}", label);
        try {
            InputStreamEntity entity = new InputStreamEntity(recordStream);
            putBuilder.setUrl(loadUrlStr)
                    .baseAuth(user, passwd)
                    .addCommonHeader()
                    .enable2PC()
                    .setLabel(label)
                    .setEntity(entity)
                    .addProperties(streamLoadProp);
            pendingLoadFuture = executorService.submit(() -> {
                LOG.info("start execute load");
                return httpClient.execute(putBuilder.build());
            });
        } catch (Exception e) {
            String err = "failed to stream load data with label: " + label;
            LOG.warn(err, e);
            throw e;
        }
        if (JSON.equals(format)) {
            recordStream.write(JSON_ARRAY_START);
        }
    }

    private void abortTransaction(long txnID) throws Exception {
        HttpPutBuilder builder = new HttpPutBuilder();
        builder.setUrl(abortUrlStr)
                .baseAuth(user, passwd)
                .addCommonHeader()
                .addTxnId(txnID)
                .setEmptyEntity()
                .abort();
        CloseableHttpResponse response = httpClient.execute(builder.build());

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200 || response.getEntity() == null) {
            LOG.warn("abort transaction response: " + response.getStatusLine().toString());
            throw new DorisRuntimeException("Fail to abort transaction " + txnID + " with url " + abortUrlStr);
        }

        ObjectMapper mapper = new ObjectMapper();
        String loadResult = EntityUtils.toString(response.getEntity());
        Map<String, String> res = mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>(){});
        if (FAIL.equals(res.get("status"))) {
            if (ResponseUtil.isCommitted(res.get("msg"))) {
                throw new DorisException("try abort committed transaction, " +
                        "do you recover from old savepoint?");
            }
            LOG.warn("Fail to abort transaction. error: {}", res.get("msg"));
        }
    }

    public void close() throws IOException {
        if (null != httpClient) {
            try {
                httpClient.close();
            } catch (IOException e) {
                throw new IOException("Closing httpClient failed.", e);
            }
        }
        if (null != executorService) {
            executorService.shutdownNow();
        }
    }
}
